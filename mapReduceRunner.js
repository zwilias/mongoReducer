/*global db, sleep, print, ObjectId, printjson, mapReduce, run, shouldRun, runAction, clearOut, tojsononeline */
/*jslint nomen: true, sloppy: true, todo: true */

/**
 * Class definitions and convenience functions for using mongo's map-reduce functionality from the inside out.
 *
 * More info on http://mongoinsideout.blogspot.com
 *
 * @author Ilias Van Peer, Geert Van Damme
 * @version 0.1
 */

/**
 * Convenience function that defines a constructor, static properties and instance methods for the _Poller class
 * and releases a Poller instance into the global namespace
 */
var initPoller = function () {

    /**
     * Creates an instance of the _Poller class.
     *
     * The _Poller class contains the main polling loop, as well as logging functionality and convenience functions
     * for logging at a number of levels. Those levels are defined in _Poller.loglevels.
     *
     * The settings for the polling loop as well as for the logging functionality can be defined in the database. This
     * should happen in the db.mapreduce.settings collections. Currently the following settings are supported:
     *
     * - interval: an integer value defining how many milliseconds the polling loop should sleep inbetween each polling action
     * - loglevel: an object with 2 key-value pairs (db and console) defining the minimum level a message should have before
     *      appearing in the output. Logging to the db writes to db.mapreduce.log, writing to the console simply prints the message.
     *      To disable all logging, set both db and console to a value higher than 3.
     *
     * When the _Poller is instantiated, it will generated a "pid". This "pid" is just an ObjectId, used to identify an instance uniquely.
     * This pid is saved to the database in the db.mapreduce.run collection, which is akin to a typical runfile.
     *
     * There may only be one single entry in the db.mapreduce.run collection, which we signify, by convention, by setting the _id to "unique".
     * When this value is removed or overwritten with a new "pid" by starting a new instance, the current instance will exit.
     *
     * @constructor
     * @this {_Poller}
     */
    function _Poller() {
        this.pid = new ObjectId();
        this.running = false;
        this.interval = false;  // getConfig() will replace this with the actual interval
        this.loglevel = false;  // getconfig will replace this with the actual desired loglevels
    }

    /**
     * Static property on _Poller with the different possible loglevels.
     */
    _Poller.loglevels = {
        /**
         * Things that are really quite redundant to see all the time, but are useful when debugging.
         */
        DEBUG: 0,

        /**
         * Quite useful when you're wondering what's going on.
         */
        INFO: 1,

        /**
         * Used for recoverable, user-side mistakes. Can signify that a correction was made to user-input.
         */
        WARNING: 2,

        /**
         * Signifies something broke.
         */
        ERROR: 3
    };

    /**
     * Defines all the methods that will become part of the Prototype. This may later be extended with extra
     * methods or be overridden by the user.
     */
    _Poller.fn = {
        /**
         * Fetches the settings from db.mapreduce.settings and updates our instance properties.
         *
         * @this {_Poller}
         */
        getConfig: function() {

            var key,
                value,
                config = {
                    interval: 1000,
                    loglevel: {
                        db: _Poller.loglevels.DEBUG,
                        console: _Poller.loglevels.INFO
                    }
                };

            for (key in config) {
                value = db.mapreduce.settings.findOne({_id: key});

                if (value !== null) {
                    config[key] = value.value;
                } else {
                    db.mapreduce.settings.insert({_id: key, value: config[key]});
                }
            }

            this.interval = config.interval;
            this.loglevel = config.loglevel;
        },

        /**
         * Logs a message and optional, additional data, with a certain level to the console and/or db.
         *
         * @this {_Poller}
         * @param {int} level       The log-level of this message. One of _Poller.loglevels.
         * @param {string} message  The message that should be logged. This will be shown in the console.
         * @param {object} data     Optional data-object that will be saved to the database giving context to the message.
         */
        log: function(level, message, data) {
            if (level >= this.loglevel.db || level >= this.loglevel.console) {
                var ts = new Date().getTime(),
                    logObj = {
                        timestamp:  ts,
                        pid:        this.pid,
                        level:      level,
                        message:    message,
                        data:       data
                    };
                if (level >= this.loglevel.db) {
                    db.mapreduce.log.insert(logObj);
                }
                if (level >= this.loglevel.console) {
                    print(ts + "\t" + this.pid.toString() + "\t" + message);
                }
            }
        },

        /**
         * Convenience function that logs a DEBUG level message.
         *
         * @see {_Poller.fn.log}
         * @this {_Poller}
         * @param {string} message  The message that should be logged. This will be shown in the console.
         * @param {object} data     Optional data-object that will be saved to the database giving context to the message.
         */
        debug: function(message, data) {
            this.log(_Poller.loglevels.DEBUG, message, data);
        },

        /**
         * Convenience function that logs an INFO level message.
         *
         * @see {_Poller.fn.log}
         * @this {_Poller}
         * @param {string} message  The message that should be logged. This will be shown in the console.
         * @param {object} data     Optional data-object that will be saved to the database giving context to the message.
         */
        info: function(message, data) {
            this.log(_Poller.loglevels.INFO, message, data);
        },

        /**
         * Convenience function that logs a WARNING level message.
         *
         * @see {_Poller.fn.log}
         * @this {_Poller}
         * @param {string} message  The message that should be logged. This will be shown in the console.
         * @param {object} data     Optional data-object that will be saved to the database giving context to the message.
         */
        warning: function(message, data) {
            this.log(_Poller.loglevels.WARNING, message, data);
        },

        /**
         * Convenience function that logs an ERROR level message.
         *
         * @see {_Poller.fn.log}
         * @this {_Poller}
         * @param {string} message  The message that should be logged. This will be shown in the console.
         * @param {object} data     Optional data-object that will be saved to the database giving context to the message.
         */
        error: function(message, data) {
            this.log(_Poller.loglevels.ERROR, message, data);
        },

        /**
         * Starts the polling loop.
         *
         * This will make sure the settings are up to date, and run the body-function in the specified scope, if any.
         *
         * @this {_Poller}
         * @param {function} body   The main body of the loop. This can, for example, be something like MapReducer.exec.
         * @param {object} scope    The scope that should be set on the body.
         */
        start: function(body, scope) {
            this.getConfig();

            this.pid = new ObjectId();
            this.running = true;

            db.mapreduce.run.save(
                {"_id": "unique", "pid": this.pid}
            );

            this.info("Starting");
            var runningPid = {};

            while (this.running) {
                this.getConfig();
                runningPid = db.mapreduce.run.findOne({"_id": "unique"});

                if (runningPid === null || !runningPid.hasOwnProperty("pid")) {
                    this.running = false;
                    this.info("Exiting, canceled");
                } else if (this.pid.toString() !== runningPid.pid.toString()) {
                    this.running = false;
                    this.info("Exiting, new instance started");
                } else {
                    db.mapreduce.run.update({"_id": "unique"}, {"$set": {"status": "running"}});

                    try {
                        body.apply(scope);
                    } catch (e) {
                        this.error("An error occurred", e);
                    }

                    this.debug("Going to sleep for " + this.interval + "ms");
                    sleep(this.interval);
                }
            }
        }
    };

    // Extend _Poller.fn with the prototype, and set the prototype to fn.
    // That way, extra functionality can be added by referring to _Poller.fn.
    Object.extend(_Poller.fn, _Poller.prototype);
    _Poller.prototype = _Poller.fn;
    Poller = new _Poller(); // release it into the global namespace!
};

/**
 * Convenience function that defines a constructor and instance methods for our MapReducer class.
 *
 * This will also ensure that the Poller object has been declared and defined, as well as create a MapReducer object and
 * release it into the global namespace.
 */
var initMapReducer = function () {
    if (typeof (Poller) === "undefined") {
        initPoller();
    }

    /**
     * Instantiates a MapReducer object.
     *
     * @constructor
     * @this {MapReducer}
     */
    function MapReducer() {
        this.counter = 0;
        this.totalcount = 0;
    }

    /**
     * Like with _Poller, we define all instance methods for MapReducer object in a static property of the MapReducer class.
     *
     * Inside this class, we often refer to "action" objects. By an "action" object, we mean a document retrieved from the db.mapreducer
     * collection.
     *
     * A minimal action object should have the following schema:
     *
     * {
     *      name: "string",
     *      collection: "string",
     *      map: function() {},
     *      reduce: function(key, values) {}
     * }
     *
     * This would create an Action object that can only manually be run by calling MapReducer.runAction(*name of object*)
     *
     * However, that's not particularily useful. To create an action that runs periodically, at minimum "interval" and "lastrun" key-value pairs
     * should be defined on the action. Interval specifies the minimal number of milliseconds between 2 consecutive runs. "lastrun" specifies
     * the epoch in milliseconds of the last time this action object finished executing.
     *
     * Whenever an action object is executed, it will receive a "previous" property with extra information about the previous time an action was ran.
     * This information includes a timestamp (generally the same as the lastrun property), as well as the mapreduceresult info. If a "previous" object
     * is already declared on an action object, it will be extended with the latest information, preserving any custom properties that may have been
     * declared.
     *
     * Additional properties may be declared on the action object:
     *
     * 1. pre: function and/or post: function
     *
     * When defined, the "pre" function will be called as a preprecessing function on the action object. The scope of the pre function will be set
     * to the action. The same goes for the "post" function which will be called as a final step in the chain as a postprocessing step.
     *
     * 2. finalize, sort, limit, scope, jsMode, verbose
     *
     * These properties will be passed through, unmodified, to the mapreduce command, if declared.
     * See their documentation in the mongo docs for more info.
     *
     * 3. out: string | object
     *
     * When specified, this will generally be passed through unmodified to the mapreduce command. When this is not specified,
     * we fallback to {out: {reduce: *action.name*}}.
     *
     * This can be modified when the action is specified to be an incremental object. If the "out" property is then set simple to a collection,
     * it will be replaced by {out: {reduce: *action.out*}}
     *
     * 4. incremental: string
     *
     * If this is set, we assume you want to make this action an incremental map-reduce action. The "incremental" property should give the path to a
     * json property defined on the documents it runs on that specifies a timestamp. We can then generate a default query that limits the input documents
     * to those created after the last time mapreduce was run.
     *
     * 5. query: string and queryf: function
     *
     * To limit the input objects to a certain set of objects, specify either the query document as a json-string or define a function that
     * returns a query document. The reason we don't simply ask for an object here, is that often these query documents will have an operator
     * such as "$gt". Such documents cannot be saved in the database.
     *
     * When queryf is declared, it will be called with its scope set to the action object, so any and all properties declared on the action document
     * will be accessible to the queryf function, including but not limited to the "previous" property.
     *
     * 6. force: boolean
     *
     * When set to "true", this action will be executed on the next run of the polling loop, no matter what.
     *
     * NOTE: to reset a map-reduce action, clear or remove its "previous" property. This will force us to remove all documents from the output collection
     * and start afresh.
     */
    MapReducer.fn = {
        /**
         * Decides whether a given action should be executed.
         *
         * There are 2 things we look at here, the "force" property, and the "interval" and "lastrun" properties. If force is set to true,
         * this action will be executed. If lastrun + interval < the current time, this action will execute. Otherwise, it won't.
         *
         * @this {MapReducer}
         * @param   {object}    action
         * @return {boolean}    True when this action should be executed, false if it should not.
         */
        shouldRun: function(action) {
            var timestamp = new Date().getTime(),
                execute = false;

            if (action.hasOwnProperty("force") && action.force === true) {
                execute = true;
                Poller.debug("Force set to true");
            } else if (action.hasOwnProperty("lastrun") && action.hasOwnProperty("interval")
                        && action.lastrun + action.interval < timestamp) {
                execute = true;
                Poller.debug("lastrun + interval = " + action.lastrun + " + " + action.interval + " = " + (action.lastrun + action.interval));
            }

            return execute;
        },

        /**
         * Attempts to extract the query document, if any, specified in this action object.
         *
         * @this {MapReducer}
         * @param {object} action
         * @return {object} The query document, created by eval'ing the action.query jsonstring or executing the action.queryf function
         */
        extractQuery: function(action) {
            var query = null;

            if (action.hasOwnProperty("query") && typeof (action.query) === "string") {
                eval("query = " + action.query);
            }

            if (action.hasOwnProperty("queryf") && typeof (action.queryf) === "function") {
                if (query !== null) {
                    Poller.warning("Both 'query' and 'queryf' are defined, using 'queryf'", action);
                }

                query = action.queryf.apply(action);
            }

            return query;
        },

        /**
         * When called, this method will find the output collection of the specified action and empty it.
         *
         * @this {MapReducer}
         * @param {object} action
         */
        clearOut: function(action) {
            var out = this.genOutOption(action, true).out;

            if (typeof out === "object") {
                if (out.hasOwnProperty("merge")) {
                    out = out.merge;
                } else if (out.hasOwnProperty("reduce")) {
                    out = out.reduce;
                }
            }

            Poller.info("Clearing " + out + " - reset", action);

            if (typeof out === "string") {
                db[out].remove();
            }
        },

        /**
         * If the incremental property was specified on the action, this method will generate some default options for doing incremental map-reduce.
         *
         * @this {MapReducer}
         * @param {object} action
         * @return {object} either an empty object if incremental does not apply, or an options object with some defaults set.
         */
        genIncrementalOptions: function(action) {
            var options = {};

            if (action.hasOwnProperty("incremental") &&
                    typeof (action.incremental) === "string" &&
                    action.hasOwnProperty("previous") &&
                    action.previous !== undefined &&
                    action.hasOwnProperty("lastrun") &&
                    action.lastrun > 0) {
                Poller.debug("Setting default incremental options");

                if (action.hasOwnProperty("out") && typeof (action.out) === "string" && typeof (action.out) !== "object") {
                    options.out = {reduce: action.out};
                }

                options.query = {};
                options.query[action.incremental] = {"$gt": action.lastrun};
            }

            return options;
        },

        /**
         * Extracts an options contained in the action object.
         *
         * Currently, this is limited to "jsMode", "verbose", "scope", "finalize", "sort" and "limit", which are passed through unmodified to the mapreduce command.
         *
         * @this {MapReducer}
         * @param {object} action
         * @return {object} An options object that can be merged into the other options.
         */
        genContainedOptions: function(action) {
            var options = {},
                possibleOptions = ["finalize", "sort", "limit", "scope", "verbose", "jsMode"],
                option;

            for (option in possibleOptions) {
                if (action.hasOwnProperty(option)) {
                    options[option] = action[option];
                }
            }

            return options;
        },

        /**
         * Extracts of generates an output collection for the specified action object.
         *
         * If the action object does not specify an output collection or action, this will throw a warning (unless supressWarnings is specified) and
         * write the results to {out: {reduce: "results" + action.name}}
         *
         * @this {MapReducer}
         * @param {object} action
         * @param {boolean} supressWarnings
         * @return {object} an options object for merging into the main options object.
         */
        genOutOption: function(action, supressWarnings) {
            var options = {};
            supressWarnings = supressWarnings === undefined
                ? false
                : supressWarnings;

            if (action.hasOwnProperty("out")) {
                options.out = action.out;
            } else {
                if (!supressWarnings) {
                    Poller.warning("No output collection/action specified, writing to results." + action.name, action);
                }
                options.out = {reduce: "results." + action.name};
            }

            return options;
        },

        /**
         * Will decide whether the ouput collection of the specified action object must be cleared, and if so, will call clearOut.
         *
         * @this {MapReducer}
         * @param {object} action
         */
        clearOutputCollection: function(action) {
            if (!action.hasOwnProperty("previous") || action.previous === null) {
                Poller.debug("Resetting output collection - previous is empty");
                this.clearOut(action);
            }
        },

        /**
         * Prepares the complete options object that will be passed into the mapreduce command.
         *
         * This will execute the following methods, in order, and merge their results:
         *
         * - this.genOutOption(action)
         * - this.genIncrementalOptions(action)
         * - this.genContainedOptions(action)
         *
         * It will the check to see if the output collection needs to be cleared.
         *
         * Finally, it will see if a query or queryf was declared, and prepare the query document.
         *
         * @this {MapReducer}
         * @param {object} action
         * @return {object} options object
         */
        extractOptions: function(action) {
            var options = {};

            // First we create an "out" option. We do this first because it might be overwritten when
            // figuring out the options for incremental map reduce functionality. Order matters!
            //
            // Next, we see if the action is an automagical incremental action. If it is, we specify
            // the options needed for that.
            //
            // Finally, we extract the options, if any, which are specified in the action object and
            // should be passed through unmodified. For now, those are "finalize", "sort" and "limit".
            Object.extend(options, this.genOutOption(action));
            Object.extend(options, this.genIncrementalOptions(action));
            Object.extend(options, this.genContainedOptions(action));

            this.clearOutputCollection(action);

            options.query = this.extractQuery(action) || options.query;

            return options;
        },

        /**
         * If a pre-function was declared, run it.
         *
         * @this {MapReducer}
         * @param {object} action
         */
        doPreprocessing: function(action) {
            if (action.hasOwnProperty("pre") && typeof (action.pre) === "function") {
                Poller.debug("Applying pre-processing function");
                action.pre.apply(action);
            }
        },

        /**
         * If a post-function was declared, run it.
         *
         * @this {MapReducer}
         * @param {object} action
         */
        doPostprocessing: function(action) {
            if (action.hasOwnProperty("post") && typeof (action.post) === "function") {
                Poller.debug("Applying post-processing function");
                action.post.apply(action);
            }
        },

        /**
         * Executes the mapReduce command on the given action with the supplied options, if any.
         *
         * @this {MapReducer}
         * @param {object} action
         * @param {object} options
         * @return {object} mapReduceResult object
         */
        doMapReduce: function(action, options) {
            Poller.debug("Applying mapReduce");
            return db[action.collection].mapReduce(
                action.map,
                action.reduce,
                options
            );
        },

        /**
         * Extracts a subset of the key-value pairs a regular mapReduceResult object contains.
         *
         * Specifically, this will extract the result, timeMillis, ok and counts properties.
         *
         * @this {MapReducer}
         * @param {object} mapReduceResult object
         * @return {object} object with "out", "timeMillis", "ok" and "counts" properties
         */
        extractCleanResult: function(result) {
            return {
                "out":          result.result,
                "timeMillis":   result.timeMillis,
                "ok":           result.ok,
                "counts":       result.counts || {}
            };
        },

        /**
         * Extends or generate a "previous" property to attach to the action.
         *
         * @this {MapReducer}
         * @param {object} action
         * @param {object} cleanResult a cleaned mapReduceResult object
         * @param {long} timestamp The epoch in milliseconds
         * @return {object} previous
         */
        generatePreviousObj: function(action, cleanResult, timestamp) {
            var previous = {};
            if (action.hasOwnProperty("previous")) {
                previous = action.previous;
            }

            previous.timestamp  = timestamp;
            previous.result     = cleanResult;

            return previous;
        },

        /**
         * Create a log entry after running the specified action
         *
         * @this {MapReducer}
         * @param {object} action
         * @param {long} timestamp
         * @param {object} clearResult
         * @param {object} options
         * @return {object} The object that was added to the log entry as additional data.
         */
        doLogActionrun: function(action, timestamp, cleanResult, options) {
            if (options.hasOwnProperty("query") && typeof (options.query) === "object") {
                options.query = tojsononeline(options.query);
            }

            var logObj = {
                "timestamp":    timestamp,
                "action":       action,
                "result":       cleanResult,
                "options":      options
            };
            Poller.info("Finished running " + action.name, logObj);

            return logObj;
        },

        /**
         * Updates the action in the database, setting force to false, lastrun, type and previous information.
         *
         * @this {MapReducer}
         * @param {object} action
         * @param {long} timestamp
         * @param {string} type
         * @param {object} previous
         */
        updateActionInfo: function(action, timestamp, type, previous) {
            var update;

            Poller.debug("Updating action-information");
            update = {
                "force":    false,
                "lastrun":  timestamp,
                "type":     type,
                "previous": previous
            };
            db.mapreduce.update(
                {"_id":     action._id},
                {"$set":    update}
            );
        },

        /**
         * Runs the specified action.
         *
         * @this {MapReducer}
         * @param {object} action
         * @param {string} type (manual, auto or undefined, defaulting to auto)
         * @return {object} the log object
         */
        runAction: function(action, type) {
            var options,
                result,
                timestamp,
                cleanResult,
                previous,
                logObj;

            this.counter += 1;

            if (type === undefined) {
                type = "auto";
            }

            this.doPreprocessing(action);

            options = this.extractOptions(action);
            result = this.doMapReduce(action, options);

            timestamp = new Date().getTime();
            cleanResult = this.extractCleanResult(result);
            previous = this.generatePreviousObj(action, cleanResult, timestamp);

            this.updateActionInfo(action, timestamp, type, previous);

            logObj = this.doLogActionrun(action, timestamp, cleanResult, options);

            this.doPostprocessing(action);

            return logObj;
        },

        /**
         * Runs the action specified by a name
         *
         * @param {string} actionName
         * @this {MapReducer}
         */
        run: function(actionName) {
            Poller.info("Trying to run action manually", actionName);
            var action = db.mapreduce.findOne({"name": actionName});

            if (action !== null) {
                Poller.info("Action found, running", actionName);
                printjson(this.runAction(action, "manual"));
            } else {
                Poller.warning("Action not found", actionName);
            }
        },

        /**
         * For each action in the specified list of actions, figure out if the action should run and if so, run it.
         *
         * @param {Array.[object]} actions  Array fo action objects
         * @this {MapReducer}
         */
        execActions: function(actions) {
            var that = this; // the anonymous function in actions.forEach can't access "this"
            Poller.debug("Found " + actions.count() + " actions");

            actions.forEach(function(action) {
                Poller.debug("Checking if " + action.name + " should be executed");

                if (that.shouldRun(action)) {
                    Poller.debug("Executing " + action.name);
                    that.runAction(action);
                }
            });
        },

        /**
         * Finds all actions, tries to execute them, and keeps some timing information and counters around.
         *
         * This is the main entrypoint in the MapReducer universe.
         *
         * @this {MapReducer}
         */
        exec: function() {
            var start = new Date().getTime(),
                end;

            this.execActions(db.mapreduce.find());
            end = new Date().getTime();
            this.totalcount += this.counter;
            db.mapreduce.run.update({"_id": "unique"}, {"$set":
                {
                    "status":   "sleeping",
                    "time":     end - start,
                    "actions":  this.counter,
                    "totalactions": this.totalcount,
                    "ping":     end
                }});
            this.counter = 0;
        }
    };

    Object.extend(MapReducer.fn, MapReducer.prototype);
    MapReducer.prototype = MapReducer.fn;
    mapReducer = new MapReducer();
};

var start = function() {
    if (typeof (mapReducer) === "undefined" || typeof (Poller) === "undefined") {
        initMapReducer();
    }

    Poller.start(mapReducer.exec, mapReducer);
};

var setup = function() {
    // insert initPoller, initMapReducer and start() into the db
    print("Saving initPoller(), initMapReducer() and start() in system.js.");
    db.system.js.save({"_id": "initPoller", "value": initPoller});
    db.system.js.save({"_id": "initMapReducer", "value": initMapReducer});
    db.system.js.save({"_id": "start", "value": start});
};

if (typeof(autostart) !== "undefined" && autostart) {
    start();
}
