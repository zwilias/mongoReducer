/*global db, sleep, print, ObjectId, printjson, mapReduce, run, shouldRun, runAction, clearOut, tojsononeline */
/*jslint nomen: true, sloppy: true, todo: true */

// TODO: clean up
// TODO: document
// TODO: move settings to db (providing sane default settings)
// TODO: perhaps provide string interpolation so debugging statements don't look as ugly
// TODO: error handling
// TODO: provide convenience functions for doing a check, autostarting, starting in a parallel shell

function _Poller() {
    this.pid = new ObjectId();
    this.running = false;
    this.interval = 1000;

    this.loglevel = {
        db: _Poller.loglevels.DEBUG,
        console: _Poller.loglevels.INFO
    };
}

_Poller.loglevels = {
    DEBUG: 0,       // things that are really quite redundant to see all the time,
                    // but are useful when debugging
    INFO: 1,        // quite useful when you're wondering what's going on
    WARNING: 2,     // generally used for recoverable things or when exiting
    ERROR: 3        // when we're broken and need fixing
};

_Poller.fn = {
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

    debug: function(message, data) {
        this.log(_Poller.loglevels.DEBUG, message, data);
    },

    info: function(message, data) {
        this.log(_Poller.loglevels.INFO, message, data);
    },

    warning: function(message, data) {
        this.log(_Poller.loglevels.WARNING, message, data);
    },

    error: function(message, data) {
        this.log(_Poller.loglevels.ERROR, message, data);
    },

    start: function(body, scope) {
        this.pid = new ObjectId();
        this.running = true;

        db.mapreduce.run.save(
            {"_id": "unique", "pid": this.pid}
        );

        this.info("Starting");

        var runningPid = {};

        while (this.running) {
            runningPid = db.mapreduce.run.findOne({"_id": "unique"});

            if (runningPid === null || !runningPid.hasOwnProperty("pid")) {
                this.running = false;
                this.warning("Exiting, canceled");
            } else if (this.pid.toString() !== runningPid.pid.toString()) {
                this.running = false;
                this.warning("Exiting, new instance started");
            } else {
                db.mapreduce.run.update({"_id": "unique"}, {"$set": {"status": "running"}});

                body.apply(scope);

                this.debug("Going to sleep for " + this.interval + "ms");
                sleep(this.interval);
            }
        }
    }
};

Object.extend(_Poller.fn, _Poller.prototype);
_Poller.prototype = _Poller.fn;
var Poller = new _Poller();

function MapReducer() {
    this.counter = 0;
    this.totalcount = 0;
}

MapReducer.fn = {
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

    clearOut: function(action) {
        var out = action.out;

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

    extractOptions: function(action) {
        var options = {},
            possibleOptions = ["finalize", "sort", "limit"],
            i,
            c,
            q,
            option,
            setCustomOut = false;

        if (action.hasOwnProperty("incremental") &&
                typeof (action.incremental) === "string" &&
                action.hasOwnProperty("previous") &&
                action.previous !== undefined &&
                action.hasOwnProperty("lastrun") &&
                action.lastrun > 0) {

            Poller.debug("Setting default incremental options");

            if (action.hasOwnProperty("out") && typeof (action.out) === "string" && typeof (action.out) !== "object") {
                options.out = {reduce: action.out};
                setCustomOut = true;
            }

            options.query = {};
            options.query[action.incremental] = {"$gt": action.lastrun};
        }

        for (i = 0, c = possibleOptions.length; i < c; i += 1) {
            option = possibleOptions[i];
            if (action.hasOwnProperty(option)) {
                options[option] = action[option];
            }
        }

        if (action.hasOwnProperty("out") && !setCustomOut) {
            options.out = action.out;
        }

        if (!options.hasOwnProperty("out")) {
            Poller.warning("No output collection/action specified, writing to results." + action.name, action);
            options.out = {reduce: "results." + action.name};
        }

        Poller.debug("Applying options");

        if (!action.hasOwnProperty("previous") || action.previous === null) {
            Poller.debug("Resetting output collection - previous is empty");
            this.clearOut(action);
        }

        q = this.extractQuery(action);
        if (q !== null) {
            options.query = q;
        }

        return options;
    },

    runAction: function(action, type) {
        var options,
            result,
            timestamp,
            cleanResult,
            previous = {},
            update,
            logObj;

        this.counter += 1;

        if (type === undefined) {
            type = "auto";
        }

        if (action.hasOwnProperty("pre") && typeof (action.pre) === "function") {
            Poller.debug("Applying pre-processing function");
            action.pre.apply(action);
        }

        options = this.extractOptions(action);

        Poller.debug("Applying mapReduce");
        result = db[action.collection].mapReduce(
            action.map,
            action.reduce,
            options
        );

        timestamp = new Date().getTime();

        cleanResult = {
            "out":          result.result,
            "timeMillis":   result.timeMillis,
            "ok":           result.ok,
            "counts":       result.counts
        };

        if (action.hasOwnProperty("previous")) {
            previous = action.previous;
        }
        previous.timestamp  = timestamp;
        previous.result     = cleanResult;

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

        if (options.hasOwnProperty("query") && typeof (options.query) === "object") {
            options.query = tojsononeline(options.query);
        }

        logObj = {
            "timestamp":    timestamp,
            "action":       action,
            "result":       cleanResult,
            "options":      options
        };

        Poller.info("Finished running " + action.name, logObj);


        if (action.hasOwnProperty("post") && typeof (action.post) === "function") {
            Poller.debug("Applying post-processing information");
            action.post.apply(action);
        }

        return logObj;
    },

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

var mr = new MapReducer();
Poller.start(mr.exec, mr);
