/*global db, sleep, print, ObjectId, printjson, mapReduce, run, shouldRun, runAction, clearOut */
/*jslint nomen: true, sloppy: true */

// TODO: clean up
// TODO: document
// TODO: move settings to db (providing sane default settings)
// TODO: perhaps provide string interpolation so debugging statements don't look as ugly
// TODO: perhaps provide convenience debug(), info(), warning(), error() functions?
// TODO: error handling
// TODO: make polling loop a function, and the body an argument
// TODO: provide convenience functions for doing a check, autostarting, starting in a parallel shell

var pid     = new ObjectId(),   // the "pid" of this runner
    running = true,             // when false, we exit
    interval = 1000,            // how many ms we should sleep in between runs
    counter = 0,                // how many mapReduce actions were executed this run
    totalcount = 0;             // how many mapReduce actions were executed by this instance


var loglevels = {
    DEBUG: 0,       // things that are really quite redundant to see all the time,
                    // but are useful when debugging
    INFO: 1,        // quite useful when you're wondering what's going on
    WARNING: 2,     // generally used for recoverable things or when exiting
    ERROR: 3        // when we're broken and need fixing
};

var loglevel = {
    db: loglevels.DEBUG,
    console: loglevels.INFO
};

db.mapreduce.run.save(
    {"_id": "unique", "pid": pid}
);

log(loglevels.INFO, "Starting");

var runningPid = {},
    start = 0,
    end = 0;

while (running) {
    runningPid = db.mapreduce.run.findOne({"_id": "unique"});

    if (runningPid === null || !runningPid.hasOwnProperty("pid")) {
        running = false;
        log(loglevels.WARNING, "Exiting, canceled");
    } else if (pid.toString() !== runningPid.pid.toString()) {
        running = false;
        log(loglevels.WARNING, "Exiting, new instance started");
    } else {
        db.mapreduce.run.update({"_id": "unique"}, {"$set": {"status": "running"}});

        doMapReduce();

        log(loglevels.DEBUG, "Going to sleep for " + interval + "ms");
        sleep(interval);
    }
}

function doMapReduce() {
    start = new Date().getTime();
    executeMapReduceActions();
    end = new Date().getTime();

    totalcount += counter;

    db.mapreduce.run.update({"_id": "unique"}, {"$set":
        {
            "status":   "sleeping",
            "time":     end - start,
            "actions":  counter,
            "totalactions": totalcount,
            "ping":     end
        }});
    counter = 0;
}

function executeMapReduceActions() {
    var actions = db.mapreduce.find();

    log(loglevels.DEBUG, "Found " + actions.count() + " actions");

    actions.forEach(function(action) {
        log(loglevels.DEBUG, "Checking if " + action.name + " should be executed");
        if (shouldRun(action)) {
            log(loglevels.DEBUG, "Executing " + action.name);
            runAction(action);
        }
    });
}

/**
 * Determine whether or not a given action should run - this time.
 *
 * There are 2 conditions in which an action will (automatically) execute:
 * - force was set to `true`
 * - lastrun + interval <= now
 *
 * If either or both of the lastrun and interval fields are missing, this action
 * will not execute. Keep this in mind when adding a new action - the lastrun field
 * must be set to an integer value, as well as the interval field.
 */
function shouldRun(action) {
    var timestamp = new Date().getTime();

    var execute = false;

    if (action.hasOwnProperty("force") && action.force === true) {
        execute = true;
        log(loglevels.DEBUG, "Force set to true");
    } else if (action.hasOwnProperty("lastrun") && action.hasOwnProperty("interval")
                && action.lastrun + action.interval < timestamp) {
        execute = true;
        log(loglevels.DEBUG, "lastrun + interval = " + action.lastrun + " + " + action.interval + " = " + (action.lastrun + action.interval));
    }

    return execute;
}

/**
 * Convenience function to easily allow to manually run an action - simply specify the name
 */
function run(actionName) {
    log(loglevels.INFO, "Trying to run action manually", actionName);

    var action = db.mapreduce.findOne({"name": actionName});
    if (action !== null) {
        log(loglevels.INFO, "Action found, running", actionName);
        printjson(runAction(action, "manual"));
    } else {
        log(loglevels.WARNING, "Action not found", actionName);
    }
}

/**
 * For internal use.
 *
 * Tries to generate the needed options for running the actual mapReduce command.
 *
 * Some options are always passed through as is:
 * - finalize
 * - sort
 * - limit
 *
 * The "out" and "query" options are generally also passed through, but conditions apply.
 *
 * The "query" option does not need to be a document; it can also be a function that returns a document
 * which can be used by the map-reduce functionality to filter the results that will have mapping
 * applied to.
 *
 * // TODO: finish this documentation
 */
function extractOptions(action) {
    var options = {},
        possibleOptions = ["finalize", "sort", "limit"],
        i,
        c,
        option,
        previous = {},
        setCustomOut = false;

    if (
            action.hasOwnProperty("incremental") &&
            typeof (action.incremental) === "string" &&
            action.hasOwnProperty("previous") &&
            typeof (action.previous) !== undefined &&
            action.previous !== {} &&
            action.hasOwnProperty("lastrun") &&
            action.lastrun > 0
        ) {

        log(loglevels.DEBUG, "Setting default incremental options");

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
        log(loglevels.WARNING, "No output collection/action specified, writing to results." + action.name, action);
        options.out = {reduce: "results." + action.name};
    }

    log(loglevels.DEBUG, "Applying options");

    if (action.hasOwnProperty("previous") && action.previous !== null) {
        previous = action.previous;
    } else {
        log(loglevels.DEBUG, "Resetting output collection - previous is empty");
        clearOut(action);
    }

    var q = extractQuery(action);
    if (q !== null) {
        options.query = q;
    }

    return options;
}

function extractQuery(action) {
    var query = null;

    if (action.hasOwnProperty("query") && typeof (action.query) === "string") {
        eval("query = " + action.query);
    }

    if (action.hasOwnProperty("queryf") && typeof (action.queryf) === "function") {
        if (query !== null) {
            log(loglevels.WARNING, "Both 'query' and 'queryf' are defined, using 'queryf'", action);
        }

        query = action.queryf.apply(action);
    }

    return query;
}

function clearOut(action) {
    var out = action.out;

    if (typeof out === "object") {
        if (out.hasOwnProperty("merge")) {
            out = out.merge;
        } else if (out.hasOwnProperty("reduce")) {
            out = out.reduce;
        }
    }

    log(loglevels.INFO, "Clearing " + out + " - reset", action);
    if (typeof out === "string") {
        db[out].remove();
    }
}

function runAction(action, type) {
    var options,
        result,
        timestamp,
        cleanResult,
        previous = {},
        update,
        logObj;

    counter += 1;

    if (type === undefined) {
        type = "auto";
    }

    if (action.hasOwnProperty("pre") && typeof (action.pre) === "function") {
        log(loglevels.DEBUG, "Applying pre-processing function");
        action.pre.apply(action);
    }

    options = extractOptions(action);

    log(loglevels.DEBUG, "Applying mapReduce");
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

    log(loglevels.DEBUG, "Updating action-information");

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

    log(loglevels.INFO, "Finished running " + action.name, logObj);


    if (action.hasOwnProperty("post") && typeof (action.post) === "function") {
        log(loglevels.DEBUG, "Applying post-processing information");
        action.post.apply(action);
    }

    return log;
}


function log(level, message, data) {
    if (level >= loglevel.db || level >= loglevel.console) {
        var ts = new Date().getTime();

        var logObj = {
            timestamp: ts,
            pid: pid,
            level: level,
            message: message,
            data: data
        };

        if (level >= loglevel.db) {
            db.mapreduce.log.insert(logObj);
        }

        if (level >= loglevel.console) {
            print(ts + "\t" + pid.toString() + "\t" + message);
        }
    }
}
