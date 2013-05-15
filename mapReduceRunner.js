/*global db, sleep, print, ObjectId, printjson, mapReduce, run, shouldRun, runAction, clearOut */
/*jslint nomen: true, sloppy: true */

var pid     = new ObjectId(),   // the "pid" of this runner
    running = true,             // when false, we exit
    interval = 1000,            // how many ms we should sleep in between runs
    counter = 0,                // how many mapReduce actions were executed this run
    totalcount = 0;             // how many mapReduce actions were executed by this instance


var loglevels = {
    DEBUG: 0,
    INFO: 1,
    WARNING: 2,
    ERROR: 3
};

var loglevel = {
    db: loglevels.DEBUG,
    console: loglevels.WARNING
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
        start = new Date().getTime();
        mapReduce();
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

        log(loglevels.DEBUG, "Going to sleep for " + interval + "ms");
        sleep(interval);
    }
}

function mapReduce() {
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

function extractOptions(action) {
    var options = {},
        possibleOptions = ["finalize", "out", "sort", "limit", "query"],
        i,
        c,
        option,
        previous = {};

    for (i = 0, c = possibleOptions.length; i < c; i += 1) {
        option = possibleOptions[i];
        if (action.hasOwnProperty(option)) {
            options[option] = action[option];
        }
    }

    log(loglevels.DEBUG, "Applying options", options);

    if (action.hasOwnProperty("previous") && action.previous !== null) {
        previous = action.previous;
    } else {
        log(loglevels.DEBUG, "Resetting output collection - previous is empty");
        clearOut(action);
    }

    if (options.hasOwnProperty("query") && typeof (options.query) === "function") {
        log(loglevels.DEBUG, "We have a query function, applying it to get a query document");
        options.query = options.query.apply(action, [previous]);
    }

    return options;
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

    logObj = {
        "timestamp":    timestamp,
        "action":       action,
        "result":       cleanResult
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
