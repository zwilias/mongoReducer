/*global db, sleep, print, ObjectId, printjson, mapReduce, run, shouldRun, runAction, clearOut */
/*jslint nomen: true, sloppy: true */

var pid     = new ObjectId(),   // the "pid" of this runner
    running = true,             // when false, we exit
    interval = 60000,           // how many ms we should sleep in between runs
    counter = 0;                // how many mapReduce actions were executed this run

db.mapreduce.run.save(
    {"_id": "unique", "pid": pid}
);

print(pid.toString() + ": running");

var runningPid = {},
    start = 0,
    end = 0;

while (running) {
    runningPid = db.mapreduce.run.findOne({"_id": "unique"});

    if (runningPid === null || !runningPid.hasOwnProperty("pid")) {
        running = false;
        print(pid.toString() + ": exiting - canceled");
    } else if (pid.toString() !== runningPid.pid.toString()) {
        running = false;
        print(pid.toString() + ": exiting - new instance running");
    } else {
        db.mapreduce.run.update({"_id": "unique"}, {"$set": {"status": "running"}});
        start = new Date().getTime();
        mapReduce();
        end = new Date().getTime();

        db.mapreduce.run.update({"_id": "unique"}, {"$set":
            {
                "status":   "sleeping",
                "time":     end - start,
                "actions":  counter,
                "ping":     end
            }});
        counter = 0;
        sleep(interval);
    }
}

function mapReduce() {
    var actions = db.mapreduce.find();

    actions.forEach(function(action) {
        if (shouldRun(action)) {
            runAction(action);
        }
    });
}

function shouldRun(action) {
    var timestamp = new Date().getTime();
    return (action.hasOwnProperty("force")  && action.force === true)
            || (action.hasOwnProperty("lastrun") && action.hasOwnProperty("interval")
                && action.lastrun + action.interval < timestamp);
}

function run(actionName) {
    var action = db.mapreduce.findOne({"name": actionName});
    if (action !== null) {
        printjson(runAction(action, "manual"));
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

    if (action.hasOwnProperty("previous") && action.previous !== null) {
        previous = action.previous;
    } else {
        clearOut(action);
    }

    if (options.hasOwnProperty("query") && typeof (options.query) === "function") {
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
        log;

    counter += 1;

    if (type === undefined) {
        type = "auto";
    }

    options = extractOptions(action);

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

    log = {
        "timestamp":    timestamp,
        "action":       action,
        "result":       cleanResult
    };

    db.mapreduce.log.insert(log);

    return log;
}
