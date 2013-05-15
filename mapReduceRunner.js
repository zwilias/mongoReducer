var pid = ObjectId();
var running = true;
var interval = 1000;

db.mapreduce.run.save(
        {"_id": "unique", "pid": pid}
    );

print(pid.toString() + ": running");

while (running) {
    var runningPid = db.mapreduce.run.findOne({"_id": "unique"});

    if (runningPid == null || !runningPid.hasOwnProperty("pid")) {
        running = false;
        print(pid.toString() + ": exiting - canceled");
    } else if (pid.toString() != runningPid.pid.toString()) {
        running = false;
        print(pid.toString() + ": exiting - new instance running");
    } else {
        db.mapreduce.run.update({"_id": "unique"}, {"$set": {"status": "running"}});
        mapReduce();
        db.mapreduce.run.update({"_id": "unique"}, {"$set": {"status": "sleeping"}});
        sleep(interval);
    }
}

function mapReduce() {
    var actions = db.mapreduce.find();

    actions.forEach(function(action) {
        var timestamp = new Date().getTime();
        if ((action.hasOwnProperty("force")  && action.force === true)
            || (action.hasOwnProperty("lastrun") && action.hasOwnProperty("interval")
                && action.lastrun + action.interval < timestamp))
        {
            runAction(action);
        }
    });
}

function run(actionName) {
    var action = db.mapreduce.findOne({"name": actionName});
    if (action != null) {
        printjson(runAction(action, "manual"));
    }
}

function extractOptions(action) {
    var options = {};
    var possibleOptions = ["finalize", "out", "sort", "limit", "query"];
    for (var i = 0, c = possibleOptions.length; i < c; i++) {
        var option = possibleOptions[i];
        if (action.hasOwnProperty(option)) {
            options[option] = action[option];
        }
    }

    if (options.hasOwnProperty("query") && typeof(options.query) === "function") {
        var previous = {};
        if (action.hasOwnProperty(previous)) {
            previous = action.previous;
        }

        options.query = options.query.apply(action, [previous]);
    }

    return options;
}


function runAction(action, type) {
    if (typeof(type) === "undefined") {
        var type = "auto";
    }

    var options = extractOptions(action);
   
    var result = db[action.collection].mapReduce
        (
         action.map, 
         action.reduce, 
         options
        );

    var timestamp = new Date().getTime();

    var cleanResult = {
        "out":          result.result,
        "timeMillis":   result.timeMillis,
        "ok":           result.ok,
        "counts":           result.counts
    };

    var previous        = {};
    if (action.hasOwnProperty("previous")) {
        previous = action.previous;
    }
    previous.timestamp  = timestamp;
    previous.result     = cleanResult;

    var update = {
        "force":    false,
        "lastrun":  timestamp,
        "type":     type,
        "previous": previous
    };

    db.mapreduce.update
        (
            {"_id":     action._id},
            {"$set":    update}
        );

    var log = {
        "timestamp":    timestamp,
        "action":       action,
        "result":       cleanResult
    };
    
    db.mapreduce.log.insert(log);

    return log;
}
