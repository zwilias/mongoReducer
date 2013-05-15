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
        mapReduce();
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

function runAction(action, type) {
    if (typeof(type) === "undefined") {
        var type = "auto";
    }

    var timestamp = new Date().getTime();

    update.force = false;
    update.lastrun = timestamp;
    update.type = type;

    db.mapreduce.update(
            {"_id": action._id},
            {"$set": update}
        );
            
    var options = {};
    var possibleOptions = ["finalize", "out", "sort", "limit", "query"];
    for (var i = 0, c = possibleOptions.length; i < c; i++) {
        var option = possibleOptions[i];
        if (action.hasOwnProperty(option)) {
            options[option] = action[option];
        }
    }

    if (options.hasOwnProperty("query") && typeof(options.query) === "function") {
        var data = {};
        if (action.hasOwnProperty("data") && action.data != null) {
            data = action.data;
        }

        options.query = options.query.apply(data, [timestamp]);
    }

    var result = db[action.collection].mapReduce
        (
         action.map, 
         action.reduce, 
         options
        );

    var log = {
        "timestamp": timestamp,
        "action": action,
        "result": { // can't save the raw result object
                    // private fields introduce recursion..
            "out": result.result,
            "timeMillis": result.timeMillis,
            "ok": result.ok,
            "counts": result.counts
        }
    };
    
    db.mapreduce.log.insert(log);

    return log;
}
