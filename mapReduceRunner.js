var pid = ObjectId();
var running = true;
var interval = 1000;

db.mapreduce.run.update(
        {}, 
        {"pid": pid}, 
        {"upsert": true}
    );

print(pid.toString() + ": running");

while (running) {
    var runningPid = db.mapreduce.run.findOne();

    if (runningPid == null || pid.toString() != runningPid.pid.toString()) {
        running = false;
    } else {
        mapReduce();
        sleep(interval);
    }
}

print(pid.toString() + ": exiting");

function mapReduce() {
    var actions = db.mapreduce.find();

    actions.forEach(function(action) {
        var timestamp = new Date().getTime();
        if ((action.hasOwnProperty("reset")  && action.reset === true)
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
        printjson(runAction(action));
    }
}

function runAction(action) {
    var timestamp = new Date().getTime();
    action.reset = false;
    action.lastrun = timestamp;
    db.mapreduce.save(action);
            
    delete action._id;

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
