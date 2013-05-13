var pid = ObjectId();
var running = true;
var interval = 1000;

db.mapreduce.run.update(
        {}, 
        {"pid": pid}, 
        {"upsert": true}
    );

print("Running with pid " + pid.toString());

while (running) {
    var runningPid = db.mapreduce.run.findOne();

    if (runningPid == null || pid.toString() != runningPid.pid.toString()) {
        running = false;
    } else {
        mapReduce();
        sleep(interval);
    }
}

print("Exiting");

function mapReduce() {
    var actions = db.mapreduce.find();

    actions.forEach(function(action) {
        var timestamp = new Date().getTime();
        if ((action.hasOwnProperty("reset")  && action.reset === true)
            || (action.hasOwnProperty("lastrun") && action.hasOwnProperty("interval")
                && action.lastrun + action.interval < timestamp))
        {
            action.reset = false;
            action.lastrun = timestamp;
            db.mapreduce.save(action);
            
            delete action._id;
            runAction(action);
        }
    });
}

function runAction(action) {
    var timestamp = new Date().getTime();

    var result = db[action.collection].mapReduce
        (
         action.map, 
         action.reduce, 
         {"out": action.out}
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
}
