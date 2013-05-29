var parseCollection = function (collection, parser) {
    var cursor = db[collection].find({parsed: {$ne: true}}),
        counter = 0,
        errors = [],
        startTime = Date.now(),
        entry,
        result,
        endTime,
        diff;

    while (cursor.hasNext()) {
        entry = cursor.next();

        try {
            result = parseLine(entry.line, parser);
            Object.extend(result.result, {parsed: true, parsed_at: Date.now()});

            db[collection].update({_id: entry._id}, {$set: result.result});
            counter += 1;
        } catch (e) {
            errors.push(e);
        }
    }

    endTime = new Date();
    diff = endTime - startTime;

    return({time: diff, count: counter, errorcount: errors.length, errors: errors});
}

var parseLine = function (line, parser) {
    var finalResult = {},
        logObj = [];

    function log(message, data) {
        var entry = {
            timestamp: Date.now(),
            message: message,
            data: data
        };

        logObj.push(entry);
    }

    parser.patterns.forEach(function(check) {
        if (check.check.test(line)) {
            log("Passed check: " + check.check, {line: line, check: check});

            var result = {};

            check.extract.forEach(function(extr) {
                var values = [],
                    i,
                    c,
                    v,
                    key;

                switch (extr.type) {
                case "split":
                    log("Using 'split'", extr.split);
                    values = line.split(extr.split);
                    break;

                case "regex":
                    log("Using 'regex'", extr.regex);
                    values = extr.regex.exec(line);
                    values.shift();
                    break;

                case "function":
                    log("Using 'function'", extr.func);
                    values = extr.func(line);
                    break;
                }

                log("Received values", values);

                for (i = 0, c = values.length; i < c; i += 1) {
                    v = values[i];
                    key = "value" + i;

                    if (extr.names.length > i) {
                        key = extr.names[i];
                    }

                    if (key) {
                        result[key] = v;
                    }
                }

                log("Applied names", result);

                Object.extend(finalResult, result);
            });
        } else {
            log("Didn't pass check", {line: line, check: check.check});
        }
    });

    return {
        line: line,
        result: finalResult,
        log: logObj
    };
}
