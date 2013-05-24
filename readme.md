# MongoReducer #

__MongoReducer__ allows running mongo's mapreduce functionality from within mongo.

## Table of Contents ##

1. [Introduction](#introduction)
2. [An action object](#an-action-object)
	1. [The `force` property](#the-force-property)
	2. [The `out` property](#the-out-property)
	3. [The `jsMode`, `verbose`, `scope`, `finalize`, `sort` and `limit` properties](#the-jsmode-verbose-scope-finalize-sort-and-limit-properties)
	4. [The `pre` and `post` properties](#the-pre-and-post-properties)
	5. [The `lastrun` property](#the-lastrun-property)
	6. [The `interval` property](#the-interval-property)
	7. [The `previous` property](#the-previous-property)
	8. [The `type` property](#the-type-property)
	9. [The `incremental` property](#the-incremental-property)
	10. [The `query` and `queryf` properties](#the-query-and-queryf-properties)
	11. [Overview](#overview)
3. [The `MapReducer` object](#the-mapreducer-object)
	1. [Relevant methods](#relevant-methods)
4. [The `Poller` object](#the-poller-object)
	1. [The "pid"](#the-pid)
	2. [Logging](#logging)
	3. [The `db.mapreduce.run` collection](#the-dbmapreducerun-collection)
	4. [Settings](#settings)
6. [Installation and Usage](#installation-and-usage) 
5. [License](#license)

***

## Introduction ##

Specifically, this is a framework of sorts, built using convention over configuration in mind, which allows saving a complete map-reduce actions, with its mapper and reducer and all other configuration and settings in the `db.mapreduce` collection and running it with a single command.

Furthermore, __MongoReducer__ comes with a polling loop which polls the `db.mapreduce` collection (amongst others) and will run all map-reduce actions found within, at specified time-intervals, without the need for a complicated setup involving a number of cronjobs and a host of other scripts for actually executing the map-reduce actions.

To this end, __MongoReducer__ is split into two main parts, the `Poller` object and the `MapReduce` object. `Poller` is an instance of the `_Poller` class, whereas `MapReduce` is an instance of the `MapReduce` class.

Let’s take a look at all the parts making up a complete __MongoReducer__ installation, from the inside out.

## An action object ##

So called “action objects” are simply a name we use to refer to any document stored in the `db.mapreduce` collection.

A minimal such _action_ object looks as follows:

```
{
	"collection":	"aCollection",
    "map":			function () {},
    "reduce":		function (key, values) {},
    "name":			"aName"
}
```

This defines a map-reduce action on the `db.aCollection` collections with the mapping function `function () {}` and the reducing function `function (key, values) {}`. As a convenience, we also name the action, in this case calling it `aName`. We actually enforce having a `name` property, which is used in various logging functions, as well, in this case, for determining the output collection since none was specified.

However, there are plenty of other properties that can be set to influence the behaviour of this map-reduce action.

### The `force` property ###

The `force` property allows forcing an action to be executed the next time the polling loop encounters this action. If the `force` property is not set, it defaults to `false`.

Example:

```
{
	"collection":	"aCollection",
	"map":			function () {},
	"reduce":		function (key, values) {},
	"name":			"aName",
	"force": 		true
}
```

This forces the `aName` action to be executed on the next run. It will afterwards automatically be set to `false`.

### The `out` property ###

The `out` property defines the output collection to which results of the map-reduce action should be written. Parallel to the `out` option property classically passed into the db._collection_.mapReduce command, this can specify either a collection name or an `action`.

In most cases, this parameter will be passed to the raw `mapReduce` command unmodified.

When the `out` property is not specified, a *warning* is emitted and the property will default to `{reduce: db.results.*action.collection*}`.

Example:

```
{
	"collection":	"aCollection",
	"map":			function () {},
	"reduce":		function (key, values) {},
	"name":			"aName",
	"out":			{ "reduce": "outCollection" }
}
```

For more information on the `out` options, please have a look at [the mongo docs on the `out` options](http://docs.mongodb.org/manual/reference/command/mapReduce/#out-options).

### The `jsMode`, `verbose`, `scope`, `finalize`, `sort` and `limit` properties ###

These options are all passed through to the raw `mapReduce` command. For more information on these properties, please have a look at [the official docs](http://docs.mongodb.org/manual/reference/command/mapReduce/).

For the `finalize` property, pay special attention to the [requirements for the `finalize` function](http://docs.mongodb.org/manual/reference/command/mapReduce/#requirements-for-the-finalize-function) as declared in the official documentation.

Also note that the `scope` document is relevant to the `map` and `reduce` functions as well, specifying further global variables accessible from their context.

As a reference for how to add these properties, here's a small example.

```
{
	"collection":	"aCollection",
	"map":			function () {},
	"reduce":		function (key, values) {},
	"name":			"aName",
	"scope": 		{ "var1": someVar, "var2": anotherVar },
	"finalize":		function (key, reducedValue) { return reducedValue; },
	"sort":			{},
	"limit": 		{},
	"jsMode":		false,
	"verbose":		true
}
```

_Caveat_: Using the `scope` property requires releasing a number of variables into the global namespace. In this case, `someVar` and `anotherVar` are expected to be resolvable in the global namespace.

### The `pre` and `post` properties ###

The `pre` and `post` properties allow defining pre- and postprocessing functions which will be called as the first and lasts steps in the map-reduce chain respectively.

The `pre` and `post` functions are both called with their scope set to the _action object_, so they both have complete, read-write access to the current action, as well as the `db` object. Note that only modifications made to the `previous` property will be saved to the database, unless you save modifications manually within the `pre` and/or `post` processing functions. Changes made to the _action object_ within the `pre` function will still be there when the `post` function is called.

Prototype example:

```
{
	"collection":	"aCollection",
	"map":			function () {},
	"reduce":		function (key, values) {},
	"name":			"aName",
	"pre":			function() { //this refers to the action object },
	"post":			function() { //this refers to the action object }
}
```

The `pre` and `post` functions may but need not be both declared.

### The `lastrun` property ###

The `lastrun` property is automatically created and/or updated, and contains the timestamp (epoch in milliseconds) of the last time this action was executed.

### The `interval` property ###

With the `interval` property, an action can be turned into a periodically executing action. The `interval` specifies the _minimum_ number of milliseconds between consecutive runs.

If `action.interval + action.lastrun < currentTimestamp`, the action is executed.

_Note_: this implies that it is necessary to have the `lastrun` property declared and set. This can be done either manually, or by setting the `force` property to true to make it execute a first time instantly, after which the `lastrun` property will be automatically set.

```
{
	"collection":	"aCollection",
	"map":			function () {},
	"reduce":		function (key, values) {},
	"name":			"aName",
	"interval":		3600000, 	// run this every hour
	"last run":		0			// starting at the next possible moment
}
```

### The `previous` property ###

After each run, the `previous` property will be populated with information regarding the finished run. The `previous` property typically has the following schema:

```
{
	"timestamp":		0, // timestamp as an epoch in milliseconds, typically the same as the last run property
	"result":		{
		"out":			"outCollection", // output collection
		"timeMillis":	5, // time in milliseconds the action mapReduce command took
		"ok":			1, // 1 if the mapReduce command finished successfully, 0 otherwise
		"counts":		{  // note that if the verbose property is set to false, this will be {}
			"input": 	0, // the number of input documents to the map-function
			"emit":		0, // the number of documents the map-function emitted
			"reduce":	0, // the number of calls to the reduce function
			"output":	0  // the final number of documents this mapReduce run generated. This can be non-zero even if input, emit and reduce were 0, when the out-action is reduce.
		}
	}
}
```

_Note_: If extra properties are defined on this document, their values will be preserved even after the `timestamp` and `result` properties are updated. This makes it possible to save extra information about the previous run in, for example, the `pre` and `post` functions, or use information from this document in the `queryf` function.

_Note_: When the `previous` property evaluates to false, we assume a `reset`, meaning that if the `out` action is either simply a collection name, or either a `reduce` or `merge` action, we will clear this collection before running this action. Use with care.

### The `type` property ###

Another generated property, this property gives information on the _type_ of the last run. This will either be `auto`, signifying that this action was run by a combination of the `interval` and `lastrun` properties or by setting `force` to `true`, or `manual` when this action was executed manually (see the section on the `MapReducer` object for more information).

### The `incremental` property ###

The `incremental` property allows marking an action as an _incremental map-reduce_ action. An incremental map-reduce action is special in that it will only look at new documents, and either merge, replace or reduce the output with the existing output.

Typically, this is achieved by passing a `query` option to the raw `mapReduce` command, selecting only newer documents. The typical use case is to have a query along the lines of `{ "*timestamp field*": { $gt: "timestamp of last run" } }`, with an `out` action along the lines of `{ "reduce": "outputCollection" }`.

This entire use-case can be simulated by setting the `incremental` property, with as the value the path to a field on input documents containing a timestamp as an epoch in milliseconds.

Example:

```
{
	"collection":	"aCollection",
	"map":			function () {},
	"reduce":		function (key, values) {},
	"name":			"aName",
	"incremental":	"timestamp"
}
```

If no `out` action is specified, it will default to `{ "reduce": "results.*action.name*" }`. If the `out` option is set to simply a collection-name, it will be converted to a reduce action on the specified collection. If the `out` action is specified as a `reduce`, `replace` or `merge` action, it will be left untouched.

_Note_: take special care when forcing a reset by emptying the `previous` property. The `query`, regardless of whether is was specified manually or auto-generated by the `incremental` property, will be ignored.

_Note_: all options generated by the `incremental` property will be overridden by user-specified properties.

### The `query` and `queryf` properties ###

The `query` and `queryf` properties both allow for specifying a query that will select which input documents to use and which input documents to ignore, for example for doing incremental map-reduces.

Since such `query` documents typically contain operators such as `$gt`, they cannot be saved in the database as a raw document: field names in the database cannot start with `$` to avoid confusion between field-names and operators. As such, we allow specifying a `query` document in two distinct ways.

Firstly, you may specify the `query` property which will be interpreted as a json string. Since this happens by `eval`'ing the property, it is also possible to enter a function as a string here which returns a valid query document.

Oftentimes, you may wish to have access to the _action object_ when creating the query document, for example to gain access to a saved timestamp or object-id. For this purpose, there is also the possibility of specifying a `queryf` property, which should be a function returning a valid query document. The scope of this function when executing - similar to the `pre` and `post` functions - is set to the _action object_, granting you access to, amongst others, the `previous` property.

When both a `query` and `queryf` property are specified, a warning will be emitted, the `query` property will be ignored and the `queryf` function will be used.

### Overview ###

Here we provide an overview of all possible (meaningful) properties that can be set on action objects. Of course you may add your own properties, but be aware not to accidentally specify one of the following.

| Property       | Type         | Generated   | Value    | Function                           |
|----------------|--------------|-------------|----------|------------------------------------|
| __name__       | required     | no          | string   | Identify and call actions by name. |
| __collection__ | required     | no          | string   | Which collection to run on.        |
| __map__        | required     | no          | function | A JavaScript function that associates or “maps” a value with a key and emits the key and value pair.|
| __reduce__     | required     | no          | function | A JavaScript function that “reduces” to a single object all the values associated with a particular key. |
| __force__      | optional     | yes [1]     | boolean  | Force an action to run.            |
| __out__        | required [2] | no          | string/object | Specified the output collection/action |
| __jsMode__     | optional     | no          | boolean  | Specifies whether to convert intermediate data into BSON format between the execution of the map and reduce functions |
| __verbose__    | optional     | no          | boolean  | Specifies whether to include the timing information in the result information. The verbose defaults to true to include the timing information. |
| __scope__      | optional     | no          | document | Specifies global variables that are accessible in the map , reduce and the finalize functions. |
| __finalize__   | optional     | no          | function | A JavaScript function that follows the reduce method and modifies the output. |
| __sort__       | optional     | no          | document | Sorts the input documents. |
| __limit__      | optional     | no          | document | Specifies a maximum number of documents to return from the collection. |
| __pre__        | optional     | no          | function | A Javascript function executed as a pre-processing step with the scope set to the _action object_. |
| __post__       | optional     | no          | function | A Javascript function executed as a post-processing step with the scope set to the _action object_. |
| __lastrun__    | optional     | yes         | integer  | Timestamp as an epoch in milliseconds of the last time this action was run. Used together with the `interval` property to define recurring actions. |
| __interval__   | optional     | no          | integer  | Minimal number of milliseconds between to consecutive runs. Used in conjunction with the `lastrun` property. |
| __previous__   | optional     | yes         | object   | Object with some information on the last run. Can be modified by pre- and post-processing functions |
| __type__       | optional     | yes         | string   | Specifies the way in which this action was last called. Either "auto" or "manual". |
| __incremental__| optional     | no [3]      | string   | Specifies a field on the input documents containing a timestamp, for automatically generated incremental map-reduce actions. |
| __query__      | optional     | no          | string   | JSON string describing a valid query document for limiting input documents. |
| __queryf__     | optional     | no          | function | A Javascript function that returns a valid query document. Called with its scope set to the _action object_. |

_Notes:_

1. This is set to false after each run, regardless of whether it was set before.
2. Although it is "required", omitting will result in a recoverable *warning*, with the option defaulting to `{ reduce: "results.*action.name*" }`.
3. Though it is not a generated property, it generates a number of properties when set.

## The `MapReducer` object ##

The `MapReducer` object offers functionality for executing map-reduce _action objects_ as they are defined above.

Internally, it has a number of helper methods for extracting various bits and pieces of necessary information, but for the end-user, generally speaking, only four methods are of importance.

When the mongoReducer framework is loaded and a mongo shell, ensuring that a MapReducer object has been instantiated can be achieved by calling the `initMapReducer()` convenience function. This will ensure all dependencies have been constructed, as well as release a MapReducer object into the global namespace.

### Relevant methods ###

#### `MapReducer.runAction(object)` ####

When the `runAction` method is passed an _action object_, it will execute this action, regardless of `force`, `interval` and `lastrun` properties.

#### `MapReducer.run(string)` ####

Convenience function that allows running an action by specifying the name.

Its inner workings are simple - look up an action with a matching name in `db.mapreduce`, and pass it to `MapReducer.runAction()`.

#### `MapReducer.execActions(Array of objects)` ####

This method should be passed an Array of _action objects_. It will iterate through those objects, figure out whether they should be run (using the `force`, `lastrun` and `interval` properties) and executes them.

#### `MapReducer.exec()` ####

The main entry point. This will look up all _action objects_, pass them through to `MapReducer.execActions()` and keep a number of statistics.

## The `Poller` object ##

The `Poller` object is responsible for starting and maintaining the polling loop and comes with some logging functionality.

### The "pid" ###

As soon as the `Poller` object is instantiated, it will generate a "pid" for itself, equivalent to the process-id of a regular process. However, it will be in an inactive state until specified otherwise. This "pid" is in fact just an instance of _ObjectId_, so uniqueness is guaranteed, and information about the parent process, time of generation, etc. can be construed from the pid.

This pid is used for uniquely identifying the `Poller` instance.

### Logging ###

The `Poller` instance has a number of logging-related methods. First and foremost, there is a `log(level, message, data)` method, which allows logging a message with optional data with a certain log level. Depending on the level and the configuration, this message and data will then be written to the console and/or/nor the database in the `db.mapreduce.log` collection.

There are 4 log-levels defined:

- 0: DEBUG
- 1: INFO
- 2: WARNING
- 3: ERROR

To filter log-messages by level, please see the section on configuration for the `Poller` object.

### The `db.mapreduce.run` collection ###

As soon as the polling loop is started by calling `Poller.start(body, scope)` (body should be a function, for example `MapReduce.exec`, and scope should be the scope that this function should run in, for example `MapReduce`), it will save its "pid" in the `db.mapreduce.run` collection.

This collection, much like a typical run-file in the linux world, will hold some "process information". This collection may only have one single entry which is - by convention - identified by an `_id` of _unique_. All other entries in this collection are ignored.

When the running `Poller` instance cannot find its own pid in the `db.mapreduce.run` collection, it will exit. This may happen when a new `Poller` is started (which will overwrite the existing document with `_id = "unique"`) or when the user force-kills the `Poller` instance by removing the document from the `db.mapreduce.run` collection.

The `db.mapreduce.run` collection will, when used in conjunction with the MapReducer object, typically contain a document with the following schema:

```
{
	"_id":			"unique",
	"pid":			ObjectId("…"),
	"status":		"sleeping", // or "running" if you happen to peek in the collection while the MapReducer is running
	"time":			0, // the number of milliseconds the last MapReducer.exec() took
	"actions":		0, // the number of map-reduce actions that were executed in the last run
	"total actions":0, // the total number of map-reduce actions executed within this Poller instance
	"ping":			0  // whenever the Poller is running, it will "ping" the database to let you know it is still alive
}
```

### Settings ###

There are a number of configuration options for the `Poller` object. All configuration goes into the `db.mapreduce.settings` collection and is read every time the `Poller` instance polls the database. As such, `Poller` instances can be reconfigured while running.

#### Interval ####

Just as the `interval` property of an _action object_ configures the minimal amount of milliseconds between two consecutive runs of an action, defining a document in the `db.mapreduce.settings` collection with an `_id` set to "interval" and a `value` set to a number allows managing the minimal number of milliseconds that should pass between two consecutive polls.

If this is not set, the `Poller` will default to _1000_ milliseconds.

Example configuration:

```
db.mapreduce.settings.insert({ "_id": "interval", "value": 1000 });
```

#### Log control ####

By default, the `Poller` object will log all messages with a level of `DEBUG` or higher to the database in the `db.mapreduce.log` collection, as well as all messages with a level of `INFO` or higher to the console.

_Note:_ for performance, it is best to make `db.mapreduce.log` a capped collection.

This can be configured by creating a document with `_id` "loglevel" which should contain a document with 2 key-value pairs; one specifying the `db` log level, and one specifying the `console` log level.

The default configuration is as follows:

```
db.mapreduce.settings.insert({
	"_id":	"loglevel",
	"value": {
		"db":		0, // DEBUG
		"console":	1  // INFO
	}
});
```

## Installation and Usage ##

There are a number of possible ways to set up and use _MongoReducer_.

_MongoReducer_ can be started from within a mongo-shell. For example:

```
$ mongo
MongoDB shell version: 2.4.3
connecting to: test
> load("mongoReducer.js")
> start()
1369401567992   519f68dfde45fcf85dcd459c        Starting
```

Instead of loading the Javascript file from within the shell, one could also pass it as an argument to the `mongo` command:

```
$ mongo mongoReducer.js --shell
MongoDB shell version: 2.4.3
connecting to: test
type "help" for help
> start()
1369401666456   519f6942b78f5c8443e0c729        Starting
```

Alternatively, one could run the `setup()` function included in _MongoReducer_ to save all needed functions in the `system.js` collection. Afterwards, one could start the Poller from within a mongo shell or do something akin to the following:

```
$ echo "setup()" | mongo mongoReducer.js --shell
MongoDB shell version: 2.4.3
connecting to: test
type "help" for help
Saving initPoller(), initMapReducer() and start() in system.js.
bye
$ echo "db.loadServerScripts(); start()" | mongo > log &
[1] 10861
```

Or, if you don't wish to store the _MongoReducer_ functions in the `system.js` collection:

```
$ echo "start()" | mongo mongoReducers.js --shell > log &
```

## License ##

_MongoReducer_ is released under the Creative Commons Attribution 3.0 Unported license.

For full details, please read the included `LICENSE` file.