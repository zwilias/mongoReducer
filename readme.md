# MongoReducer #

__MongoReducer__ allows running mongo's mapreduce functionality from within mongo.

Specifically, this is a framework of sorts, built using convention over configuration in mind, which allows saving a complete map-reduce actions, with its mapper and reducer and all other configuration and settings in the `db.mapreduce` collection and running it with a single command.

Furthermore, __MongoReducer__ comes with a polling loop which polls the `db.mapreduce` collection (amongst others) and will run all map-reduce actions found within, at specified time-intervals, without the need for a complicated setup involving a number of cronjobs and a host of other scripts for actually executing the map-reduce actions.

To this end, __MongoReducer__ is split into two main parts, the `Poller` object and the `MapReduce` object. `Poller` is an instance of the `_Poller` class, whereas `MapReduce` is an instance of the `MapReduce` class.

Let’s take a look at all the parts making up a complete __MongoReducer__ installation, from the inside out.

## An action object ##

So called “action objects” are simply a name we use to refer to any document stored in the `db.mapreduce` collection.

A minimal such _action_ object looks as follows:

```json
{
	"collection":	“aCollection”,
    "map": 		function () {},
    "reduce": 	function (key, values) {},
    "name": 		“aName”
}
```

This defines a map-reduce action on the `db.aCollection` collections with the mapping function `function () {}` and the reducing function `function (key, values) {}`. As a convenience, we also name the action, in this case calling it `aName`. We actually enforce having a `name` property, which is used in various logging functions, as well, in this case, for determining the output collection since none was specified.

However, there are plenty of other properties that can be set to influence the behaviour of this map-reduce action.

### The `force` property ###

The `force` property allows forcing an action to be executed the next time the polling loop encounters this action. If the `force` property is not set, it defaults to `false`.

Example:

```json
{
	"collection":	“aCollection”,
    "map": 		function () {},
    "reduce": 	function (key, values) {},
    "name": 		“aName”,
    “force”:		true
}
```

This forces the `aName` action to be executed on the next run. It will afterwards automatically be set to `false`.

### The `out` property ###

