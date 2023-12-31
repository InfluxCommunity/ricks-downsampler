# Summary

A project to make it easy to downsample with InfluxDB 3.0. It handles running a downsampling task of up to 1 minute resolution, but can handle hour and day intervals as well.

# Envars

This code is designed to be run in a container, and uses a collection of environment variables to configure it.

## Required Envars
* `RUN_INTERVAL` - A string in the form of an InfluxQL time interval. i.e. an integer then a time specifier. Examples: 1m, 6h, 7d. Allowable time specifiers are m (minutes), h (hours), and d (days).
* `SOURCE_HOST` - the host of the influxdb instance from which you are downsampling.
* `SOURCE_DB` - the database that you want to downsample.
* `SOURCE_TOKEN` - a token that read the database you want to downsample.
* `SOURCE_MEASUREMENT` - the name of the measurement (also called "table") to downsample.
* `TARGET_DB` - the name of the database to which to write the downsampled data

## Optional Envars
* `RUN_PREVIOUS_INTERVAL` - If "true" the task will immediately run the previous time interval. See below.
* `NO_SCHEMA_CACHE` - If "true" the task will not persist schema information between runs. Defaults to false. This allows the downsampling to adapt to schema changes on the fly, but incurs a performance penalty of 2 extra metadata queries. Ignored if TAGS, INCLUDE_FIELDS, or EXCLUDE_FIELDS was set.
* `INCLUDE_TAGS` - A comma separated list of tags (no spaces) to include in the group clause. If ommitted, all tags will be included. If included, `NO_SCHEMA_CACHE` will be ignored. 
* `INCLUDE_TAG_VALUES` - A bit of JSON defining a dictionary of tag values to tag keys to include. For xample: `'{"A":["a","c"],"B":["b"]}'` will match any rows where the tag key A has a tag value of a or c, and where tag key B has a value of b.
* `TASK_ID` - use for the task_id tag when logging. If not supplied, a random id will be generated on startup.
* `TARGET_HOST` - if you wish to downsample to a different influxdb instance, you can provide a different host
* `TARGET_TOKEN` - if you wish to supply a different write token, you can supply it, especially if you are targeting a different host.
* `TARGET_MEASUREMENT` - if you wish to supply a different name for downsampled measurement, otherwise, it will use the name of the source measurement.
* `BACKFILL_START` - if you wish to run the process starting from a point in the pass you can include a timestamp in ISO 8601 format (ex: "2023-07-24T16:20:00Z"). The process will go back to that time, calculate the next runtime as describe below, and downsample each internval serially until it reaches the current time (or the time specified in BACKFILL_END), and then process will quit. 
* `BACKFILL_END` - Time to stop the downsampling in ISO 8601 format.
* `RUN_ONCE` - If 'true' will run the downsampling task once, and then quit. Respects `RUN_PREVIOUS_INTERVAL`.
* `MAX_WRITE_RETRIES` - Specifies how many retries for each batch of data in case of a write failure. Writes are retried with a simple exponential backoff, per batch. Defaults to 5.
* `AGGREGATE` - Specify an aggregate function to apply to all fields. Defaults to MEAN. It should support all aggregates and selectors currently [documented](https://docs.influxdata.com/influxdb/cloud-serverless/reference/influxql/feature-support/#function-support) to be supported by InfluxQL.
* `CONTAINER_LOG_LEVEL` - Set the verbosity of the logs coming from the container itself. Note that this does not impact the task run logs sent to InfluxDB, this is only for the logs sent to stdout and stderr. This is useful for controlling the amount of logs being written to logging services, and thus helpful for controlling costs. It can be set to one of the following values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. `DEBUG` logs all messages, while `CRITICAL` only logs the most severe messages. If `CONTAINER_LOG_LEVEL` is not set, the default level is `INFO`, which logs informational messages and any message of higher severity such as warnings and errors. `ERROR` is recommended for running the container in a hosted container runtime environment.


# Logging

The downsampling tasks will log performance information and errors, assuming that you provide it with the information that it needs to write those logs to an InfluxDB instance. You do this by providing the following envars:

## Logging Envars

* `LOG_HOST` - the host of the influxdb instance you are using for logging the task runs. This can be the same or a different instance than the one that hosts the data you are downsampling.
* `LOG_DB` - the database where you want to store the log information.
* `LOG_TOKEN` - a token that can write to that log.
* `LOG_ORG` - this is only required if you are using InfluxDB Cloud Serverless.

If you leave out any of the first three envars (`LOG_HOST`, `LOG_DB`, or `LOG_TOKEN`), logging will be skipped.

## Logging Schema

### Tags

* `error` - can be "query", or "write", depending on where the error was encountered.
* `task_host`- the host of the running task.
* `interval` - the interval of the downsampling (such as 1m, 10m, 1h, etc...).
* `task_id` - identifies the running downsampling task.
  
### Fields

* `query_gen_time` - the amount of time spent generating the query
* `query_time` - the amount of time executing the query and converting the results into a dataframe
* `row_count` - the number of rows produced from the query
* `start` - the beginning of the time window for the downsampling
* `stop` - the end of the time window for the downsamping
* `source_host` - the hostname of the InfluxDB instance that has the data being downsampled
* `source_measurement` - the name of the measurement (table) being downsampled
* `exception` - any error message associated and error

# Time Behaviour

The first run of the downsampling will occur based on a combination of wall time and the specified run interval.

## Minutes

For run times specified in minutes, it will assume a base schedule at the start of the previous hour, and then when it runs will assume the end time range of the query is that hour plus a multiple of the interval. Time intervals expressed in 60 minutes or more will start at the next hour. If that will put the next run past the next hour, the first run will be exactly on the next hour. Subsequent runs will be exactly the run interval apart.

Some examples:

* Time Interval is 10m, and the current time is 11:05, the first run will be at 11:10, and subsequent runs at 11:20, 11:30, etc...
* Time interval is 1m, and the current time is 12:11, the first run will be at 12:12, and subsequent runs at 12:13, 12:14, etc...
* Time interval is 61m, and the current time is 12:59, the first run will be at 13:00, and subsequent runs at 14:01, 15:02, etc...
* Time interval is 10m, and the current time is 23:59, the first run will be at 00:00 the next day (1 minute later), then at 00:10, 00:20, etc...

## Hours

For run times specified in hours, the first run will always occur exactly at the start of the next hour, and then at intervals of the specified hours after.

## Days

For run times specified in days, the first run will be at mindnight, and then at intervals of the specified days after.

## RUN_PREVIOUS_INTERVAL

If set to true, the downsampling will run immediately for the previous interval and then proceed as normal.

Some examples with ```RUN_PREVIOUS_INTERVAL=true```:

* Time Interval is 10m, and the current time is 11:05, the first run will be at 11:00, and subsequent runs at 11:10, 11:20, etc...
* Time interval is 1m, and the current time is 12:11:59, the first run will be at 12:11:00, and subsequent runs at 12:12, 12:13, etc...
