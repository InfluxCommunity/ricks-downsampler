# Summary
A project to make it easy to downsample with InfluxDB 3.0. It handles running a downsampling task of up to 1 minute resolution, but can handle hour and day intervals as well.

# Envars
This code is designed to be run in a container, and uses a collection of environment variables to configure it.

# Required Envars
 * RUN_INTERVAL - A string in the form of an InfluxQL time interval. i.e. an integer then a time specifier. Examples: 1m, 6h, 7d. Allowable time specifiers are m (munutes), h (hours), and d (days).
  * SOURCE_HOST - the host of the influxdb instance from which you are downsampling.
  * SOURCE_DB - the database that you want to downsample.
  * SOURCE_TOKEN - a token that read the database you want to downsample.
  * SOURCE_MEASUREMENY - the name of the measurement (also called "table") to downsample.
  * SOURCE_ORG - the organization (only required if you are using InfluxDB Cloud Serverless).

 # Option Envars
 * RUN_PREVIOUS_INTERVAL - If "true" the task will immediately run the previous time internval. See below.

 ## Logging
 You can set up an influxdb instance to log runs of the downsampling job. To do that, you need to pass in the following envars:
  * LOG_HOST - the host of the influxdb instance you are using for logging the task runs. This can be the same or a different instance than the one that hosts the data you are downsampling.
  * LOG_DB - the database where you want to store the log information.
  * LOG_TOKEN - a token that can write to that log.
  * LOG_ORG - this is only required if you are using InfluxDB Cloud Serverless. 

  If you leave out any of the first three envars (LOG_HOST, LOG_DB, or LOG_TOKEN), logging will simply be skipped.

 # Time Behvavior
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