A project to make it easy to downsample with InfluxDB 3.0.

# Required Envars
 * RUN_INTERVAL - A string in the form of an InfluxQL time interval. i.e. an integer then a time specifier. Examples: 1m, 6h, 7d. Allowable time specifiers are m (munutes), h (hours), and d (days).


 # Time Behvavior
 The first run of the downsampling will occur based on a combination of wall time and the specified run interval.

## Minutes
 For run times specified in minutes, it will assume a base schedule at the start of the previous hour, and then when it runs will assume the end time range of the query is that hour plus a multiple of the interval. Time intervals expressed in 60 minutes or more will start at the next hour. If that will put the next run past the next hour, the first run will be exactly on the next hour. Subsequent runs will be exactly the run interval apart.
 
 Some examples:
  * Time Interval is 10m, and the current time is 11:05, the first run will be at 11:10, and subsequent runs at 11:20, 11:30, etc...
  * Time interval is 1m, and the current time is 12:11, the first run will be at 12:12, and subsequent runs at 12:12, 12:13, etc...
  * Time interval is 61m, and the current time is 12:59, the first run will be at 13:00, and subsequent runs at 14:01, 15:02, etc...
  * Time interval is 10m, and the current time is 23:59, the first run will be at 00:00 the next day (1 minute later), then at 00:10, 00:20, etc...