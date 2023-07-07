A project to make it easy to downsample with InfluxDB 3.0.

# Required Envars
 * RUN_INTERVAL - A string in the form of an InfluxQL time interval. i.e. an integer then a time specifier. Examples: 1m, 6h, 7d. Allowable time specifiers are m (munutes), h (hours), and d (days).