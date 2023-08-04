import os
import re
import socket
import time
import string
import random
import logging
from pythonjsonlogger import jsonlogger

from dateutil.parser import parse
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timezone
from influxdb_client_3 import InfluxDBClient3, Point, SYNCHRONOUS, write_client_options
from schedule_calculator import get_next_run_time, get_then
from schema_configuration import populate_fields, populate_tags, populate_tag_values
from influxql_generator import get_query

logging_client = None
source_client = None
target_client = None
source_measurement = ""
target_measurement = ""
fields = None
tags = None
ignore_schema_cache = False
source_host = ""
task_id = "" 
interval = ""
aggregate = ""
tag_values = None
logger = None

def parse_interval(explicit_interval=None):
    global interval
    if explicit_interval is None:
        interval = os.getenv('RUN_INTERVAL')
  
    match = re.fullmatch(r'(\d+)([mhd])', interval)
    if match is None:
        logger.critical(f"invalid interval format: {interval}")
        exit(1)
    t = int(match.group(1))
    if t < 1:
        logger.critical(f"invalid interval format: {interval}")
        exit(1)
    return t, match.group(2)

def setup_aggregate():
    global aggregate
    aggregate = os.getenv("AGGREGATE", "MEAN")
    allowed_aggregates = ["count","distinct","mean","median","stddev","sum","first","last","max","min"]

    if aggregate.lower() not in allowed_aggregates:
        help_url  = "https://docs.influxdata.com/influxdb/cloud-serverless/reference/influxql/feature-support/#function-support"
        logger.critical(f"aggregate {aggregate} not allowed. Only {allowed_aggregates} accepted. See {help_url}")
        exit(1)

def setup_tags_and_fields():
    global fields
    global tags
    global tag_values
    global ignore_schema_cache
    global source_client
    global source_measurement

    if fields is None or ignore_schema_cache:
        fields = populate_fields(source_client, source_measurement)

    if tags is None or ignore_schema_cache:
        tags = populate_tags(source_client, source_measurement)
    
    if tag_values is None or ignore_schema_cache:
        tag_values = populate_tag_values()

def get_down_sampled_data(query):
    if source_client is None or source_measurement == "":
        logger.critical("Source InfluxDB instance not defined. Exiting ...")
        exit(1)
    else:
        try:
            reader = source_client.query(query, language="influxql", mode="chunk")
            return (True, reader)
        except Exception as e:
            return (False, str(e))

def run(interval_val, interval_type, now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    now = now.replace(second=0,microsecond=0)
    then = get_then(interval_val, interval_type, now)

    logger.info(f"Running job for {then.strftime('%Y-%m-%dT%H:%M:%SZ')} to {now.strftime('%Y-%m-%dT%H:%M:%SZ')}. Time stamp will be {then.strftime('%Y-%m-%dT%H:%M:%SZ')}.")

    # generate the query
    start_time = time.time()

    # setting up tags and fields in the run function in case the user
    # has set NO_SCHEMA_CACHE=true, it can recalcuate the schema on successive runs
    setup_tags_and_fields()
    log_tags = [("task_id", task_id),
                ("source_host", source_host),
                ("source_measurement",source_measurement),
                ("target_measurement",target_measurement),
                ("interval", interval),
                ("task_id", task_id),
                ("task_host",socket.gethostname())]
    
    log_fields =  [("start", then.strftime('%Y-%m-%dT%H:%M:%SZ')),
                    ("stop", now.strftime('%Y-%m-%dT%H:%M:%SZ'))]

    query = get_query(fields, source_measurement, then, now, tags, interval, aggregate, tag_values)
    logger.debug(f"running query: {query}")
    end_time = time.time()

    query_gen_time = end_time - start_time
    log_fields.append(("query_gen_time",query_gen_time))

    # execute the query
    start_time = time.time()

    # get_downsampled data will return an arrow stream reader if successful
    # if success == false, reader will be an exception string
    success, reader = get_down_sampled_data(query)
    end_time = time.time()
    query_time = end_time - start_time
    log_fields.append(("query_time", query_time))

    if not success:
        exception_string = reader
        log_tags.append(("error","query"))
        log_fields.append(("exception",exception_string))
        log("task_log", log_tags, log_fields)
        logger.error(f"Downsampling job failed with {exception_string}")
        return


    #write the downsampled data
    start_time = time.time()

    # if success if false, result is an error string
    # otherwise results is the count of rows written
    success, result, row_count, retries  = write_downsampled_data(reader)
    end_time = time.time()
    write_time = end_time - start_time
    log_fields.append(("write_time", write_time))
    if not success:
        log_tags.append(("error","write"))
        log_fields.append(("exception",result))
        log_fields.append(("row_count", row_count))
        log_fields.append(("retries", retries))
        log("task_log", log_tags, log_fields)
        logger.error(f"Downsampling job failed with {result}, {retries} retries, {row_count} rows written")
        return
    log_fields.append(("retries", retries))
    log_fields.append(("row_count", row_count))
    #log the results
    log("task_log", log_tags, log_fields)
    logger.info(f"Downsampling job run successfully for {row_count} rows")

def write_downsampled_data(reader):
    row_count = 0
    retries = 0
    try:
        while True:
            batch, buff = reader.read_chunk()

            df = batch.to_pandas()
            row_count += df.shape[0]
    
            if 'iox::measurement' in df.columns:
                df = df.drop('iox::measurement', axis=1)

            max_retries = int(os.getenv("MAX_WRITE_RETRIES", 5))
            for i in range(max_retries):
                try:
                    target_client.write(record=df,
                                        data_frame_measurement_name=target_measurement,
                                        data_frame_timestamp_column="time",
                                        data_frame_tag_columns=tags)
                    # if write is successful, break the retry loop
                    break
                    retries += 1

                except Exception as e:
                    logger.error(f"Error on write attempt {i+1}: {str(e)}")
                    wait_time = (2 ** i) + random.random()  # exponential backoff with jitter
                    time.sleep(wait_time)
                    # if this was the last retry and it still failed, re-raise the exception
                    if i == max_retries - 1:
                        raise

    except StopIteration as e:
        return True, None, row_count, retries
    
    except Exception as e:
        logger.error(f"write failed with exception {str(e)}")
        return False, str(e), row_count, retries

def log(measurement, tags, fields):
    logger.info(f"logging: {measurement},{tags},{fields}")
    if logging_client is None:
        logger.info("No logging client specified, skipping logging")
        return
    point = Point(measurement)
    for field in fields:
        point.field(field[0], field[1])
    for tag in tags:
        point.tag(tag[0], tag[1])
    try:
        logging_client.write(point)
    except Exception as e:
        logger.error(f"Logging failed with exception {str(e)}")

def setup_source_client():
    host = os.getenv('SOURCE_HOST')
    db = os.getenv('SOURCE_DB')
    token = os.getenv('SOURCE_TOKEN')
    org = os.getenv('SOURCE_ORG', 'none')
    global source_measurement
    source_measurement = os.getenv('SOURCE_MEASUREMENT')

    global source_host
    source_host = host

    if None in [host, db, token, source_measurement]:
        logger.critical("Source host, database, token, or measurement not defined. Aborting ...")
        exit(1)
    else:
        global source_client
        
        source_client = InfluxDBClient3(host=host, database=db, token=token, org=org)

def setup_target_client():
    global source_host
    global source_measurement
    global target_measurement

    host = os.getenv('TARGET_HOST', source_host)
    db = os.getenv('TARGET_DB')
    token = os.getenv('TARGET_TOKEN', os.getenv('SOURCE_TOKEN'))
    org = os.getenv('TARGET_ORG', 'none')
    target_measurement = os.getenv('TARGET_MEASUREMENT', source_measurement)

    if None in [host, db, token, source_measurement]:
        logger.critical("Target host, database, token, or measurement not defined. Aborting ...")
        exit(1)
    else:
        global target_client
        wco = write_client_options(write_options=SYNCHRONOUS)
        target_client = InfluxDBClient3(host=host, database=db, token=token, org=org, write_client_options=wco)

def setup_container_logging():
    host = os.getenv('LOG_HOST')
    db = os.getenv('LOG_DB')
    token = os.getenv('LOG_TOKEN')
    org = os.getenv('LOG_ORG', 'none')

    if None in [host, db, token]:
        logger.info("Log host, database, or token not defined. Skipping logging.")
    else:
        global logging_client
        logging_client = InfluxDBClient3(host=host, database=db, token=token, org=org)

def setup_task_id():
    global task_id
    task_id = os.getenv('TASK_ID', ''.join(random.choices(string.ascii_uppercase + string.digits, k=7)))

def setup_no_schema_cache_option():
    global ignore_schema_cache
    ignore_schema_cache_opt = os.getenv('NO_SCHEMA_CACHE', "false")
    ignore_schema_cache  = ignore_schema_cache_opt.lower() in ['true', '1']

def backfill(interval_val, interval_type):
    backfill_start = os.getenv('BACKFILL_START')
    backfill_end = os.getenv('BACKFILL_END')

    
    if backfill_start is not None:
        if backfill_end is None:
            backfill_end = datetime.now(timezone.utc)
        else:
            backfill_end = parse(backfill_end).astimezone(timezone.utc)

        try:
            then =  parse(backfill_start).astimezone(timezone.utc)

            while then < backfill_end:
                then = get_next_run_time(interval_val, interval_type, then)
                run(interval_val, interval_type, now=then)
            exit(0)

        except Exception as e:
            logger.critical(f"Parsing backfill failed with exception {str(e)}")
            exit(1)

def run_previous_interval(interval_val, interval_type):
    run_previous_opt = os.getenv('RUN_PREVIOUS_INTERVAL', 'false')
    logger.debug(f"RUN_PREVIOUS_INTERVAL set to: {run_previous_opt}")
    run_previous = run_previous_opt.lower() in ['true', '1']
    logger.debug(f"RUN_PREVIOUS_INTERVAL is {run_previous}")
    if run_previous:
        logger.info(f"Running previous interval")
        now = get_next_run_time(interval_val, interval_type, run_previous=True, now=datetime.utcnow())
        run(interval_val, interval_type, now=now)
    else:
        logger.info(f"Skipping previous internval")

def schedue_and_run(interval_val, interval_type):
    # set the start date based on the interval type
    # set the values for the intervals
    start_date = datetime.now()
    interval_settings = {"days":0, "hours":0,"minutes":0}
    if interval_type == "m":
        start_date = start_date.replace(minute=0,second=0,microsecond=0)
        interval_settings["minutes"] = interval_val
    elif interval_type =="h":
        start_date = start_date.replace(minute=0,second=0,microsecond=0)
        interval_settings["hours"] = interval_val
    elif interval_type == "d":
        start_date = start_date.replace(hour=0,minute=0,second=0,microsecond=0)
        interval_settings["days"] = interval_val

    #specify and run the job
    scheduler = BlockingScheduler()
    scheduler.add_job(run, 
                      'interval', 
                        days = interval_settings["days"],
                        hours=interval_settings["hours"],
                        minutes=interval_settings["minutes"], 
                        seconds=0,
                        start_date=start_date,
                        args = [interval_val, interval_type],
                        max_instances=10
                        )
    scheduler.start()

def setup_logger():
    global logger
    logger = logging.getLogger()

    # Create a handler for STDOUT
    handler = logging.StreamHandler()

    # Format the log in JSON format
    formatter = jsonlogger.JsonFormatter()
    handler.setFormatter(formatter)

    # Set the log level
    logging_level = os.getenv("CONTAINER_LOG_LEVEL", "INFO")
    numeric_level = getattr(logging, logging_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {logging_level}')
    
    # Assign handler to the logger
    logger.addHandler(handler)
    logger.setLevel(numeric_level)
    logger.debug(f"Logger set to logging level: {logging_level}, {numeric_level}")

if __name__ == "__main__":
    # set up the logger
    setup_logger()

    interval_val, interval_type = parse_interval()

    # parse input and setup global resources
    setup_task_id()
    setup_source_client()
    setup_target_client()
    setup_container_logging()
    setup_no_schema_cache_option()
    setup_aggregate()

    # run as backfill job and excit if defined by the user
    backfill(interval_val, interval_type)

    # run the previous interval if requested by the user
    run_previous_interval(interval_val, interval_type)

    # start the job and run forever
    schedue_and_run(interval_val, interval_type)