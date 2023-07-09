import os
import re
import socket
import time
import string
import random
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
from influxdb_client_3 import InfluxDBClient3, Point, SYNCHRONOUS
from schedule_calculator import get_next_run_time, get_then
from schema_configuration import populate_fields, populate_tags
from influxql_generator import get_query

logging_client = None
source_client = None
target_client = None
source_measurement = ""
fields = None
tags = None
ignore_schema_cache = False
source_host = ""
task_id = "" 
interval = ""

def parse_interval(interval):
    match = re.fullmatch(r'(\d+)([mhd])', interval)
    if match is None:
        raise ValueError(f'Invalid format: {interval}')
    t = int(match.group(1))
    if t < 1:
        raise ValueError('Time period must be greater than 0')
    return t, match.group(2)

def setup_tags_and_fields():
    global fields
    global tags
    global ignore_schema_cache
    global source_client
    global source_measurement

    if fields is None or ignore_schema_cache:
        fields = populate_fields(source_client, source_measurement)

    if tags is None or ignore_schema_cache:
        tags = populate_tags(source_client, source_measurement)

def get_down_sampled_data(query):
    if source_client is None or source_measurement == "":
        print("Source InfluxDB instane not defined. Existing ...")
        exit(1)
    else:
        try:
            table = source_client.query(query, language="influxql")
            return (True, table.to_pandas())
        except Exception as e:
            return (False, str(e))

def run(interval_val, interval_type, now=None):
    global source_measurement
    global interval
    global fields
    global tags

    if now is None:
        now = datetime.utcnow()
    now = now.replace(second=0,microsecond=0)
    then = get_then(interval_val, interval_type, now)

    print(f"{then.strftime('%Y-%m-%dT%H:%M:%SZ')} to {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    start_time = time.time()

    setup_tags_and_fields()
    query = get_query(fields, source_measurement, then, now, tags, interval)
    end_time = time.time()

    query_gen_time = end_time - start_time

    start_time = time.time()
    success, result = get_down_sampled_data(query)
    end_time = time.time()
    query_time = end_time - start_time
    if not success:
        log_query_error(result, now, then, query_gen_time, query_time)
        return
    
    print(result)
    row_count = len(result)
 
    global logging_client
    try:
        log_run(now, then, query_gen_time, query_time, row_count)
    except Exception as e:
        print(f"Logging failed due to {str(e)}")

def log_run(now, then, query_gen_time, query_time, row_count):
    global interval
    global source_measurement
    global source_host
    global task_id

    if logging_client is not None:
        point = (Point("task_log")
         .field("start", then.strftime('%Y-%m-%dT%H:%M:%SZ'))
         .field("stop", now.strftime('%Y-%m-%dT%H:%M:%SZ'))
         .field("query_gen_time", query_gen_time)
         .field("query_time",query_time)
         .field("row_count", row_count)
         .tag("task_id", task_id)
         .tag("error", "none")
         .tag("source_host", source_host)
         .tag("source_measurement",source_measurement)
         .tag("interval", interval)
         .tag("task_host",socket.gethostname()))
        
        logging_client.write(point)

def log_query_error(result, now, then, query_gen_time, query_time):
    global interval
    global source_measurement
    global source_host
    global task_id

    if logging_client is not None:
        point = (Point("task_log")
        .field("start", then.strftime('%Y-%m-%dT%H:%M:%SZ'))
        .field("stop", now.strftime('%Y-%m-%dT%H:%M:%SZ'))
        .field("query_gen_time", query_gen_time)
        .field("query_time",query_time)
        .field("exception", result)
        .tag("error", "query")
        .tag("task_id", task_id)
        .tag("source_host", source_host)
        .tag("source_measurement",source_measurement)
        .tag("interval", interval)
        .tag("task_host",socket.gethostname()))

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
        print("Source host, database, token, or measurement not defined. Aborting ...")
        exit(1)
    else:
        global source_client
        source_client = InfluxDBClient3(host=host, database=db, token=token, org=org)

def setup_target_client():
    global source_host
    global source_measurement

    host = os.getenv('TARGET_HOST', source_host)
    db = os.getenv('TARGET_DB')
    token = os.getenv('TARGET_TOKEN', os.getenv('SOURCE_TOKEN'))
    org = os.getenv('TARGET_ORG', 'none')

    if None in [host, db, token, source_measurement]:
        print("Target host, database, token, or measurement not defined. Aborting ...")
        exit(1)
    else:
        global target_client
        target_client = InfluxDBClient3(host=host, database=db, token=token, org=org, write_client_options=SYNCHRONOUS)

def setup_logging():
    host = os.getenv('LOG_HOST')
    db = os.getenv('LOG_DB')
    token = os.getenv('LOG_TOKEN')
    org = os.getenv('LOG_ORG', 'none')

    if None in [host, db, token]:
        print("Log host, database, or token not defined. Skipping logging.")
    else:
        global logging_client
        logging_client = InfluxDBClient3(host=host, database=db, token=token, org=org)
        global interval
        interval = os.getenv('RUN_INTERVAL')

def setup_task_id():
    global task_id
    task_id = os.getenv('TASK_ID', ''.join(random.choices(string.ascii_uppercase + string.digits, k=7)))

def setup_no_schema_cache_option():
    global ignore_schema_cache
    ignore_schema_cache_opt = os.getenv('NO_SCHEMA_CACHE', "false")
    ignore_schema_cache  = ignore_schema_cache_opt.lower() in ['true', '1']

if __name__ == "__main__":
    # parse the user input
    interval = os.getenv('RUN_INTERVAL')
    interval_val, interval_type = parse_interval(interval)

    run_previous_opt = os.getenv('RUN_PREVIOUS_INTERVAL', 'false')
    run_previous = run_previous_opt.lower() in ['true', '1']

    setup_task_id()
    setup_source_client()
    setup_target_client()
    setup_logging()
    setup_no_schema_cache_option()

    if run_previous:
        now = get_next_run_time(interval_val, interval_type, run_previous=True, now=datetime.utcnow())
        run(interval_val, interval_type, now=now)

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
                        args = [interval_val, interval_type]
                        )
    scheduler.start()