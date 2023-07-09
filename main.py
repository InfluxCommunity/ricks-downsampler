import os
import re
import socket
import time
import string
import random
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
from influxdb_client_3 import InfluxDBClient3, Point
from schedule_calculator import get_next_run_time, get_then

logging_client = None
source_client = None
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

def populate_fields():
    global source_client
    global source_measurement
    global fields

    if source_client is None or source_measurement == "":
        print("Source InfluxDB instane not defined. Existing ...")
        exit(1)
    else:
        query = f'SHOW FIELD KEYS FROM "{source_measurement}"'
        fields_table = source_client.query(query, language="influxql")
        fields = dict(zip([f.as_py() for f in fields_table["fieldKey"]], 
                            [f.as_py() for f in fields_table["fieldType"]]))

def populate_tags():
    global source_client
    global source_measurement
    global tags
    
    if source_client is None or source_measurement == "":
        print("Source InfluxDB instane not defined. Existing ...")
        exit(1)
    else:
        query = f'SHOW TAG KEYS FROM "{source_measurement}"'
        tags_table = source_client.query(query, language="influxql")
        tags = tags_table["tagKey"]
    
def field_is_num(field_name, fields_dict):
    numeric_types = ['integer', 'float', 'double']
    field_type = fields_dict.get(field_name)
    if field_type in numeric_types:
        return True
    return False    

def generate_fields_string(fields_dict):
    query = ''
    for field_name, field_type in fields_dict.items():
        if field_is_num(field_name, fields_dict):
            if query != '':
                query += ',\n'
            query += f'\tmean("{field_name}") as "{field_name}"'
    return query

def generate_group_by_string(tags_list, interval):
    group_by_clause = f'time({interval})'

    for tag in tags_list:
        group_by_clause += f', {tag}'

    return group_by_clause

def get_fields_query_string():
    global fields
    global ignore_schema_cache
    if fields is None or ignore_schema_cache:
        populate_fields()
    return generate_fields_string(fields)

def get_tags_query_string():
    global tags
    global interval
    if tags is None or ignore_schema_cache:
        populate_tags()
    return generate_group_by_string(tags, interval)

def create_downsampling_query(fields_clause, measurement, then, now, tags_clause):
    query = f"""
SELECT
    {fields_clause}
FROM
    {measurement}
WHERE
    time > '{then}'
AND
    time < '{now}'
GROUP BY
    {tags_clause}
    """
    return query

def get_down_sample_data(query):
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

    if now is None:
        now = datetime.utcnow()
    now = now.replace(second=0,microsecond=0)
    then = get_then(interval_val, interval_type, now)

    print(f"{then.strftime('%Y-%m-%dT%H:%M:%SZ')} to {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    start_time = time.time()
    fields_string = get_fields_query_string()
    tag_string = get_tags_query_string()
    query = create_downsampling_query(fields_string, source_measurement, then, now, tag_string)
    end_time = time.time()

    query_gen_time = end_time - start_time

    start_time = time.time()
    success, result = get_down_sample_data(query)
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

def setup_schema():
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
    setup_logging()
    setup_schema()

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