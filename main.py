import os
import re
import socket
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
from influxdb_client_3 import InfluxDBClient3, Point
from schedule_calculator import get_next_run_time, get_next_run_time_days, get_next_run_time_hours, get_next_run_time_minutes, get_then

logging_client = None
source_client = None
source_measurement = ""

interval = ""
def parse_interval(interval):
    match = re.fullmatch(r'(\d+)([mhd])', interval)
    if match is None:
        raise ValueError(f'Invalid format: {interval}')
    t = int(match.group(1))
    if t < 1:
        raise ValueError('Time period must be greater than 0')
    return t, match.group(2)


    
def run(interval_val, interval_type, now=None):
    if now is None:
        now = datetime.utcnow()
    now = now.replace(second=0,microsecond=0)
    then = get_then(interval_val, interval_type, now)

    print(f"{then.strftime('%Y-%m-%dT%H:%M:%SZ')} to {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    global logging_client
    try:
        log_run(now, then)
    except Exception as e:
        print(f"Logging failed due to {str(e)}")

def log_run(now, then):
    global interval
    if logging_client is not None:
        point = (Point("task_log")
         .field("start", then.strftime('%Y-%m-%dT%H:%M:%SZ'))
         .field("stop", now.strftime('%Y-%m-%dT%H:%M:%SZ'))
         .tag("interval", interval)
         .tag("task_host",socket.gethostname()))
        
        logging_client.write(point)

def setup_source_client():
    host = os.getenv('SOURCE_HOST')
    db = os.getenv('SOURCE_DB')
    token = os.getenv('SOURCE_TOKEN')
    org = os.getenv('SOURCE_ORG', 'none')
    global source_measurement
    source_measurement = os.getenv('SOOURCE_MEASUREMENT')

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

if __name__ == "__main__":
    # parse the user input
    interval = os.getenv('RUN_INTERVAL')
    interval_val, interval_type = parse_interval(interval)

    run_previous_opt = os.getenv('RUN_PREVIOUS_INTERVAL', 'false')
    run_previous = run_previous_opt.lower() in ['true', '1']

    setup_source_client()
    setup_logging()
    
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