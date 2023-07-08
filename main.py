import os
import re
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta


def parse_interval(interval):
    match = re.fullmatch(r'(\d+)([mhd])', interval)
    if match is None:
        raise ValueError(f'Invalid format: {interval}')
    t = int(match.group(1))
    if t < 1:
        raise ValueError('Time period must be greater than 0')
    return t, match.group(2)

def get_next_run_time(interval_val, interval_type):
    if interval_type == "m":
        return get_next_run_time_minutes(interval_val)
    elif interval_type == "h":
        return get_next_run_time_hours(interval_val)
    elif interval_type == "d":
        return get_next_run_time_days(interval_val)

def get_next_run_time_minutes(minutes, now=None):
    if now == None:
        now = datetime.now()

    if minutes < 60:
        next_minute = ((now.minute // minutes) + 1) * minutes
        if next_minute >= 60:
            return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            return now.replace(minute=next_minute, second=0, microsecond=0)
    else:
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

def get_next_run_time_hours(hours, now=None):
    if now == None:
        now = datetime.now()
    return now.replace(hour=now.hour + 1, minute=0, second=0, microsecond=0)

def get_next_run_time_days(days, now=None):
    if now == None:
        now = datetime.now()
    return now.replace(day = now.day + 1, hour=0, minute=0, second=0, microsecond=0)
   

def run():
    now = datetime.now()
    print(f"{now}:{now.replace(second=0,microsecond=0)}")
    
if __name__ == "__main__":
    # parse the user input
    interval = os.getenv('RUN_INTERVAL')
    interval_val, interval_type = parse_interval(interval)
    next_run_time = get_next_run_time(interval_val, interval_type)

    # establish the baseline for timing
    start_date = datetime.now()
    interval_settings = {"days":0, "hours":0,"minutes":0}

    # set the start date based on the interval type
    # set the values for the intervals
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
                        start_date=start_date)
    scheduler.start()