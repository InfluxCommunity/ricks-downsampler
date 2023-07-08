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

def get_next_run_time(interval_val, interval_type, now=None, run_previous=False):
    if interval_type == "m":
        t =  get_next_run_time_minutes(interval_val, now)
        if run_previous:
            t = t.replace(minute=t.minute-interval_val)
        return t
    elif interval_type == "h":
        t = get_next_run_time_hours(interval_val, now)
        if run_previous:
            t = t.replace(hour=t.hour-interval_val )
        return t
    elif interval_type == "d":
        t = get_next_run_time_days(interval_val, now)
        if run_previous:
            t = t.replace(day=t.date-interval_val)
        return t

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
   

def get_then(interval_val, interval_type, now):
    if interval_type == "m":
        return now - timedelta(minutes=interval_val)
    elif interval_type == "h":
        return now - timedelta(hours=interval_val)
    elif interval_type == "d":
        return now - timedelta(days=interval_val)
    
def run(interval_val, interval_type, now=None):
    if now is None:
        now = datetime.utcnow()
    now = now.replace(second=0,microsecond=0)
    then = get_then(interval_val, interval_type, now)

    print(f"{then.strftime('%Y-%m-%dT%H:%M:%SZ')} to {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")

if __name__ == "__main__":
    # parse the user input
    interval = os.getenv('RUN_INTERVAL')
    interval_val, interval_type = parse_interval(interval)

    run_previous_opt = os.getenv('RUN_PREVIOUS_INTERVAL', 'false')
    run_previous = run_previous_opt.lower() in ['true', '1']

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