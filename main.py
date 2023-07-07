import os
import re
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta


def parse_interval(interval):
    match = re.fullmatch(r'(\d+)([mhd])', interval)
    if match is None:
        raise ValueError(f'Invalid format: {interval}')
    return int(match.group(1)), match.group(2)

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
    
def schedule():
    pass
    
    
    # scheduler = BlockingScheduler()
    # scheduler.add_job(run, IntervalTrigger(minutes=run_interval), args=[run_interval])
    # run(run_interval)
    # scheduler.start()

def run(run_interval):
    pass
    # now = datetime.now()
    # prev = now - timedelta(minutes=run_interval)
    # stop = now.strftime('%Y-%m-%d %H:%M:00')
    # start = prev.strftime('%Y-%m-%d %H:%M:00')
    # print(f"sample from {start} to {stop}")

if __name__ == "__main__":
    interval = int(os.getenv('RUN_INTERVAL'))
    inter_val, interval_type = parse_interval(interval)
  