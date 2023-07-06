import os
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta

def schedule():
    interval = int(os.getenv('RUN_INTERVAL'))
    scheduler = BlockingScheduler()
    scheduler.add_job(run, IntervalTrigger(minutes=interval), id='my_job', interval=interval)
    
    scheduler.start()

def run(interval=0):
    now = datetime.now()
    prev = now - timedelta(minutes=interval)
    stop = now.strftime('%Y-%m-%d %H:%M:00')
    start = prev.strftime('%Y-%m-%d %H:%M:00')
    print(f"sample from {start} to {stop}")



if __name__ == "__main__":
    schedule()