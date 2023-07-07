import os
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta

def schedule():
    run_interval = int(os.getenv('RUN_INTERVAL', 60))
    
    scheduler = BlockingScheduler()
    scheduler.add_job(run, IntervalTrigger(minutes=run_interval), args=[run_interval])
    run(run_interval)
    scheduler.start()

def run(run_interval):
    now = datetime.now()
    prev = now - timedelta(minutes=run_interval)
    stop = now.strftime('%Y-%m-%d %H:%M:00')
    start = prev.strftime('%Y-%m-%d %H:%M:00')
    print(f"sample from {start} to {stop}")

if __name__ == "__main__":
    schedule()