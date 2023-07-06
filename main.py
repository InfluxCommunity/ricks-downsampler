import os
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

def schedule():
    scheduler = BlockingScheduler()
    scheduler.add_job(run, IntervalTrigger(minutes=1), id='my_job')
    scheduler.start()

def run():
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:00')
    print(timestamp)



if __name__ == "__main__":
    schedule()