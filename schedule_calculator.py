from datetime import datetime, timedelta

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