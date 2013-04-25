import time, datetime

def GetTheTime(start):
    elapsed = (time.time() - start)
    add_time = elapsed
    add_time = round(add_time, 2)
    
    if elapsed < 60:
        elapsed = round(elapsed, 2)
        elapsed = str(elapsed) + " seconds" 
    elif elapsed > 60 and elapsed < 3600:
        elapsed = elapsed / 60
        elapsed = round(elapsed, 2)
        elapsed = str(elapsed) + " minutes"
    else:
        elapsed = elapsed / 3600
        elapsed = round(elapsed, 2)
        elapsed = str(elapsed) + " hours"

    nlist = []
    nlist = [elapsed,add_time]
    return nlist