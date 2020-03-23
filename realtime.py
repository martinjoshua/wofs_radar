from apscheduler.schedulers.blocking import BlockingScheduler
import time
import os
import sys
import datetime as DT
import calendar
import logging
import subprocess
from Config import settings
from utils.radar import getFromFile
from slurm.jobs import runOPAWSForTime, runMRMSForTime

_hour_offset = 12
_TEST = bool(settings.default_debug)
radars = getFromFile(DT.datetime.utcnow())

if _TEST == True:
   rtimes = ', '.join(str(t) for t in range(60))    #test the code every minute
else:
   rtimes = settings.default_runtimes    # T+5min radar processing start time

logging.basicConfig()

wofs_sched = BlockingScheduler()

def get_time_for_cycle(the_time):
    minute = (the_time.minute//15)*15
    return the_time.replace(minute=0, second=0)+DT.timedelta(minutes=minute)

@wofs_sched.scheduled_job('cron', minute=rtimes)
def opaws():
    gmt = time.gmtime()  # for file names, here we need to use GMT time
    cycle_time = get_time_for_cycle(DT.datetime(*gmt[:6]))

    print("\n >>>> BEGIN OPAWS ======================================================================")
    print("\n Begin processing for cycle time:  %s" % (cycle_time.strftime("%Y%m%d_%H%M")))

    runOPAWSForTime(cycle_time, len(radars))
    
    print("\n <<<<< END =======================================================================")

@wofs_sched.scheduled_job('cron', minute=rtimes)
def mrms():
    gmt = time.gmtime()  # for file names, here we need to use GMT time
    cycle_time = get_time_for_cycle(DT.datetime(*gmt[:6]))

    print("\n >>>> BEGIN MRMS ======================================================================")
    print("\n Begin processing for cycle time:  %s" % (cycle_time.strftime("%Y%m%d_%H%M")))

    runMRMSForTime(cycle_time)
    
    print("\n <<<<< END =======================================================================")

# Start the ball rolling here....
wofs_sched.start()
