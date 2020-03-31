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

from optparse import OptionParser
from crontab import CronTab

_TEST = bool(settings.default_debug)

if _TEST == True:
   rtimes = ','.join(str(t) for t in range(60))    #test the code every minute
else:
   rtimes = settings.default_runtimes    # T+5min radar processing start time

logging.basicConfig()

def get_time_for_cycle(the_time):
    minute = (the_time.minute//15)*15
    return the_time.replace(minute=0, second=0)+DT.timedelta(minutes=minute)

def setCronJob(doEnable):
    comment='WoFS VR Realtime'
    cron = CronTab(user=True)
    jobs = list(cron.find_comment(comment))
    if len(jobs) > 1:
        raise Exception('Multiple cron jobs were returned with the comment "{}"; there must only be one job with the aforementioned comment per user crontab.')
    if jobs == None or len(jobs) == 0: 
        job = cron.new(command = "{0}/jobs/realtime.sh {0} >> {0}/wofs-radar.log 2>&1".format(os.getcwd()), comment = comment)
    else: job = jobs[0]
    mins = list(map(int, rtimes.split(',')))
    job.minute.on(*mins)
    if doEnable == False:
        cron.remove(job)
    cron.write(user=True)

def main(options):
    if options.start == True:
        setCronJob(True)
    
    if options.stop == True:
        setCronJob(False)

    if options.now == True:
        gmt = time.gmtime() 
        cycle_time = get_time_for_cycle(DT.datetime(*gmt[:6]))
        radars = getFromFile(DT.datetime.utcnow())
        runMRMSForTime(cycle_time)
        runOPAWSForTime(cycle_time, len(radars))

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-n", "--now", dest="now", default=False,  action = 'store_true')
    parser.add_option("-s", "--start", dest="start", default=False,  action = 'store_true')
    parser.add_option("-e", "--stop",   dest="stop",   default=False,  action = 'store_true')

    (options, args) = parser.parse_args()

    main(options)
