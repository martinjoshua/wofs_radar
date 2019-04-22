#!/usr/bin/env python

from apscheduler.schedulers.blocking import BlockingScheduler
import time
import os
import sys
import datetime as DT
import logging
import subprocess

_wofs_VEL_dir       = "/work/wicker/REALTIME/VEL"
_wofs_radar_dir     = "/work/wicker/REALTIME/WOFS_radar"
_slurm_opaws_string = "/work/wicker/REALTIME/WOFS_radar/slurm_opaws.job --start %s"
_slurm_concatenate  = "/work/wicker/REALTIME/WOFS_radar/obs_seq_combine_ncdf.py -d %s -f %s"

_TEST = False

if _TEST == True:
   rtimes = ', '.join(str(t) for t in range(60))    #test the code every minute
else:
   rtimes = "6,21,36,51"    # T+5min radar processing start time


#-----------------------------------------------------------------------------
# Utility to round a datetime object to nearest 15 min....

def get_time_for_cycle(the_time):
    minute = (the_time.minute//15)*15
    return the_time.replace(minute=0, second=0)+DT.timedelta(minutes=minute)

#-----------------------------------------------------------------------------
# Main program
# Need to keep two times - the cycle analysis time, and the actual wall time for log

local_today = time.localtime()
today = "%s%2.2d%2.2d" % (local_today.tm_year, local_today.tm_mon, local_today.tm_mday)

print("\n ==============================================================================")
print("\n Starting cron_wofs_radar script at: %s " % time.strftime("%Y-%m-%d %H:%M:%S"))
print("\n ==============================================================================\n")

# Need to start a log file...

logging.basicConfig()

# the insubstantiates a scheduler

wofs_sched = BlockingScheduler()

# Scheduler block of code - this submits the slurm radar processing scripts from
#   pyOPAWS and pyMRMS directories.  

@wofs_sched.scheduled_job('cron', minute=rtimes)
def scheduled_job():

    gmt = time.gmtime()  # for file names, here we need to use GMT time

    cycle_time = get_time_for_cycle(DT.datetime(*gmt[:6]))

    cycle_time_str  = cycle_time.strftime("%Y%m%d%H%M")    
    cycle_time_str2 = cycle_time.strftime("%Y%m%d_%H%M")
    
    local_time = time.localtime()  # get this so we know when script was submitted...
    now = "%s %2.2d %2.2d %2.2d%2.2d" % (local_time.tm_year, \
                                         local_time.tm_mon,  \
                                         local_time.tm_mday, \
                                         local_time.tm_hour, \
                                         local_time.tm_min)

    print("\n >>>> BEGIN ======================================================================")
    print("\n Begin processing for cycle time:  %s" % (cycle_time_str))

    # OPAWS processing
    cmd = (_slurm_opaws_string % (cycle_time_str))
    print("\n Cmd: %s \n" % (cmd))
    
    if _TEST != True:
        try:
#             ret = os.system(cmd)
            OPAWSret = subprocess.Popen([cmd],shell=True)
            OPAWSret.wait()
            print("\n Slurm_opaws job completed at %s" % (now))
        except:
            print("\n Slurm_opaws job failed: %s" % (now))
            
    # combine all the files...
    directory = "%s/%s%2.2d%2.2d" % (_wofs_VEL_dir, local_time.tm_year, \
                                     local_time.tm_mon, local_time.tm_mday)
    wildcard =  "_VR_{}.nc"
    cmd = (_slurm_concatenate % (directory, wildcard.format(cycle_time_str2)))
    print("\n Cmd: %s \n" % (cmd))    

    if _TEST != True:
        try:
#             ret = os.system(cmd)
            ret = subprocess.Popen([cmd],shell=True)
            ret.wait()
            print("\n Slurm_concatenate job completed at %s" % (now))
        except:
            print("\n Slurm_concatenate job failed: %s" % (now))
    
    print("\n <<<<< END =======================================================================")

# Start the ball rolling here....
wofs_sched.start()
