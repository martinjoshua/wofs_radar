#!/usr/bin/env python

#-----------------------------------------------------------------------
#
# Documentation
#
# This script is used to take a list of radars for the WARN on FORECAST
# domain, and each time it is called, create N (N=no. of radars) netCDF
# files of data (superobbed) to cartesian grid.  The program runs
# a task-parallel set of jobs.  After the N radar files are created,
# the script then combines the files into one file for GSI to read.
#
#
# 06/24/2019-LJW:  Fixed code so that the radar file and directory
#                  are set to the run day by subtracting 12 hours
#                  from the GMT time supplied.
#                  This is done using "_hour_offset" variable
#-----------------------------------------------------------------------

from apscheduler.schedulers.blocking import BlockingScheduler
import time
import os
import sys
import datetime as DT
import logging
import subprocess

_wofs_VEL_dir       = "/scratch/wicker/REALTIME/VEL"
_wofs_radar_dir     = "/scratch/LDM/NEXRAD2"
_slurm_opaws_string = "/work/wicker/REALTIME/WOFS_radar/slurm_opaws.job --start %s"
_slurm_concatenate  = "/work/wicker/REALTIME/WOFS_radar/obs_seq_combine_ncdf.py -d %s -f %s > obs_seq_combine.log"

_TEST = False

# Used by combine to get the correct directory
_hour_offset = 12

if _TEST == True:
   rtimes = ', '.join(str(t) for t in range(60))    #test the code every minute
else:
   rtimes = "6,21,36,51"    # T+5min radar processing start time

#-------------------------------------------------------------------------------
# This is a handy function to make sure we get the correct day for radar file
#      I.E., we need to figure out what day it is, even if UTC is after 00Z

def utc_to_local(utc_dt):
    # get integer timestamp to avoid precision lost
    timestamp = calendar.timegm(utc_dt.timetuple())
    local_dt = DT.datetime.fromtimestamp(timestamp)
    assert utc_dt.resolution >= DT.timedelta(microseconds=1)
#   return local_dt.replace(microsecond=utc_dt.microsecond)
    return local_dt.replace(microsecond=utc_dt.microsecond) - DT.timedelta(hours=_hour_offset)


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
#   pyOPAWS and combines the individual radar files into a single netCDF file for GSI to read.

@wofs_sched.scheduled_job('cron', minute=rtimes)
def scheduled_job():

    gmt = time.gmtime()  # for file names, here we need to use GMT time

    cycle_time = get_time_for_cycle(DT.datetime(*gmt[:6]))

    cycle_time_str  = cycle_time.strftime("%Y%m%d%H%M")    
    cycle_time_str2 = cycle_time.strftime("%Y%m%d_%H%M")
    yyyy_mm_dd_directory = cycle_time - DT.timedelta(hours=_hour_offset)
    
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
            OPAWSret = subprocess.Popen([cmd],shell=True)
            OPAWSret.wait()
            print("\n Slurm_opaws job completed at %s" % (now))
        except:
            print("\n Slurm_opaws job failed: %s" % (now))
            
# combine all the files...
    directory = "%s/%s" % (_wofs_VEL_dir,yyyy_mm_dd_directory.strftime("%Y%m%d"))
    wildcard =  "_VR_{}.nc"
    cmd = (_slurm_concatenate % (directory, wildcard.format(cycle_time_str2)))

    if _TEST != True:
        try:
            COMBINEret = subprocess.Popen([cmd],shell=True)
            COMBINEret.wait()
            print("\n Slurm_concatenate job completed at %s" % (now))
        except:
            print("\n Slurm_concatenate job failed: %s" % (now))
    
    print("\n <<<<< END =======================================================================")

# Start the ball rolling here....
wofs_sched.start()
