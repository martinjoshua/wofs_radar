from apscheduler.schedulers.blocking import BlockingScheduler
import time
import os
import sys
import datetime as DT
import logging
import subprocess

_wofs_radar_dir      = "/work/wicker/REALTIME/WOFS_radar"
_WOFS_grid_info      = "/scratch/wof/realtime/radar_files/"
_NEXRAD_dir          = "/work/wicker/REALTIME/NEXRAD2"
_geget_nexrad_string  = "/work/wicker/REALTIME/WOFS_radar/Fetch_nexrad/get_aws_nexrad.py --newse %s --noanal --start %s --end %s -d %s" 

_TEST = True

if _TEST == True:
   rtimes = ', '.join(str(t) for t in range(60))    #test the code every minute
else:
   rtimes = "5,20,35,50"    # T+5min radar processing start time


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

wofs_get_nexrad_sched = BlockingScheduler()


@wofs_get_nexrad_sched.scheduled_job('cron', minute=rtimes)
def scheduled_job():

    gmt = time.gmtime()  # for file names, here we need to use GMT time

    cycle_time = get_time_for_cycle(DT.datetime(*gmt[:6])) - DT.timedelta(minutes=5)
    cycle_time2 = get_time_for_cycle(DT.datetime(*gmt[:6])) + DT.timedelta(minutes=5)

    cycle_time_str  = cycle_time.strftime("%Y%m%d%H%M")    
    cycle_time_str2 = cycle_time2.strftime("%Y%m%d%H%M")    
    
    local_time = time.localtime()  # get this so we know when script was submitted...
    now = "%s %2.2d %2.2d %2.2d%2.2d" % (local_time.tm_year, \
                                         local_time.tm_mon,  \
                                         local_time.tm_mday, \
                                         local_time.tm_hour, \
                                         local_time.tm_min)
    thedate = "%s%2.2d%2.2d" % (local_time.tm_year, local_time.tm_mon, local_time.tm_mday)

    print("\n >>>> BEGIN ======================================================================")
    print("\n Begin processing for cycle time:  %s" % (cycle_time_str))

    # make radar file name
    radar_file_csh = os.path.join(_WOFS_grid_info, ("radars.%s.csh" % thedate))

    print("\n ============================================================================")
    print("\n CRON_WOFS_GETNEXRAD: Reading radar_file: %s" % radar_file_csh)
    print("\n ============================================================================")

    # MRMS processing
    cmd = (_get_nexrad_string % (radar_file_csh,cycle_time_str, cycle_time_str2, _NEXRAD_dir))
    print("\n Cmd: %s \n" % (cmd))
    
    if _TEST != True:
        try:
            GETret = subprocess.Popen([cmd],shell=True)
            GETret.wait()
            print("\n Slurm_mrms job finished at %s" % (now))
        except:
            print("\n Slurm_mrms job failed: %s" % (now))

    print("\n <<<<< END =======================================================================")

# Start the ball rolling here....
wofs_get_nexrad_sched.start()
