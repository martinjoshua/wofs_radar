#!/usr/bin/env python

from apscheduler.schedulers.blocking import BlockingScheduler
import time
import os
import sys
import datetime as DT
import logging
import subprocess
from optparse import OptionParser

_wofs_VEL_dir       = "/work/wicker/REALTIME/VEL"
_wofs_radar_dir     = "/work/wicker/REALTIME/WOFS_radar"
_slurm_mrms_string  = "/work/wicker/REALTIME/WOFS_radar/slurm_mrms.job --start %s"
_slurm_opaws_string = "/work/wicker/REALTIME/WOFS_radar/slurm_opaws.job --start %s"
_slurm_concatenate  = "/work/wicker/REALTIME/WOFS_radar/obs_seq_combine_ncdf.py -d %s -f %s"

_TEST = False

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

#
# Scheduler block of code - this submits the slurm radar processing scripts from
#   pyOPAWS and pyMRMS directories.  

def main():

   parser = OptionParser()
   parser.add_option("-t", "--time",     dest="time",        default=None,  type="string", help = "format:  YYYMMDDHHMM")
   parser.add_option(      "--no_mrms",  dest="no_mrms",     default=False,  help = "Boolean to not run MRMS processing", \
                                                                                  action="store_true")
   parser.add_option(      "--no_opaws", dest="no_opaws",    default=False,  help = "Boolean to not run OPAWS processing", \
                                                                                  action="store_true")
   parser.add_option(      "--no_combine", dest="no_combine", default=False,  help = "Boolean to not combine netCDF files", \
                                                                                  action="store_true")

   (options, args) = parser.parse_args()

   print ''
   print ' ================================================================================'

   if options.time == None:
         
       print "\n\n ***** USER MUST SPECIFY TIME   **"
       parser.print_help()
       print
       sys.exit(1)
   
   else:   
      gmt = DT.datetime.strptime(options.time, "%Y%m%d%H%M")
      print("\n ==============================================================================")
      print("\n Starting cron_wofs_rerun script for time: %s " % gmt.strftime("%Y-%m-%d %H:%M:%S"))
      print("\n ==============================================================================\n")


   cycle_time_str = gmt.strftime("%Y%m%d%H%M")  
   cycle_time_str2 = gmt.strftime("%Y%m%d_%H%M")
   
   local_time = time.localtime()  # get this so we know when script was submitted...
   now = "%s %2.2d %2.2d %2.2d%2.2d" % (local_time.tm_year, \
                                        local_time.tm_mon,  \
                                        local_time.tm_mday, \
                                        local_time.tm_hour, \
                                        local_time.tm_min)

   print("\n >>>> BEGIN ======================================================================")
   print("\n Begin processing for cycle time:  %s" % (cycle_time_str))

   # MRMS processing
   cmd = (_slurm_mrms_string % (cycle_time_str))
   if options.no_mrms == False:
       print("\n Cmd: %s \n" % (cmd))
   
   if _TEST != True:
       try:
           if options.no_mrms == False:
               MRMSret = subprocess.Popen([cmd],shell=True)
               MRMSret.wait()
               print("\n Slurm_mrms job finished at %s" % (now))
       except:
           print("\n Slurm_mrms job failed: %s" % (now))

   # OPAWS processing
   cmd = (_slurm_opaws_string % (cycle_time_str))
   if options.no_opaws == False:
       print("\n Cmd: %s \n" % (cmd))
   
   if _TEST != True:
       try:
           if options.no_opaws == False:
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
   if options.no_combine == False:
       print("\n Cmd: %s \n" % (cmd))    

   if _TEST != True:
       try:
           if options.no_combine == False:
               ret = subprocess.Popen([cmd],shell=True)
               ret.wait()
               print("\n Slurm_concatenate job completed at %s" % (now))
       except:
           print("\n Slurm_concatenate job failed: %s" % (now))
   
   print("\n <<<<< END =======================================================================")

#-------------------------------------------------------------------------------
# Main program for testing...
#
if __name__ == "__main__":
    sys.exit(main())
