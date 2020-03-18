import time
import os
import sys
import datetime as DT
import logging
import subprocess
from optparse import OptionParser
from Config import settings

_VR_obs_seq_dir     = settings.opaws_obs_seq
_slurm_mrms_string  = "/work/wicker/REALTIME/WOFS_radar/slurm_mrms.job --start %s"
_slurm_opaws_string = "python slurm_opaws.job --start %s"
_slurm_concatenate  = "python -m slurm_combine_VR_ncdf -d %s -f %s > slurm_combine_VR.log"

_TEST = False

_hour_offset = 12

if _TEST == True:
   rtimes = ', '.join(str(t) for t in range(60))    #test the code every minute
else:
   rtimes = "5,20,35,50"    # T+5min radar processing start time
   
local_time = time.localtime()  # get this so we know when script was submitted...
now = "%s %2.2d %2.2d %2.2d%2.2d" % (local_time.tm_year, \
                                     local_time.tm_mon,  \
                                     local_time.tm_mday, \
                                     local_time.tm_hour, \
                                     local_time.tm_min)

#-----------------------------------------------------------------------------
# Utility to round a datetime object to nearest 15 min....

def get_time_for_cycle(the_time):
    minute = (the_time.minute//15)*15
    return the_time.replace(minute=0, second=0)+DT.timedelta(minutes=minute)

#
# Scheduler block of code - this submits the slurm radar processing scripts from
#   pyOPAWS and pyMRMS directories.  

def main(time=None, no_mrms=False, no_opaws=False, no_combine=False):

   gmt = DT.datetime.strptime(time, "%Y%m%d%H%M")
   print("\n ==============================================================================")
   print("\n Starting rerun_wofs_rradar script for time: %s " % gmt.strftime("%Y-%m-%d %H:%M:%S"))
   print("\n ==============================================================================\n")
   
   cycle_time_str = gmt.strftime("%Y%m%d%H%M")  
   cycle_time_str2 = gmt.strftime("%Y%m%d_%H%M")
   yyyy_mm_dd_directory = gmt - DT.timedelta(hours=_hour_offset)
   
   print("\n >>>> BEGIN ======================================================================")
   print("\n Begin processing for cycle time:  %s" % (cycle_time_str))

   # MRMS processing
   cmd = (_slurm_mrms_string % (cycle_time_str))
   if no_mrms == False:
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
   if no_opaws == False:
       print("\n Cmd: %s \n" % (cmd))
   
   if _TEST != True:
       try:
           if no_opaws == False:
               OPAWSret = subprocess.Popen([cmd],shell=True)
               OPAWSret.wait()
               print("\n Slurm_opaws job completed at %s" % (now))
       except:
           print("\n Slurm_opaws job failed: %s" % (now))
           
   # combine all the files...
   
   directory = "%s/%s" % (_VR_obs_seq_dir,yyyy_mm_dd_directory.strftime("%Y%m%d"))
   wildcard =  "_VR_{}.nc"
   cmd = (_slurm_concatenate % (directory, wildcard.format(cycle_time_str2)))
   if no_combine == False:
       print("\n Cmd: %s \n" % (cmd))    

   if _TEST != True:
       try:
           if no_combine == False:
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

   parser = OptionParser()
   parser.add_option("-s", "--start",   dest="start",        default=None,  type="string", help = "Begin time: format:  YYYMMDDHHMM")
   parser.add_option("-e", "--end",     dest="end",         default=None,  type="string", help = "End   time: format:  YYYMMDDHHMM")
   parser.add_option(      "--no_mrms",  dest="no_mrms",     default=False,  help = "Boolean to not run MRMS processing", \
                                                                                  action="store_true")
   parser.add_option(      "--no_opaws", dest="no_opaws",    default=False,  help = "Boolean to not run OPAWS processing", \
                                                                                  action="store_true")
   parser.add_option(      "--no_combine", dest="no_combine", default=False,  help = "Boolean to not combine netCDF files", \
                                                                                  action="store_true")

   (options, args) = parser.parse_args()

   print('')
   print(' ================================================================================')

   if options.start == None:         
       print("\n\n ***** USER MUST SPECIFY AT LEAST A SINGLE TIME   **")
       parser.print_help()
       print()
       sys.exit(1)

#-----------------------------------------------------------------------------
# Main program
# Need to keep two times - the cycle analysis time, and the actual wall time for log

   local_today = time.localtime()
   today = "%s%2.2d%2.2d" % (local_today.tm_year, local_today.tm_mon, local_today.tm_mday)

   print("\n ==============================================================================")
   print("\n Starting rerun_wofs_radar script at: %s " % time.strftime("%Y-%m-%d %H:%M:%S"))
   print("\n ==============================================================================\n")

   if options.end == None:
       sys.exit( main(time      = options.start,      \
                      no_combine= options.no_combine, \
                      no_mrms   = options.no_mrms,    \
                      no_opaws  = options.no_opaws) )
   else:  # loop through time

       start_time = DT.datetime.strptime(options.start, "%Y%m%d%H%M") 
       stop_time  = DT.datetime.strptime(options.end, "%Y%m%d%H%M") 
       dtime      = DT.timedelta(minutes=15)
       
       while start_time <= stop_time:

           print("\n Running time: %s " % start_time.strftime("%Y,%m,%d,%H,%M"))
           
           main(time      = start_time.strftime("%Y%m%d%H%M"),     
                no_combine= options.no_combine, 
                no_mrms   = options.no_mrms,   
                no_opaws  = options.no_opaws) 
                
           start_time = start_time + dtime
