import types, os
from time import sleep
import subprocess
import datetime as DT
from pyOPAWS.opaws2d import run
from optparse import OptionParser
from Config import settings
from utils.radar import getFromFile
from jobs.run import runMRMSForTime, runOPAWSForTime, runRASSForTime

def main(start_time, end_time):

    total_minutes = (end_time - start_time) / DT.timedelta(minutes=1)

    dtime = 15 # minutes

    runtimes = [ start_time + DT.timedelta(minutes=i*dtime) for i in range(1 + int(total_minutes/dtime)) ]

    print(runtimes)

    radars = getFromFile(start_time)
    
    for cycle_time in runtimes:
        if settings.mrms_enabled == True: runMRMSForTime(cycle_time)
        if settings.opaws_enabled == True: runOPAWSForTime(cycle_time, len(radars))
        if settings.rass_enabled == True: runRASSForTime(cycle_time, len(radars))

        if settings.default_slurm_enabled == True:
            print('Pausing for slurm before submitting next job')
            sleep(180)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-s", "--start", dest="start", default=None,  type="string", help = "YYYYMMDDHHMM start time")
    parser.add_option("-e", "--end",   dest="end",   default=None,  type="string", help = "YYYYMMDDHHMM end time")

    (options, args) = parser.parse_args()
    main(DT.datetime.strptime(options.start, "%Y%m%d%H%M"), DT.datetime.strptime(options.end, "%Y%m%d%H%M"))