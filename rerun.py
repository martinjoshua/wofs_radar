import types, os
import subprocess
import datetime as DT
from pyOPAWS.opaws2d import run
from optparse import OptionParser
from Config import settings
from utils.radar import getFromFile
from slurm.jobs import runMRMSForTime, runOPAWSForTime

def main(start_time, end_time):

    total_minutes = (end_time - start_time) / DT.timedelta(minutes=1)

    dtime = 15 # minutes

    runtimes = [ start_time + DT.timedelta(minutes=i*dtime) for i in range(1 + int(total_minutes/dtime)) ]

    print(runtimes)

    radars = getFromFile(start_time)
    
    for cycle_time in runtimes:
        runMRMSForTime(cycle_time)
        runOPAWSForTime(cycle_time, len(radars))

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-s", "--start", dest="start", default=None,  type="string", help = "YYYYMMDDHHMM start time")
    parser.add_option("-e", "--end",   dest="end",   default=None,  type="string", help = "YYYYMMDDHHMM end time")

    (options, args) = parser.parse_args()
    main(DT.datetime.strptime(options.start, "%Y%m%d%H%M"), DT.datetime.strptime(options.end, "%Y%m%d%H%M"))