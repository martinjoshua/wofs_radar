import types, os
import datetime as DT
from optparse import OptionParser
from Config import settings
from utils.radar import getFromFile
from rass.mrms import run

def main(start_time, radarIndex):
    radars = getFromFile(start_time)

    radar = radars[radarIndex]

    print('Assigned radar for slurm task %s : %s' % (radarIndex, radar))

    obj = types.SimpleNamespace()
    obj.dname = os.path.join(settings.rass_input, start_time.strftime("%Y%m%d"), radar)
    obj.out_dir = settings.rass_output
    obj.write = settings.rass_write
    obj.window = start_time.strftime("%Y,%m,%d,%H,%M")
    obj.onlyVR = settings.rass_onlyvr
    obj.plot = int(settings.rass_plot)
    obj.dx = float(settings.rass_dx)
    obj.roi = float(settings.rass_roi)
    obj.newse = None
    obj.method = None
    obj.shapefiles = None
    obj.interactive = None
    run(radar, start_time, obj)

if __name__ == "__main__":
    parser = OptionParser()

    parser.add_option("--cycle",    dest="cycle",    type="string", default=None,  \
                                    help = "Cycle time YYYYMMDDHHMM")

    parser.add_option("--radarIndex",    dest="radarIndex",    type="int", default=None,  \
                                help = "Radar")                                   

    (options, args) = parser.parse_args()
    main(DT.datetime.strptime(options.cycle, "%Y%m%d%H%M"), options.radarIndex)