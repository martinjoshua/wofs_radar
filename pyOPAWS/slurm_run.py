import types, os
import datetime as DT
from pyOPAWS.opaws2d import run
from optparse import OptionParser
from Config import settings
from utils.radar import getFromFile

def main(start_time, radarIndex):
    # radar_index = int(os.environ['SLURM_ARRAY_TASK_ID'])

    radars = getFromFile(start_time)

    radar = radars[radarIndex]

    print('Assigned radar for slurm task %s : %s' % (radarIndex, radar))

    obj = types.SimpleNamespace()
    obj.dname = os.path.join(settings.opaws_feed, radar)
    obj.out_dir = settings.opaws_obs_seq
    obj.write = True
    obj.window = start_time.strftime("%Y,%m,%d,%H,%M")
    obj.onlyVR = True
    obj.plot = int(settings.opaws_plot)
    obj.dx = float(settings.opaws_dx)
    obj.roi = float(settings.opaws_roi)
    obj.qc = 'Minimal'
    obj.unfold = 'region'
    obj.newse = None
    obj.method = None
    obj.shapefiles = None
    obj.interactive = None
    run(obj)

if __name__ == "__main__":
    parser = OptionParser()

    parser.add_option("--cycle",    dest="cycle",    type="string", default=None,  \
                                    help = "Cycle time YYYYMMDDHHMM")

    parser.add_option("--radarIndex",    dest="radarIndex",    type="int", default=None,  \
                                help = "Radar")                                   

    (options, args) = parser.parse_args()
    main(DT.datetime.strptime(options.cycle, "%Y%m%d%H%M"), options.radarIndex)