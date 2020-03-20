import types, os
import datetime as DT
from pyOPAWS.opaws2d import run
from optparse import OptionParser
from Config import settings
from utils.radar import getFromFile

def main(start_time):
    radar_index = int(os.environ['SLURM_ARRAY_TASK_ID'])

    radars = getFromFile(DT.datetime.strptime(start_time, "%Y,%m,%d,%H,%M"))

    radar = radars[radar_index]

    print('Assigned radar for slurm task %s : %s' % (radar_index, radar))

    obj = types.SimpleNamespace()
    obj.dname = os.path.join(settings.opaws_feed, radar)
    obj.out_dir = settings.opaws_obs_seq
    obj.write = True
    obj.window = start_time
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

    parser.add_option("--window",    dest="window",    type="string", default=None,  \
                                    help = "Time of window location in YYYY,MM,DD,HH,MM")

    (options, args) = parser.parse_args()
    main(options.window)