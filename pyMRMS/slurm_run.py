import types, os
import datetime as DT
from optparse import OptionParser
from Config import settings
from utils.radar import getLatLonFromFile
from pyMRMS.prep_mrms import run

def main(start_time):
    lat, lon = getLatLonFromFile(start_time)

    obj = types.SimpleNamespace()
    obj.dir = settings.mrms_feed
    obj.realtime = start_time.strftime("%Y%m%d%H%M")
    obj.grep = '*.netcdf.gz'
    obj.write = True
    obj.out_dir = settings.mrms_obs_seq
    obj.plot = int(settings.opaws_plot)
    obj.loc = [lat, lon]
    obj.thin = 1    
    run(obj)

if __name__ == "__main__":
    parser = OptionParser()

    parser.add_option("--cycle",    dest="cycle",    type="string", default=None,  \
                                    help = "Cycle time YYYYMMDDHHMM")                              

    (options, args) = parser.parse_args()
    main(DT.datetime.strptime(options.cycle, "%Y%m%d%H%M"))