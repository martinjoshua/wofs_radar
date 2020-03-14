import unittest
import types, calendar, os, datetime
from pyMRMS.prep_mrms import run
from multiprocessing import Pool

class TestMRMS(unittest.TestCase):

    def test_mrms_multiple(self):
        start_time = datetime.datetime(2020, 3, 2, 17, 0, 0)
        total_minutes  = 845
        dtime      = 15 # minutes
        _MRMS_obs_seq = '/scratch/joshua.martin/MRMS'
        _MRMS_feed = '/scratch/LDM/MRMS'

        def utc_to_local(utc_dt):
            _hour_offset = 12
            # get integer timestamp to avoid precision lost
            timestamp = calendar.timegm(utc_dt.timetuple())
            local_dt = datetime.datetime.fromtimestamp(timestamp)
            assert utc_dt.resolution >= datetime.timedelta(microseconds=1)
            return local_dt.replace(microsecond=utc_dt.microsecond) - datetime.timedelta(hours=_hour_offset)

        def getLatLon(radar_file_path, time):
            # create path to WOFS radar file
            # This method assumes that radar data will only be parsed up to 11:59 PM local time -
            #      so we convert the input UTC time to local time...
            radar_csh_file = os.path.join(radar_file_path, ("radars.%s.csh" % time.strftime("%Y%m%d")))

            # Parse center lat and lon out of the c-shell radar file - HARDCODED!
            # If the file does not exist, then we exit out of this run
            try:
                fhandle = open(radar_csh_file)
            except:
                print("\n ============================================================================")
                print("\n CANNOT OPEN radar CSH file, exiting MRMS processing:  %s" % radar_csh_file)
                print("\n ============================================================================")

            all_lines  = fhandle.readlines()
            lat = float(all_lines[7].split(" ")[2])
            lon = float(all_lines[8].split(" ")[2])
            fhandle.close()

            print("\n ============================================================================")
            print("\n Lat: %f  Lon: %f centers will be used for MRMS sub-domain" % (lat, lon))
            print("\n ============================================================================")

            return (lat, lon)

        lat, lon = getLatLon('./RADAR', start_time)
 
        def getOptions(runtime):
            obj = types.SimpleNamespace()
            obj.dir = os.path.join(_MRMS_feed, runtime.strftime("%Y/%m/%d"))
            obj.write = True
            obj.out_dir = os.path.join(_MRMS_obs_seq, start_time.strftime("%Y%m%d"))
            obj.realtime = runtime.strftime("%Y%m%d%H%M")
            obj.plot = 3
            obj.loc = [lat, lon]
            obj.grep = '*.netcdf.gz'
            obj.thin = 1
            return obj
 
        times = [start_time + datetime.timedelta(minutes=i*dtime) for i in range(int(total_minutes/dtime))]

        pool = Pool(5)
        pool.map(run, list(map(lambda t: getOptions(t), times)))


if __name__ == '__main__':
    unittest.main()