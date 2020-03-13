import unittest
import types
import time
import logging
import os, sys
import datetime

from pyOPAWS.opaws2d import run

class TestOPAWSMultiple(unittest.TestCase):

    def test_multiple(self):
        start_time = datetime.datetime(2020, 3, 2, 17, 0, 0)
        stop_time  = start_time + datetime.timedelta(minutes = 845)
        dtime      = datetime.timedelta(minutes=15)

        def getRadars():
            radar_file_csh = os.path.join("./RADAR", ("radars.%s.csh" % start_time.strftime("%Y%m%d")))
            # Parse center lat and lon out of the c-shell radar file - HARDCODED!
            # If the file does not exist, then we exit out of this run
            fhandle = open(radar_file_csh)
            # Read radars out of radars.YYYYMMDD file
            fhandle    = open(radar_file_csh)
            all_lines  = fhandle.readlines()
            radar_list = all_lines[6].split("(")[1].split(")")[0].split()
            fhandle.close()
            return radar_list
        
        obj = types.SimpleNamespace()
        obj.out_dir = './out/20200302'
        obj.write = True
        obj.onlyVR = True
        obj.plot = 3
        obj.dx = 5000.000000
        obj.roi = 1000.000000
        obj.qc = 'Minimal'
        obj.unfold = 'region'
        obj.newse = None
        obj.method = None
        obj.shapefiles = None
        obj.interactive = None
        
        while start_time < stop_time:
            for radar in getRadars():
                obj.dname = os.path.join('./RADAR', radar)
                obj.window = start_time.strftime("%Y,%m,%d,%H,%M")
                print("\n opaws2D called at %s" % (time.strftime("%Y-%m-%d %H:%M:%S")))
                run(obj)
            start_time = start_time + dtime

if __name__ == '__main__':
    unittest.main()