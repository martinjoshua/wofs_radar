import unittest
import types
import time
import logging
import copy
import os, sys
import datetime
from functools import reduce
from multiprocessing import Pool
from pyOPAWS.opaws2d import run

class TestOPAWSMultiple(unittest.TestCase):

    def test_multiple(self):
        start_time = datetime.datetime(2020, 3, 2, 17, 0, 0)
        total_minutes  = 845
        dtime      = 15 # minutes

        def getRadars():
            radar_file_csh = os.path.join("./RADAR", ("radars.%s.csh" % start_time.strftime("%Y%m%d")))
            # Parse center lat and lon out of the c-shell radar file - HARDCODED!
            # If the file does not exist, then we exit out of this run
            fhandle    = open(radar_file_csh)
            all_lines  = fhandle.readlines()
            radar_list = all_lines[6].split("(")[1].split(")")[0].split()
            fhandle.close()
            return radar_list
        
        def getOptions(dname, window):
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
            obj.dname = dname
            obj.window = window
            return obj
        
        radar_list = getRadars()
        radars_times = reduce(list.__add__, [ [(r, start_time + datetime.timedelta(minutes=i*dtime)) for r in radar_list] for i in range(int(total_minutes/dtime))])
        
        pool = Pool(5)
        pool.map(run, list(map(lambda r_t: getOptions(os.path.join('./RADAR', r_t[0]), r_t[1].strftime("%Y,%m,%d,%H,%M")), radars_times)))

if __name__ == '__main__':
    unittest.main()