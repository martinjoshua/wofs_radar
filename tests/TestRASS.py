import unittest
import types, calendar, os, datetime
from rass.mrms import run
from Config import settings
from multiprocessing import Pool

class TestRASS(unittest.TestCase):
    def test_kohx(self):
        obj = types.SimpleNamespace()
        obj.out_dir = settings.rass_output + '/20200302'
        obj.write = True
        obj.onlyVR = True
        obj.plot = 0
        obj.dx = 2000.000000
        obj.roi = 1000.000000
        obj.newse = None
        obj.method = None
        obj.shapefiles = None
        obj.interactive = None
        run('KOHX', datetime.datetime(2020, 3, 3, 6, 1, 0), obj)

        run('KOHX', datetime.datetime(2020, 3, 3, 6, 13, 0), obj)

        run('KOHX', datetime.datetime(2020, 3, 3, 6, 15, 0), obj)

if __name__ == '__main__':
    unittest.main()