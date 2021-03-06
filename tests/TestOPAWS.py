import unittest
import types
from pyOPAWS.opaws2d import run
from Config import settings

class TestOPAWS(unittest.TestCase):
    def test_kdmx(self):
        obj = types.SimpleNamespace()
        obj.dname = settings.opaws_feed + '/KDMX'
        obj.out_dir = settings.opaws_obs_seq + '/20200302'
        obj.write = True
        obj.window = '2020,03,03,06,23'
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
        run(obj)

if __name__ == '__main__':
    unittest.main()