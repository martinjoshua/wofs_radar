import time
import os
import sys
from optparse import OptionParser

_wofs_radar_dir     = "/work/wicker/REALTIME/"

parser = OptionParser()
parser.add_option("-d", "--day", dest="day", default=None,  type="string", help = "YYYYMMDD to check on")
                                                                                
(options, args) = parser.parse_args()

if options.day == None:
    time_str  = time.strftime("%Y%m%d")
else:
    time_str   = options.day

print("\n ==============================================================================")
print("\n Checking status of WOFS radar processing for: %s-%s-%s " % (time_str[0:4], time_str[4:6], time_str[6:8]))
print("\n ==============================================================================\n")

dbz_dir = os.path.join(_wofs_radar_dir, "REF", time_str)
print("\n Checking status of WOFS MRMS directory\n")
os.system("ls -las %s" % dbz_dir)
print("\n")
input("Press Enter to continue...")
print("\n Checking status of WOFS velocity directory\n")
vel_dir = os.path.join(_wofs_radar_dir, "VEL", time_str)
os.system("ls -las %s" % vel_dir)
