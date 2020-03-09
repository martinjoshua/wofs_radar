#!/bin/csh

/work/wicker/REALTIME/WOFS_radar/pyOPAWS/opaws2d.py -u phase -d /work/wicker/realtime/pyROTH/KMPX -o ./ -w --window 2019,07,19,23,15 --onlyVR -p 3 --dx 5000.000000  --roi 1000.000000
