#!/bin/csh

/work/wicker/REALTIME/WOFS_radar/pyOPAWS/opaws2d.py -u region -d /work/wicker/realtime/pyROTH/KGRB -o ./ -w --window 2019,07,20,01,45 --onlyVR -p 1 --dx 5000.000000  --roi 1000.000000
