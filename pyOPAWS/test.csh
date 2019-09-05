#!/bin/csh

/work/wicker/REALTIME/WOFS_radar/pyOPAWS/opaws2d.py -u phase -d /scratch/LDM/NEXRAD2/KJAX -o /scratch/wicker/REALTIME/VEL/20190904 -w --window 2019,09,04,18,30 --onlyVR -p 3 --dx 5000.000000  --roi 1000.000000
