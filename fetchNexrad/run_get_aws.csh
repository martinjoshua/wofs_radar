#!/bin/csh

setenv DATE  '20190503'
setenv START '201905021700'
setenv WINDOW  605     # number of minutes to look for files...(this is 10 hours, 5 min)
setenv NEXRAD_DIR '/scratch/wicker/realtime/OBSGEN/NEXRAD'
setenv WOFS_RADAR_DIR '/scratch/wof/realtime/radar_files/'

python get_aws_nexrad.py --newse $WOFS_RADAR_DIR/radars.$DATE.csh --start $START --window $WINDOW -d $NEXRAD_DIR
