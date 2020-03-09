#!/bin/csh

setenv DATE  '20200302'
setenv START '202003021700'
setenv WINDOW  845     # number of minutes to look for files...(e.g., for 10 hours, 605 min)
setenv NEXRAD_DIR '/scratch/wicker/realtime/OBSGEN/NEXRAD'
setenv WOFS_RADAR_DIR '/scratch/wicker/realtime'

python get_aws_nexrad.py --newse $WOFS_RADAR_DIR/radars.$DATE.csh --start $START --window $WINDOW -d $NEXRAD_DIR
