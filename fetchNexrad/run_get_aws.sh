#!/bin/bash

export DATE='20200302'
export START='202003021700'
export WINDOW=845     # number of minutes to look for files...(e.g., for 10 hours, 605 min)
export NEXRAD_DIR='/mnt/c/Joshua/Linux/wofs_radar/RADAR'
export WOFS_RADAR_DIR='../RADAR'

python get_aws_nexrad.py --newse ./radars.$DATE.csh --start $START --window $WINDOW -d $NEXRAD_DIR