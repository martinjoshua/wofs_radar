So this is the reorganized WoFS radar acquisition system.  

Requirements:

1.  python2 install with netCDF, numpy, maplotlib, basemap, and a few others things installed.
  * a later version of this will provide a conda environment for this.

2. Getting radar data from AWS.

In the directory fetchNexrad, there is a csh and python script.  The csh is a macro that
runs the get_aws_nexrad.py script.  The python script now can cross days, meaning that 
one does not have to do two downloads per day (one from 18-00Z, and one from 00Z-03Z, etc.)
The command looks like:

  python get_aws_nexrad.py --newse $WOFS_RADAR_DIR/radars.$DATE.csh --start $START --window $WINDOW -d $NEXRAD_DIR

One has to have AWS credentials available on the machine running the script (.e.g, is there an ".aws" in /home/...)
Edit the following variables

   setenv DATE  '20190503'
   setenv START '201905021700'

and where you want the data to go:

   setenv NEXRAD_DIR '/my_local_disk/my_local/dir/NEXRAD'

Run the script from the csh-file.


3. Radar processing:  The cron_wofs files are here, but have not been changed since spring 2019.  
   For post realtime, you dont use these.  The scripts that run are listed below:

   rerun_wofs_radar.py
   slurm_opaws.job
   slurm_mrms.job
   slurm_combine_VR_ncdf.py
   wofs_dirs.py

The primary file you need to edit is:  wofs_dirs.py.  This file has all the directory
structure for the WoFS radar processing, and you need to set up where things are coming from
and where things are going.  For the VR data, it assumes one has a NEXRAD L2 directory structure of

   /some_disk/some_NEXRAD/KLTX
   /some_disk/some_NEXRAD/KFDR
   /some_disk/some_NEXRAD/KINX

etc., meaning its somewhere and then each radar is located in its own directory.  
To use a local directory, you would edit the line in wofs_dirs.py:

    _WSR88D_feed     = "/my_local_disk/my_local_dir/NEXRAD"

The other place to edit is where you want the VR to goto:

    _VR_obs_seq_dir  = "/my_local_disk/my_local_dir/REALTIME/VEL"

For MRMS data, you would again figure out where the LDM feed is from and set:

    _MRMS_feed       = "/scratch/LDM/MRMS"

as well as where you want the obs_seq_*.nc files to land:o

    _MRMS_obs_seq    = "/my_local_disk/my_local_dir/REALTIME/REF"

log files are placed in a "log" sub-directory with each data type

To run VR-ONLY data processing, simply setup of the wofs_dirs.py file, and then get ready to run

    rerun_wofs_rerun_wofs_radar.py --start YYYMMDDHHMM --end YYYMMDDHHMM --no_mrms

this will go and grab the wofs_radar file, look for the radars listed in that file, then attempt 
to grab from "_WSR88D_feed" directory the 20-30 radars for that day and process files every 15 mins
start to end.  In the " _VR_obs_seq_dir" directory, there will be a DART, netCDF, and a plot file for each radar
at each time, and then a combined netCDF file with no radar name for that time - this is the file ingested.

4. MRMS data will be documented later
