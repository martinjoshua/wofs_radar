#!/usr/bin/env python
#############################################################
#
#  script to download large numbers of NEXRAD LVL2 files 
#  from Amazon S3 and process them to create VR superob files 
#  in HDF5 and DART obs_seq files
#
#  Python requirements:  boto3 install
#
#############################################################
#
# Written by Lou Wicker Feb, 2017
#
# Thanks to Tony Reinhart for getting me the code for S3
#
#############################################################

import datetime as DT
import sys, os
import boto3
from optparse import OptionParser
import datetime as DT
from multiprocessing import Pool
import time as cpu
import glob

#=======================================================================================================================
# Definitions

_unfold_method = "region"   # "region", "phase", "None" --> region works best, but if it fails, try phase

_nthreads = 2

_chk_dir_size = 300

_region        = 'us-east-1'

_window = 10

_wget_string   = "wget -c https://noaa-nexrad-level2.s3.amazonaws.com/"

_get_aws_string= "/work/wicker/REALTIME/WOFS_radar/get_aws_nexrad.py --start %s --window %s -r %s >& %s "

debug = True

#=======================================================================================================================
# RunProcess is a function that runs a system command for parallel processing

def RunProcess(cmd):

    print("\n Executing command:  %s " % cmd)

    os.system(cmd)

    print("\n %s completed...." % cmd)

    return

#=======================================================================================================================
def get_folder_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

#=======================================================================================================================
# Parse and run NEWS csh radar file

def parse_NEWSe_radar_file(radar_file_csh, start, window, outdir):

# Parse radars out of the shell script - NOTE - line that contains radar list is line=6 HARDCODED

    fhandle    = open(radar_file_csh)
    all_lines  = fhandle.readlines()
    radar_list = all_lines[6].split("(")[1].split(")")[0].split()
    fhandle.close()

# Now create the calls to prep_nexrad.py
    
    for radar in radar_list:
        print(" \n Now processing %s \n " % radar)
        log_file = os.path.join(outdir, "%s.%s.log" % (radar, start.strftime("%Y%m%d")))
        cmd = _get_aws_string % ( start.strftime("%Y%m%d%H%M"), window, radar, log_file  )
        
        if debug:
            print(cmd)

        os.system(cmd)

#   if not no_down:
#       old_size = 0
#       new_size = get_folder_size(start_path = '.')
#       cpu.sleep(_chk_dir_size)
#       while old_size < new_size:
#           old_size = new_size
#           new_size = get_folder_size(start_path = '.')
#           cpu.sleep(_chk_dir_size)

    return
#=======================================================================================================================
# getS3filelist

def getS3FileList(radar, datetime, window):

    noaas3 = boto3.client('s3', region_name = _region)

    files = []

    prefix = "%s/%s/" % (datetime.strftime("%Y/%m/%d"), radar)

    if debug:
        print(" \n getS3FileList string: %s \n" % prefix)

    file_list = noaas3.list_objects_v2(Bucket='noaa-nexrad-level2', Delimiter='/', Prefix=prefix)

    for i in file_list['Contents'][:]:

        file_time = DT.datetime.strptime(os.path.basename(i['Key'])[4:17], "%Y%m%d_%H%M")

        if file_time >= datetime and file_time < datetime + window:
            files.append(i['Key'])
            print("Found file:  %s for time %s" % (file_time.strftime("%Y%m%d_%H%M"), datetime.strftime("%Y%m%d_%H%M")))
        else:
            continue

    #here you can then feed the list to boto3.s3.copy_object or use wget proce

    return files

#-------------------------------------------------------------------------------
# Main program for testing...
#
if __name__ == "__main__":

#
# Command line interface 
#
    parser = OptionParser()

    parser.add_option(      "--newse", dest="newse",    type="string", default=None, \
                                      help = "NEWSe radars description file to parse and run prep_nexrad on" )

    parser.add_option("-r", "--radar",    dest="radar",    type="string", default=None, \
                                      help = "What radar to download")

    parser.add_option(      "--start",    dest="start",    type="string", default=None,  \
                                     help = "Start time of search in YYYY,MM,DD,HH")

    parser.add_option(      "--window",   dest="window",   type="int", default=None,  \
                                     help = "Window time in minutes to look for files")

    parser.add_option("-d", "--dir",      dest="dir",      type="string", default=None,  \
                                     help = "directory for radar files")

    parser.add_option(      "--nthreads", dest="nthreads", type="int",    default=_nthreads, \
                                     help = "Number of download threads to run")

    (options, args) = parser.parse_args()

    if options.start == None:
        print "\n                NO START DATE AND HOUR SUPPLIED, EXITING.... \n "
        parser.print_help()
        print
        sys.exit(1)
    else:
        start   = DT.datetime.strptime(options.start, "%Y%m%d%H%M") 

    if options.window == None:
        print("\n Using default window:  %d minutes" % _window)
        DT_window = DT.timedelta(minutes=_window)
        options.window = _window
    else:
        print("\n Using supplied window:  %d minutes" % options.window)
        DT_window = DT.timedelta(minutes=options.window)

    if options.radar == None and options.newse == None:
        print "\n                NO RADAR SUPPLIED, EXITING.... \n "
        parser.print_help()
        print
        sys.exit(1)
    else:
        radar = options.radar
    
    if options.dir == None:
        out_dir = options.radar
    else:
        out_dir = options.dir

    if options.newse:
       print(" \n now processing NEWSe radar file....\n ")
       parse_NEWSe_radar_file(options.newse, start, options.window, out_dir)
       sys.exit(0)
        
# Make sure we got somewhere to put the radar files
 
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

# If we are downloading from AWS, get file list from Amazon S3 and parse the files to get the correct hours  

    filelist = []

    ctime = start 

    if debug:
        print("\nGetting file list from AWS\n")

    newfiles = getS3FileList(radar, ctime, DT_window)

    for nf in newfiles:
        filelist.append(nf)

# Download each file and put it into a directory

    c0 = cpu.time()
    
    pool = Pool(processes=options.nthreads)              # set up a queue to run
 
    if debug:
        print("\nDownloading files from AWS\n")

    for file in filelist:
     
         cmd = "%s%s -P %s" % (_wget_string, file, out_dir)
         
         if debug:
             print(cmd)
         
         pool.apply_async(RunProcess, (cmd,))
 
    pool.close()
    pool.join()

    cpu0 = cpu.time() - c0
 
    print "\nDownload from Amazon took   %f  secs\n" % (round(cpu0, 3))


    cpu0 = cpu.time() - c0
    
    print "\Downloading %s files took   %f  secs\n" % (radar, round(cpu0, 3))
