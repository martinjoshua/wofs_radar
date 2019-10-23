#-----------------------------------------------------------------------
# This file contains the directories needed by WoFS radar processing.  
# Change things here to where you want them to come FROM or go TO...
#  
#
# In the rerun, slurm_mrms, or slurm_opaws scripts, these are imported 
# directly via
#
# from wofs_dirs import _MRMS_feed, _WSR88D_feed, ....
#
#  etc.
#-----------------------------------------------------------------------
#

import os

# Find current working directory

WOFS_DIR = os.getcwd()

#-----------------------------------------------------------------------
# WOFS grid info directory

_WOFS_grid_info  = "/scratch/wof/realtime/radar_files"

#-----------------------------------------------------------------------
# MRMS processing

_MRMS_feed       = "/scratch/LDM/MRMS"
_MRMS_obs_seq    = "/scratch/wicker/REALTIME/REF"
_prep_mrms       = WOFS_DIR+"/pyMRMS/prep_mrms.py"
_MRMS_log        = _MRMS_obs_seq+"/logs"

#-----------------------------------------------------------------------
# VR processing

_WSR88D_feed     = "/scratch/wicker/realtime/OBSGEN/NEXRAD"
_VR_obs_seq_dir  = "/scratch/wicker/REALTIME/VEL"
_opaws2D         = WOFS_DIR+"/pyOPAWS/opaws2d.py"
_opaws_logs      = _VR_obs_seq_dir+"/logs"

#-----------------------------------------------------------------------
# Command strings to run within rerun...

_slurm_mrms_string  = WOFS_DIR+"/slurm_mrms.job --start %s" 
_slurm_opaws_string = WOFS_DIR+"/slurm_opaws.job --start %s"
_slurm_concatenate  = WOFS_DIR+"/obs_seq_combine_ncdf.py -d %s -f %s"

#
#-----------------------------------------------------------------------
