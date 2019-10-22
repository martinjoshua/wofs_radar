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
# from slurm_mrms.job
_MRMS_feed       = "/scratch/LDM/MRMS"
_MRMS_obs_seq    = "/scratch/wicker/REALTIME/REF"
_MRMS_log        = "/scratch/wicker/REALTIME/REF/logs"
_WOFS_grid_info  = "/scratch/wof/realtime/radar_files"
_prep_mrms       = "/work/wicker/REALTIME/WOFS_radar/pyMRMS/prep_mrms.py"

#-----------------------------------------------------------------------
# from slurm_opaws.job
_VR_obs_seq_dir  = "/scratch/wicker/REALTIME/VEL"
_WSR88D_feed     = "/scratch/wicker/realtime/OBSGEN/NEXRAD"
_WOFS_grid_info  = "/scratch/wof/realtime/radar_files"
_opaws2D         = "~/REALTIME/WOFS_radar/pyOPAWS/opaws2d.py"
_opaws_logs      = "/scratch/wicker/REALTIME/VEL/logs"

#-----------------------------------------------------------------------
# from rerun_wofs
_slurm_mrms_string  = "~/REALTIME/WOFS_radar/slurm_mrms.job --start %s"
_slurm_opaws_string = "~/REALTIME/WOFS_radar/slurm_opaws.job --start %s"
_slurm_concatenate  = "~/REALTIME/WOFS_radar/obs_seq_combine_ncdf.py -d %s -f %s"
