import subprocess
import datetime as DT
from Config import settings

# TODO: check config for "use_slurm", if false, spin off python processes

def runOPAWSForTime(run_time, totalRadars):
    cmd = "JOBID=$(sbatch --parsable --array=0-%i --export=CYCLETIME=%s jobs/opaws.job) && sbatch --export=COMBINETIME=%s --depend=afterany:$JOBID jobs/combine.job" % (totalRadars-1, run_time.strftime("%Y%m%d%H%M"), run_time.strftime("%Y%m%d_%H%M"))
    print(cmd)
    OPAWSret = subprocess.Popen([cmd],shell=True)
    OPAWSret.wait()        
    print("\n Slurm_opaws job completed at %s" % DT.datetime.now().strftime("%H:%M:%S"))

def runMRMSForTime(run_time):
    cmd = "sbatch --export=CYCLETIME=%s jobs/mrms.job" % run_time.strftime("%Y%m%d%H%M")
    print(cmd)
    OPAWSret = subprocess.Popen([cmd],shell=True)
    OPAWSret.wait()        
    print("\n Slurm_mrms job completed at %s" % DT.datetime.now().strftime("%H:%M:%S"))