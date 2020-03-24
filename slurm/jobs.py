import subprocess
import datetime as DT
from Config import settings

# TODO: check config for "use_slurm", if false, spin off python processes

def runOPAWSForTime(run_time, totalRadars):
    date = run_time.strftime("%Y%m%d%H%M")
    cmd = "JOBID=$(sbatch --job-name=opaws_%s --parsable --array=0-%i --export=CYCLETIME=%s jobs/opaws.job) " % (date, totalRadars-1, date)
    cmd += "&& sbatch --job-name=opaws_combine_%s --export=COMBINETIME=%s --depend=afterany:$JOBID jobs/combine.job" % (date, run_time.strftime("%Y%m%d_%H%M"))
    print(cmd)
    OPAWSret = subprocess.Popen([cmd],shell=True)
    OPAWSret.wait()        
    print("\n Slurm_opaws job completed at %s" % DT.datetime.now().strftime("%H:%M:%S"))

def runMRMSForTime(run_time):
    date = run_time.strftime("%Y%m%d%H%M")
    cmd = "sbatch --job-name=mrms_%s --export=CYCLETIME=%s jobs/mrms.job" % (date, date)
    print(cmd)
    OPAWSret = subprocess.Popen([cmd],shell=True)
    OPAWSret.wait()        
    print("\n Slurm_mrms job completed at %s" % DT.datetime.now().strftime("%H:%M:%S"))