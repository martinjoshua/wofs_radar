import subprocess
import datetime as DT
from Config import settings
from multiprocessing import Pool
from utils.radar import getFromFile
from pyOPAWS.run import main as opaws
from rass.run import main as rass

def runOPAWSForTime(run_time, totalRadars):
    date = run_time.strftime("%Y%m%d%H%M")
    if settings.default_slurm_enabled == True:
        cmd = "JOBID=$(sbatch --job-name=opaws_%s --parsable --array=0-%i --export=CYCLETIME=%s jobs/opaws.job) " % (date, totalRadars-1, date)
        cmd += "&& sbatch --job-name=opaws_combine_%s --export=COMBINETIME=%s --depend=afterany:$JOBID jobs/combine.job" % (date, run_time.strftime("%Y%m%d_%H%M"))
        print(cmd)
        OPAWSret = subprocess.Popen([cmd],shell=True)
        OPAWSret.wait()
    else:
        pool = Pool(processes=totalRadars)
        for i, _ in enumerate(getFromFile(run_time)):
            pool.apply_async(opaws, (run_time, i))
        pool.close()
        pool.join()   
    print("\n Slurm_opaws job submitted at %s" % DT.datetime.now().strftime("%H:%M:%S"))

def runRASSForTime(run_time, totalRadars):
    date = run_time.strftime("%Y%m%d%H%M")
    if settings.default_slurm_enabled == True:
        cmd = "JOBID=$(sbatch --job-name=rass_%s --parsable --array=0-%i --export=CYCLETIME=%s jobs/rass.job) " % (date, totalRadars-1, date)
        cmd += "&& sbatch --job-name=rass_combine_%s --export=COMBINETIME=%s --depend=afterany:$JOBID jobs/combine.job" % (date, run_time.strftime("%Y%m%d_%H%M"))
        print(cmd)
        RASSret = subprocess.Popen([cmd],shell=True)
        RASSret.wait()
    else:
        pool = Pool(processes=totalRadars)
        for i, _ in enumerate(getFromFile(run_time)):
            pool.apply_async(rass, (run_time, i))
        pool.close()
        pool.join()   
    print("\n Slurm_rass job submitted at %s" % DT.datetime.now().strftime("%H:%M:%S"))    

def runMRMSForTime(run_time):
    date = run_time.strftime("%Y%m%d%H%M")
    if settings.default_slurm_enabled == True:
        cmd = "sbatch --job-name=mrms_%s --export=CYCLETIME=%s jobs/mrms.job" % (date, date)
    else:
        cmd = "python -m pyMRMS.run --cycle %s" % date
    print(cmd)
    OPAWSret = subprocess.Popen([cmd],shell=True)
    OPAWSret.wait()
    print("\n Slurm_mrms job submitted at %s" % DT.datetime.now().strftime("%H:%M:%S"))