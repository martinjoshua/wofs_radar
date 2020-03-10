from configparser import ConfigParser

config = ConfigParser()
config.read('app.cfg')

def is_debug():
    return config.getboolean('DEFAULT', 'debug')

def get_default(setting):
    return config.get('DEFAULT', setting)

def get_mrms(setting):
    return config.get("MRMS", setting)

def get_opaws(setting):
    return config.get("OPAWS", setting)
    
def get_job(setting):
    return config.get("JOBS", setting)