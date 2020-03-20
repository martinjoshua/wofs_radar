import os
from Config import settings

def getFromFile(start_time):
    radar_file_csh = os.path.join(settings.default_grid_info, ("radars.%s.csh" % start_time.strftime("%Y%m%d")))
    fhandle    = open(radar_file_csh)
    all_lines  = fhandle.readlines()
    radar_list = all_lines[6].split("(")[1].split(")")[0].split()
    fhandle.close()
    return radar_list