import os
from Config import settings

def getFromFile(start_time):
    radar_file_csh = os.path.join(settings.default_grid_info, ("radars.%s.csh" % start_time.strftime("%Y%m%d")))
    fhandle    = open(radar_file_csh)
    all_lines  = fhandle.readlines()
    radar_list = all_lines[6].split("(")[1].split(")")[0].split()
    fhandle.close()
    return radar_list

def getLatLonFromFile(start_time):
    radar_file_csh = os.path.join(settings.default_grid_info, ("radars.%s.csh" % start_time.strftime("%Y%m%d")))
    fhandle    = open(radar_file_csh)
    all_lines  = fhandle.readlines()
    lat = float(all_lines[7].split(" ")[2])
    lon = float(all_lines[8].split(" ")[2])
    fhandle.close()
    return (lat, lon)

