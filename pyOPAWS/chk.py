import netCDF4 as ncdf
import pylab as P
f = ncdf.Dataset('/scratch/wicker/realtime/VEL/20190904/obs_seq_KJAX_VR_20190904_1830.nc')
lat = f.variables['lat'][...]
lon = f.variables['lon'][...]
P.scatter(lon,lat)
P.show()
