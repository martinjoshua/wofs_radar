#############################################################
# opaws2D: A program to process LVL-2 volumes, unfold       #
#          radial velocities, and thin the radar data       #
#          using a Cressman analysis scheme.                #
#          Gridded data is then written out to a DART       #
#          format file for assimilation.                    #
#                                                           #
#       Python package requirements:                        #
#       ----------------------------                        #
#       numpy                                               #
#       scipy                                               #
#       matplotlib                                          #
#       pyart (ARM-DOE python radar toolkit)                #   
#       pyproj                                              # 
#       netCDF4                                             #
#       matplotlib                                          #
#       optparse                                            #
#                                                           #
#      Originally coded by Blake Allen, August 2016         #
#############################################################
#
#        Big modifications by Lou Wicker 2016-2017
#
#############################################################
from __future__ import print_function
from builtins import map
from builtins import zip
from builtins import range
from builtins import object

import os
import sys
import glob
import re
import time as timeit

# Need to set the backend BEFORE loading pyplot
import matplotlib as mpl
mpl.use('Agg')

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import scipy.interpolate
import scipy.ndimage as ndimage
import scipy.spatial
from optparse import OptionParser
from matplotlib.offsetbox import AnchoredText
from utils.dart_tools import opaws_write_DART_ascii
from pyOPAWS.radar_QC import *

import netCDF4 as ncdf
import datetime as DT
import xarray as xr
import pandas as pd
import metpy.calc as mpcalc
from metpy.units import units

from utils.cressman import *
import pyart

from pyproj import Proj
import pylab as plt  
from mpl_toolkits.basemap import Basemap
from pyart.graph import cm

# Ignore annoying warnings (unless the code breaks, then comment these lines out)
import warnings
#warnings.filterwarnings("ignore")

# re grep pattern to harden file data & time parsing
parse_radar_name2 = '(?:[A-Z]{4}_)'
parse_radar_name = '(?:[A-Z]{4}_)'

# AWS filename sparse
_AWS_L2Files = True
_AWS_L2Name_Style = "%s/*%s*"
#LDM filename parse
_LDM_L2Name_Style = "%s/*_%s"

# debug flag
debug = False

_thres_vr_from_ref     = True
_default_QC            = "Minimal"

# missing value
_missing = -99999.

# True here uses the basemap county database to plot the county outlines.
_plot_counties = True

# Colorscale information
_ref_scale = (0.,74.)
_vr_scale  = (-50.,50.)

# Need for Lambert conformal (default) coordinate projection
truelat1, truelat2 = 30.0, 60.0

# Parameter dict for Gridding
_grid_dict = {
              'grid_spacing_xy' : 3000.,         # meters
              'domain_radius_xy': 150000.,       # meters
              'anal_method'     : 'Cressman',    # options are Cressman, Barnes (1-pass)
              'ROI'             : 1000.,         # Cressman ~ analysis_grid * sqrt(2), Barnes ~ largest data spacing in radar
              'min_count'       : 3,             # regular radar data ~3, high-res radar data ~ 10
              'min_weight'      : 0.2,           # min weight for analysis Cressman ~ 0.3, Barnes ~ 2
              'min_range'       : 10000.,        # min distance away from the radar for valid analysis (meters)
              'projection'      : 'lcc',         # map projection to use for gridded data
              'mask_vr_with_dbz': True,
              '0dbz_obtype'     : True,
              'thin_zeros'      : 4,
              'halo_footprint'  : 3,
              'nthreads'        : 1,
              'max_height'      : 10000.,
              'MRMS_zeros'      : [True,      6000.], # True: creates a single level of zeros where composite DBZ < _dbz_min
              'model_grid_size' : [900000., 900000.]  # Used to create a common grid for all observations (special option) 
             }

# Parameter dict setting radar data parameters
               
_radar_parameters = {
                     'min_dbz_analysis': 25.0, 
                     'max_range': 150000.,
                     'max_Nyquist_factor': 3,                    # Filter(Vr): dont allow output of velocities > Nyquist*factor
                     'max_Radial_Velocity': 50.,                  # Filter(Vr): mask any Vr's greater, likely to be poor unfolding
                     'region_interval_splits': 5,                # default is 3, increase to improve mesoscale regions of unfolding.  
                     'field_label_trans': [False, "DBZC", "VR"]  # RaxPol 31 May - must specify for edit sweep files
                    }

# Dict for the standard deviation of obs_error for reflectivity or velocity (these values are squared when written to DART) 
           
_obs_errors = {
                'reflectivity'  : 5.0,
                '0reflectivity' : 5.0, 
                'velocity'      : 3.0
              }
        
# default plot levels when more than 1 is desired - mostly for debugging
_plevels = [0, 1, 2, 3, 4, 5]

# List for when window is used to find a file within a specific window - units are minutes
           
_window_param = [ -10, 10 ]

#=========================================================================================
# Class variable used as container

class Gridded_Field(object):
  
  def __init__(self, name, data=None, **kwargs):    
    self.name = name    
    self.data = data    
    
    if kwargs != None:
      for key in kwargs:  setattr(self, key, kwargs[key])
      
  def keys(self):
    return self.__dict__

#=========================================================================================
# Read in sounding file

def get_sounding(file=None):
    
    if file == None:
        file = './KGRB_2019072000_raob.txt'
        df = pd.read_fwf(file, skiprows=1, usecols=[1,6,7],
        names=['height', 'direction', 'speed'])
        wind_dir = df['direction'].values * units.degrees
        wind_spd = df['speed'].values*.514
        u, v = mpcalc.wind_components(wind_spd, wind_dir)

        for n in np.arange(u.shape[0]):
            print("%8.1f  %6.3f  %6.3f  %5.2f  %5.2f  %5.2f" % 
                   (df['height'].values[n], df['direction'].values[n], wind_spd[n], u[n], v[n]))

        return Gridded_Field('sounding', height = df['height'].values,
                                         u_wind = u,
                                         v_wind = v)
    else:
        print('No sounding found')
        return None

#=========================================================================================
# DBZ Mask

def dbz_masking(ref, thin_zeros=2):

  if _grid_dict['MRMS_zeros'][0] == True:   # create one layer of zeros based on composite ref
  
      print("\n Creating new 0DBZ levels for output\n")
      
      nz, ny, nx = ref.data.shape
      
      zero_dbz = np.ma.zeros((ny, nx), dtype=np.float32)

      c_ref = ref.data.max(axis=0)  
      
      raw_field = np.where(c_ref.mask==True, 0.0, c_ref.data)
      
      max_neighbor = (ndimage.maximum_filter(raw_field, size=_grid_dict['halo_footprint']) > 0.1)
      
      zero_dbz.mask = np.where(max_neighbor, True, False)

# the trick here was to realize that you need to first flip the zero_dbz_mask array and then thin by shutting off mask      
      if thin_zeros > 0:
          mask2 = np.logical_not(zero_dbz.mask)                                            # true for dbz>10
          mask2[::thin_zeros, ::thin_zeros] = False
          zero_dbz.mask = np.logical_or(max_neighbor, mask2)
      
      ref.zero_dbz = zero_dbz

      new_z = np.ma.zeros((2, ny, nx), dtype=np.float32)

      for n, z in enumerate(_grid_dict['MRMS_zeros'][1:]):
          new_z[n] = z
                
      ref.zero_dbz_zg = new_z
      ref.cref        = c_ref
     
  else: 
      mask = (ref.data.mask == True)  # this is the original no data mask from interp
  
      ref.data.mask = False           # set the ref mask to false everywhere
    
      ref.data[mask] = 0.0             
  
      nlevel = ref.data.shape[0]
  
      for n in np.arange(nlevel):  
          max_values = ndimage.maximum_filter(ref.data[n,:,:], size=_grid_dict['halo_footprint'])
          halo  = (max_values > 0.1) & mask[n]    
          ref.data.mask[n,halo] = True

      if thin_zeros > 0:

          for n in np.arange(nlevel):   
              mask1 = np.logical_and(ref.data[n] < 0.1, ref.data.mask[n] == False)  # true for dbz=0
              mask2 = ref.data.mask[n]                                              # true for dbz>10
              mask1[::thin_zeros, ::thin_zeros] = False
              ref.data.mask[n] = np.logical_or(mask1, mask2)

  if _grid_dict['max_height'] > 0:
      mask1  = (ref.zg - ref.radar_hgt) > _grid_dict['max_height']
      mask2 = ref.data.mask
      ref.data.mask = np.logical_or(mask1, mask2)
        
  return ref

def vel_masking(vel, ref, volume):
    """
        VR Masking
    """
    # Mask the radial velocity where dbz is masked

    print(" Size of VR  from objective analysis  mask: %d" % np.sum(vel.data.mask == False))
    print(" Size of dBZ from objective analysis  mask: %d" % np.sum(ref.data.mask == False))

    vel.data.mask = np.logical_or(vel.data.mask, ref.data.mask)

    print(" Size of VR mask after minimum dBZ mask: %d" % np.sum(vel.data.mask == False))

    # Limit max/min values of radial velocity (bad unfolding, too much "truth")

    for m in np.arange(vel.data.shape[0]):
    #      Vr_max = volume.get_nyquist_vel(m)
        mask1  = (np.abs(vel.data[m]) > _radar_parameters['max_Radial_Velocity'])
        vel.data.mask[m] = np.logical_or(vel.data.mask[m], mask1)
            
    if _grid_dict['max_height'] > 0:
        mask1 = (vel.zg - vel.radar_hgt) > _grid_dict['max_height']
    #     print(" Size of height mask: %d" % np.sum(mask1 == False))
        mask2 = vel.data.mask
        vel.data.mask = np.logical_or(mask1, mask2)
        print(" Size of VR mask after height mask: %d" % np.sum(vel.data.mask == False))
        
    return vel

def grid_data(volume, field, LatLon=None):
    """
        Grid data using parameters defined above in grid_dict 
    """

    # Two ways to grid the data:  radar centered or external grid
    if LatLon == None:   # the grid is centered on the radar

        grid_spacing_xy = _grid_dict['grid_spacing_xy']
        domain_length   = _grid_dict['domain_radius_xy']
        grid_pts_xy     = 1 + 2*np.int(domain_length/grid_spacing_xy)
        nx, ny          = (grid_pts_xy, grid_pts_xy)
        radar_lat       = volume.latitude['data'][0]
        radar_lon       = volume.longitude['data'][0]
        xg              = -domain_length + grid_spacing_xy * np.arange(grid_pts_xy)
        yg              = -domain_length + grid_spacing_xy * np.arange(grid_pts_xy)
        
        map = Proj(proj='lcc', ellps='WGS84', datum='WGS84', lat_1=truelat1, lat_2=truelat2, lat_0=radar_lat, lon_0=radar_lon)
        xoffset, yoffset = list(map(radar_lon, radar_lat)) 
        lons, lats = list(map(xg, yg, inverse=True))
        
    else:  # grid based on model grid center LatLon

        grid_spacing_xy = _grid_dict['grid_spacing_xy']
        nx              = 1 + np.int(_grid_dict['model_grid_size'][0] / grid_spacing_xy)
        ny              = 1 + np.int(_grid_dict['model_grid_size'][1] / grid_spacing_xy)
        grid_pts_xy     = max(nx, ny)
        xg              = -0.5*_grid_dict['model_grid_size'][0] + grid_spacing_xy * np.arange(nx)
        yg              = -0.5*_grid_dict['model_grid_size'][1] + grid_spacing_xy * np.arange(ny)
        radar_lat       = volume.latitude['data'][0]
        radar_lon       = volume.longitude['data'][0]
        
        map = Proj(proj='lcc', ellps='WGS84', datum='WGS84', lat_1=truelat1, lat_2=truelat2, lat_0=LatLon[0], lon_0=LatLon[1])
        
        xoffset, yoffset = list(map(radar_lon, radar_lat))
        lons, lats = list(map(xg, yg, inverse=True))

    if _grid_dict['anal_method'] == 'Cressman':
        anal_method = 1
    else:
        anal_method = 2

    roi        = _grid_dict['ROI']
    nthreads   = _grid_dict['nthreads']
    min_count  = _grid_dict['min_count']
    min_weight = _grid_dict['min_weight']
    min_range  = _grid_dict['min_range']

    ########################################################################

    print('\n Gridding radar data with following parameters')
    print(' ---------------------------------------------\n')
    print(' Method of Analysis:      {}'.format(_grid_dict['anal_method']))
    print(' Horizontal grid spacing: {} km'.format(grid_spacing_xy/1000.))
    print(' Grid points in x,y:      {},{}'.format(int(nx),int(ny)))
    print(' Weighting function:      {}'.format(_grid_dict['anal_method']))
    print(' Radius of Influence:     {} km'.format(_grid_dict['ROI']/1000.))
    print(' Minimum gates:           {}'.format(min_count))
    print(' Minimum weight:          {}'.format(min_weight))
    print(' Minimum range:           {} km'.format(min_range/1000.))
    print(' Map projection:          {}'.format(_grid_dict['projection']))
    print(' Xoffset:                 {} km'.format(np.round(xoffset/1000.)))
    print(' Yoffset:                 {} km'.format(np.round(yoffset/1000.)))
    print(' Field to be gridded:     {}\n'.format(field)) 
    print(' Min / Max X grid loc:    {} <-> {} km\n'.format(0.001*xg[0], 0.001*xg[-1]))
    print(' Min / Max Y grid loc:    {} <-> {} km\n'.format(0.001*yg[0], 0.001*yg[-1]))
    print(' Min / Max Longitude:     {} <-> {} deg\n'.format(lons[0], lons[-1]))
    print(' Min / Max Latitude:      {} <-> {} deg\n'.format(lats[0], lats[-1]))
    print(' ---------------------------------------------\n') 

    def wf(z_in):
        """
            Local weight function for pyresample
        """

        if _grid_dict['anal_method'] == 'Cressman':
            w    = np.zeros((z_in.shape), dtype=np.float64)
            ww   = (roi**2 - z_in**2) / (roi**2 + z_in**2)
            mask = (np.abs(z_in) <  roi)
            w[mask] = ww[mask] 
            return w
            
        elif _grid_dict['anal_method'] == 'test':
            return np.ones((z_in.shape), dtype=np.float64)
            
        elif _grid_dict['anal_method'] == 'Barnes':

            return np.exp(-(z_in/roi)**2)
            
        else:  # Gasparoi and Cohen...

            gc = np.zeros((z_in.shape), dtype=np.float64)
            z = abs(z_in)
            r = z / roi
            z1 = (((( r/12.  -0.5 )*r  +0.625 )*r +5./3. )*r  -5. )*r + 4. - 2./(3.*r)
            z2 = ( ( ( -0.25*r +0.5 )*r +0.625 )*r  -5./3. )*r**2 + 1.
            m1 = np.logical_and(z >= roi, z < 2*roi)
            m2 = (z <  roi)
            gc[m1] = z1[m1]
            gc[m2] = z2[m2]      
            return gc


    tt = timeit.clock()

    #####################################################################################   
    try:
        v_iter = volume.iter_field(field)
        field_name = field
    except KeyError:
        print("\n No dealiased velocity present, gridding RAW radial velocity\n")
        v_iter = volume.iter_field("velocity")
        field_name = "velocity"
        
    if field == "reflectivity":
        sweeps = volume.reflectivity
    else:
        sweeps = volume.velocity

    #####################################################################################
    # Create 3D arrays for analysis grid, the vertical dimension is the number of tilts

    new_grid    = np.zeros((len(sweeps), ny, nx))
    new_mask    = np.full((len(sweeps), ny, nx), False)
    elevations  = np.zeros((len(sweeps),))
    sweep_time  = np.zeros((len(sweeps),))
    zgrid       = np.zeros((len(sweeps), ny, nx))
    nyquist     = np.zeros((len(sweeps),))
        
    # Grid only those valid sweeps 
    
    total = 0
    for n, sweep_level in enumerate(sweeps):
    
        sweep_data = volume.get_field(sweep_level, field_name)

        begin, end = volume.get_start_end(sweep_level)
        
        sweep_time[n] = volume.time['data'][begin:end].mean()
        elevations[n] = volume.get_elevation(sweep_level).mean()
        nyquist[n]    = volume.get_nyquist_vel(sweep_level)
        
        omask = (sweep_data.mask == False)
        
        x, y, z = volume.get_gate_x_y_z(sweep_level)
        
        obs = sweep_data[omask].ravel()
        xob = x[omask].ravel() + xoffset
        yob = y[omask].ravel() + yoffset
        
        ix = np.searchsorted(xg, xob)
        iy = np.searchsorted(yg, yob)
        
        if obs.size > 0:
            tmp = obs_2_grid2d(obs, xob, yob, xg, yg, ix, iy, anal_method, min_count, min_weight, min_range, \
                                        2.0*grid_spacing_xy, _missing)
            new_grid[n] = tmp
            new_mask[n] = (tmp <= _missing)
            total = total + np.sum(new_mask[n] == False)
        else:
            new_grid[n] = np.full((ny,nx), _missing)
            new_mask[n] = np.full((ny,nx), True)
            
        if field == "reflectivity":
            new_mask[n] = np.logical_or(new_mask[n], new_grid[n] < _radar_parameters['min_dbz_analysis'])
            print(" Sweep: %2.2d Elevation: %5.2f  Number of valid reflectivity points:  %d" % (n, elevations[n],np.sum(new_mask[n]==False)))
        else:
            print(" Sweep: %2.2d Elevation: %5.2f  Number of valid grid points:  %d" % (n, elevations[n],np.sum(new_mask[n]==False)))

        # Create z-field

        zobs= z.ravel()
        xob = x.ravel() + xoffset
        yob = y.ravel() + yoffset

        zobs = np.where( zobs < 0.0, 0.0, zobs)
        
        ix = np.searchsorted(xg, xob)
        iy = np.searchsorted(yg, yob)

        #      tmp=inverse_distance(xob, yob, zobs, xg, yg, 2.0*grid_spacing_xy, gamma=None, kappa=None,
        #                    min_neighbors=min_count, kind='cressman')

        tmp = obs_2_grid2d(zobs, xob, yob, xg, yg, ix, iy, 1, 1, 0.1, min_range, 2.0*grid_spacing_xy, -99999.)
        zgrid[n] = tmp
        
    print("\n %f secs to run superob analysis for all levels \n" % (timeit.clock()-tt))

    print('\n Total number of valid obs in volume: %d \n' % total)
    print('\n Total number of valid obs in volume: %d \n' % np.sum(new_mask == False))

    return Gridded_Field("data_grid", field = field, data = np.ma.array(new_grid, mask=new_mask), basemap = map, 
                            xg = xg, yg = yg, zg = np.ma.array(zgrid, mask=new_mask),                   
                            lats = lats, lons = lons, elevations=elevations,
                            radar_lat = radar_lat, radar_lon = radar_lon, radar_hgt=volume.altitude['data'][0],
                            time = volume.time, sweep_time = sweep_time, metadata = volume.metadata, nyquist = nyquist  ) 

###########################################################################################
#
# Read environment variables for plotting a shapefile

def plot_shapefiles(map, shapefiles=None, color='k', linewidth=0.5, counties=False, ax=None):

    if shapefiles:
        try:
            shapelist = os.getenv(shapefiles).split(":")

            if len(shapelist) > 0:

                for item in shapelist:
                    items      = item.split(",")
                    shapefile  = items[0]
                    color      = items[1]
                    linewidth  = items[2]

                    s = map.readshapefile(shapefile,'myshapes',drawbounds=False)

                    for shape in map.counties:
                        xx, yy = list(zip(*shape))
                        map.plot(xx, yy, color=color, linewidth=linewidth, ax=ax, zorder=4)

        except OSError:
            print("PLOT_SHAPEFILES:  NO SHAPEFILE ENV VARIABLE FOUND ")
            
    if counties:
            map.drawcounties(ax=ax, linewidth=0.5, color='k', zorder=5)
            
########################################################################
#
# Create two panel plot of processed, gridded velocity and reflectivity data  

def plot_gridded(ref, vel, sweep, fsuffix=None, dir=".", shapefiles=None, interactive=True, LatLon=None):
  
# Set up colormaps 

  from matplotlib.colors import BoundaryNorm
   
  cmapr = cm.NWSRef
  cmapr.set_bad('white',1.0)
  cmapr.set_under('white',1.0)

  cmapv = cm.Carbone42
  cmapv.set_bad('white',1.)
  cmapv.set_under('black',1.)
  cmapv.set_over('black',1.)
  
  normr = BoundaryNorm(np.arange(10, 85, 5), cmapr.N)
  normv = BoundaryNorm(np.arange(-48, 50, 2), cmapv.N)
  
  min_dbz = _radar_parameters['min_dbz_analysis']  
  xwidth = ref.xg.max() - ref.xg.min()
  ywidth = ref.yg.max() - ref.yg.min()

# Create png file label

  if fsuffix == None:
      print("\n opaws2D.grid_plot:  No output file name is given, writing to %s" % "VR_RF_...png")
      filename = "%s/VR_RF_%2.2d_plot.png" % (dir, sweep)
      print("\n opaws2D.grid_plot:  No output file name is given, writing to %s" % filename)
  else:
      filename = "%s_%2.2d.png" % (os.path.join(dir, fsuffix), sweep)
      print("\n opaws2D.grid_plot:  Writing plot to %s" % filename)

  fig, (ax1, ax2) = plt.subplots(1, 2, sharey=True, figsize=(14,10))
  
# Set up coordinates for the plots

  if LatLon == None:
      bgmap = Basemap(projection=_grid_dict['projection'], width=xwidth, \
                  height=ywidth, resolution='c', lat_0=ref.radar_lat, lon_0=ref.radar_lon, ax=ax1)
      xoffset, yoffset = bgmap(ref.radar_lon, ref.radar_lat)
      xg, yg = bgmap(ref.lons, ref.lats)
  else:
      bgmap = Basemap(projection=_grid_dict['projection'], width=xwidth, \
                  height=ywidth, resolution='c', lat_0=LatLon[0], lon_0=LatLon[1], ax=ax1)
      xoffset, yoffset = bgmap(ref.radar_lon, ref.radar_lat)    
      xg, yg = bgmap(ref.lons, ref.lats)
      
#   print xg.min(), xg.max(), xg.shape
#   print yg.min(), yg.max(), yg.shape
  
  xg_2d, yg_2d = np.meshgrid(xg, yg)
 
#   print xg.min(), xg_2d[0,0], xg_2d[-1,-1], xg.max(), xg.shape
#   print yg.min(), yg.max(), yg.shape
 
# fix xg, yg coordinates so that pcolormesh plots them in the center.

  dx2 = 0.5*(ref.xg[1] - ref.xg[0])
  dy2 = 0.5*(ref.yg[1] - ref.yg[0])
  
  xe = np.append(xg-dx2, [xg[-1] + dx2])
  ye = np.append(yg-dy2, [yg[-1] + dy2])

# REFLECTVITY PLOT

  if shapefiles:
      plot_shapefiles(bgmap, shapefiles=shapefiles, counties=_plot_counties, ax=ax1)
  else:
      plot_shapefiles(bgmap, counties=_plot_counties, ax=ax1)
 
  bgmap.drawparallels(list(range(10,80,1)),    labels=[1,0,0,0], linewidth=0.5, ax=ax1)
  bgmap.drawmeridians(list(range(-170,-10,1)), labels=[0,0,0,1], linewidth=0.5, ax=ax1)

  im1 = bgmap.pcolormesh(xe, ye, ref.data[sweep], cmap=cmapr, vmin = _ref_scale[0], vmax = _ref_scale[1], ax=ax1)
  cbar = bgmap.colorbar(im1, location='right')
  cbar.set_label('Reflectivity (dBZ)')
  ax1.set_title('Thresholded Reflectivity (Gridded)')
  bgmap.scatter(xoffset,yoffset, c='k', s=50., alpha=0.8, ax=ax1)
  
  at = AnchoredText("Max dBZ: %4.1f" % (ref.data[sweep].max()), loc=4, prop=dict(size=12), frameon=True,)
  at.patch.set_boxstyle("round,pad=0.,rounding_size=0.2")
  ax1.add_artist(at)

# Plot zeros as "o"

  try:
      r_mask = (ref.zero_dbz.mask == False)
#     print("\n Plotting zeros from MRMS level\n")
      bgmap.scatter(xg_2d[r_mask], yg_2d[r_mask], s=25, facecolors='none', \
                    edgecolors='k', alpha=1.0, ax=ax1) 
                    
  except AttributeError:
      print("\n Plotting zeros from full 3D grid level (non-MRMS form)\n")
      r_mask = np.logical_and(ref.data[sweep] < 1.0, (ref.data.mask[sweep] == False))
      bgmap.scatter(xg_2d[r_mask], yg_2d[r_mask], s=25, facecolors='none', \
                    edgecolors='k', alpha=1.0, ax=ax1)
  
# RADIAL VELOCITY PLOT

  if LatLon == None:
      bgmap = Basemap(projection=_grid_dict['projection'], width=xwidth, \
                  height=ywidth, resolution='c', lat_0=ref.radar_lat,lon_0=ref.radar_lon, ax=ax2)
      xoffset, yoffset = bgmap(ref.radar_lon, ref.radar_lat)
  else:
      bgmap = Basemap(projection=_grid_dict['projection'], width=xwidth, \
                  height=ywidth, resolution='c', lat_0=LatLon[0], lon_0=LatLon[1], ax=ax2)
      xoffset, yoffset = bgmap(ref.radar_lon, ref.radar_lat)            
  
                  
  if shapefiles:
      plot_shapefiles(bgmap, shapefiles=shapefiles, counties=_plot_counties, ax=ax2)
  else:
      plot_shapefiles(bgmap, counties=_plot_counties, ax=ax2)
    
  bgmap.drawparallels(list(range(10,80,1)),labels=[1,0,0,0], linewidth=0.5, ax=ax2)
  bgmap.drawmeridians(list(range(-170,-10,1)),labels=[0,0,0,1],linewidth=0.5, ax=ax2)

  vr_mask = (vel.data.mask == False)[sweep]
  vr_data = vel.data[sweep]
  
  im1 = bgmap.pcolormesh(xe, ye, vr_data, cmap=cmapv, vmin=_vr_scale[0], vmax=_vr_scale[1], ax=ax2)
  cbar = bgmap.colorbar(im1,location='right')
  cbar.set_label('Dealised Radial Velocity (meters_per_second)')
  ax2.set_title('Gridded/Thresholded/Unfolded VR / Nyquist: %4.1f m/s' % vel.nyquist[sweep]) 
  bgmap.scatter(xoffset,yoffset, c='k', s=50., alpha=0.8, ax=ax2)

  at = AnchoredText("Max Vr: %4.1f \nMin Vr: %4.1f " % \
                 (vr_data.max(), vr_data.min()), loc=4, prop=dict(size=12), frameon=True,)
  at.patch.set_boxstyle("round,pad=0.,rounding_size=0.2")
  ax2.add_artist(at)  
    
# Now plot locations of nan data

  v_mask = (vel.data.mask == True)
  bgmap.scatter(xg_2d[v_mask[sweep]], yg_2d[v_mask[sweep]], c='k', s = 1., alpha=0.5, ax=ax2)

# Get other metadata....for labeling

  instrument_name = ref.metadata['instrument_name']
  time_start = ncdf.num2date(ref.time['data'][0], ref.time['units'])
  time_text = time_start.isoformat().replace("T"," ")
  title = '\nDate:  %s   Time:  %s Z   Elevation:  %2.2f deg' % (time_text[0:10], time_text[10:19], ref.elevations[sweep])
  plt.suptitle(title, fontsize=24)
  
  plt.savefig(filename)
  
  if interactive:  plt.show()

#####################################################################################################
def write_radar_file(ref, vel, filename=None):
    
  _time_units    = 'seconds since 1970-01-01 00:00:00'
  _calendar      = 'standard'

  if filename == None:
      print("\n write_DART_ascii:  No output file name is given, writing to %s" % "obs_seq.txt")
      filename = "obs_seq.nc"
  else:
      dirname = os.path.dirname(filename)
      basename = "%s_%s.nc" % ("obs_seq", os.path.basename(filename))
      filename =  os.path.join(dirname, basename)

  _stringlen     = 8
  _datelen       = 19
     
# Extract grid and ref data
        
  dbz        = ref.data
  lats       = ref.lats
  lons       = ref.lons
  hgts       = ref.zg + ref.radar_hgt
  kind       = ObType_LookUp(ref.field.upper())  
  R_xy       = np.sqrt(ref.xg[20]**2 + ref.yg[20]**2)
  elevations = beam_elv(R_xy, ref.zg[:,20,20])
  
# if there is a zero dbz obs type, reform the data array 
  try:
      nx1, ny1       = ref.zero_dbz.shape
      zero_data      = np.ma.zeros((2, ny1, nx1), dtype=np.float32)
      zero_hgts      = np.ma.zeros((2, ny1, nx1), dtype=np.float32)
      zero_data[0]   = ref.zero_dbz
      zero_data[1]   = ref.zero_dbz
      zero_hgts[0:2] = ref.zero_dbz_zg[0:2]
      cref           = ref.cref
      zero_flag = True
      print("\n write_DART_ascii:  0-DBZ separate type added to netcdf output\n")
  except AttributeError:
      zero_flag = False
      print("\n write_DART_ascii:  No 0-DBZ separate type found\n")
      
# Extract velocity data
  
  vr                  = vel.data
  platform_lat        = vel.radar_lat
  platform_lon        = vel.radar_lon
  platform_hgt        = vel.radar_hgt

# Use the volume mean time for the time of the volume
      
  dtime   = ncdf.num2date(ref.time['data'].mean(), ref.time['units'])
  days    = ncdf.date2num(dtime, units = "days since 1601-01-01 00:00:00")
  seconds = np.int(86400.*(days - np.floor(days)))  
  
# create the fileput filename and create new netCDF4 file

#filename = os.path.join(path, "%s_%s%s" % ("Inflation", DT.strftime("%Y-%m-%d_%H:%M:%S"), ".nc" ))

  print("\n -->  Writing %s as the radar file..." % (filename))
    
  rootgroup = ncdf.Dataset(filename, 'w', format='NETCDF4')
      
# Create dimensions

  shape = dbz.shape
  
  rootgroup.createDimension('nz',   shape[0])
  rootgroup.createDimension('ny',   shape[1])
  rootgroup.createDimension('nx',   shape[2])
  rootgroup.createDimension('stringlen', _stringlen)
  rootgroup.createDimension('datelen', _datelen)
  if zero_flag:
      rootgroup.createDimension('nz2',   2)
  
# Write some attributes

  rootgroup.time_units   = _time_units
  rootgroup.calendar     = _calendar
  rootgroup.stringlen    = "%d" % (_stringlen)
  rootgroup.datelen      = "%d" % (_datelen)
  rootgroup.platform_lat = platform_lat
  rootgroup.platform_lon = platform_lon
  rootgroup.platform_hgt = platform_hgt

# Create variables

  R_type  = rootgroup.createVariable('REF', 'f4', ('nz', 'ny', 'nx'), zlib=True, shuffle=True )    
  V_type  = rootgroup.createVariable('VEL', 'f4', ('nz', 'ny', 'nx'), zlib=True, shuffle=True )
  
  if zero_flag:
      R0_type   = rootgroup.createVariable('0REF',  'f4', ('nz2', 'ny', 'nx'), zlib=True, shuffle=True )    
      Z0_type   = rootgroup.createVariable('0HGTS', 'f4', ('nz2', 'ny', 'nx'), zlib=True, shuffle=True )
      CREF_type = rootgroup.createVariable('CREF', 'f4', ('ny', 'nx'), zlib=True, shuffle=True )
      
  V_dates = rootgroup.createVariable('date', 'S1', ('datelen'), zlib=True, shuffle=True)
  V_xc    = rootgroup.createVariable('XC', 'f4', ('nx'), zlib=True, shuffle=True)
  V_yc    = rootgroup.createVariable('YC', 'f4', ('ny'), zlib=True, shuffle=True)
  V_el    = rootgroup.createVariable('EL', 'f4', ('nz'), zlib=True, shuffle=True)

  V_lat   = rootgroup.createVariable('LATS', 'f4', ('ny'), zlib=True, shuffle=True)
  V_lon   = rootgroup.createVariable('LONS', 'f4', ('nx'), zlib=True, shuffle=True)
  V_hgt   = rootgroup.createVariable('HGTS', 'f4', ('nz', 'ny', 'nx'), zlib=True, shuffle=True)

# Write variables

  rootgroup.variables['date'][:] = ncdf.stringtoarr(dtime.strftime("%Y-%m-%d_%H:%M:%S"), _datelen)
  
  rootgroup.variables['REF'][:,:,:] = dbz[:]
  rootgroup.variables['VEL'][:,:,:] = vr[:]

  rootgroup.variables['XC'][:]   = ref.xg[:]
  rootgroup.variables['YC'][:]   = ref.yg[:]
  rootgroup.variables['EL'][:]   = elevations[:]
  rootgroup.variables['HGTS'][:] = ref.zg[:]
  rootgroup.variables['LATS'][:] = lats[:]
  rootgroup.variables['LONS'][:] = lons[:]
  
  if zero_flag:
       rootgroup.variables['0REF'][:]   = zero_data
       rootgroup.variables['0HGTS'][:]  = zero_hgts
       rootgroup.variables['CREF'][:]   = cref
  
  rootgroup.sync()
  rootgroup.close()
  
  return filename  
#=========================================================================================
# Defines the data frame for each observation type
#
def obs_seq_xarray(len):

    return (np.recarray(len,
                    dtype = [
                             ('value',               'f8'),
                             ('lat',                 'f8'),
                             ('lon',                 'f8'),
                             ('height',              'f8'),
                             ('error_var',           'f4'),
                             ('utime',               'f8'),
                             ('date',                'S128'),
                             ('day',                 'i8'),
                             ('second',              'i8'),
                             ('platform_lat',        'f8'),
                             ('platform_lon',        'f8'),
                             ('platform_hgt',        'f8'),
                             ('platform_dir1',       'f8'),
                             ('platform_dir2',       'f8'),
                             ('platform_dir3',       'f8'),
                             ('platform_nyquist',    'f8')
                            ]), \
                         {
                           'lat':   ["degrees", "latitude of observation"],
                           'lon':   ["degrees", "longitude of observation (deg. west)"],
                           'height':["meters",  "height above sea level"],
                           'utime': ["seconds since 1970-01-01 00:00:00", "time of observation"],
                           'date':  ["", "date and time of obsevation"]
                          })

#####################################################################################################
def write_obs_seq_xarray(field, filename=None, obs_error=3., volume_name=None):

   _time_units    = 'seconds since 1970-01-01 00:00:00'
   _calendar      = 'standard'

   if filename == None:
      print("\n WRITE_DATA_XARRAY:  No output file name is given, writing to %s" % "obs_seq.txt")
      filename = "obs_seq.nc"
   else:
      dirname = os.path.dirname(filename)
      basename = "%s_%s.nc" % ("obs_seq", os.path.basename(filename))
      filename =  os.path.join(dirname, basename)

   _stringlen     = 8
   _datelen       = 19

# Extract data.city data

   fld           = field.data.data[:,:,:]
   mask          = field.data.mask[:,:,:]
   lats          = field.lats
   lons          = field.lons
   xgrid         = field.xg[:]
   ygrid         = field.yg[:]
   zgrid         = field.zg[:,:,:]
   msl_hgt       = field.zg[:,:,:] + field.radar_hgt

   platform_lat  = field.radar_lat
   platform_lon  = field.radar_lon
   platform_hgt  = field.radar_hgt

# Use the volume mean time for the time of the volume

   utime   = ncdf.num2date(field.time['data'].mean(), field.time['units'])
   secs    = ncdf.date2num(utime, units = _time_units)
# Retain DART time stamps
   days    = ncdf.date2num(utime, units = "days since 1601-01-01 00:00:00")
   seconds = np.int(86400.*(days - np.floor(days)))

   print("\n -->  Writing %s as the radar file..." % (filename))
#  mask_check = data.mask && numpy.isnan().any()

   nobs = np.sum(mask==False)
   print("\n -----> Number of good observations for xarray:  %d" % nobs)

   # Create numpy rec array that can be converted to a pandas table.

   out, attributes = obs_seq_xarray(nobs)

   # There is a far better way to do this but this is fast enough for now...

   n = 0

   for k in np.arange(fld.shape[0]):
      for j in np.arange(fld.shape[1]):
         for i in np.arange(fld.shape[2]):

            if mask[k,j,i] == False:

               dx    = xgrid[i]
               dy    = ygrid[j]
               dz    = zgrid[k,j,i]
               range = np.sqrt(dx**2 + dy**2 + dz**2)
               dir1  = dx / range
               dir2  = dy / range
               dir3  = dz / range

               out.value[n]              = fld[k,j,i]
               out.error_var[n]          = obs_error**2
               out.lon[n]                = lons[i]
               out.lat[n]                = lats[j]
               out.height[n]             = msl_hgt[k,j,i]
               out.date[n]               = utime
               out.utime[n]              = secs
               out.day[n]                = days
               out.second[n]             = seconds
               out.platform_lon[n]       = platform_lon
               out.platform_lat[n]       = platform_lat
               out.platform_hgt[n]       = platform_hgt
               out.platform_dir1[n]      = dir1
               out.platform_dir2[n]      = dir2
               out.platform_dir3[n]      = dir3
               out.platform_nyquist[n]   = field.nyquist[k]

               n = n + 1
  
   # Create an xarray dataset for file I/O
   xa = xr.Dataset(pd.DataFrame.from_records(out))

#  # reset index to be a master index across all obs
   xa = xa.rename({'dim_0': 'index'})

   # Write the xarray file out (this is all there is, very nice guys!)
   xa.to_netcdf(filename, mode='w')
   xa.close()

   # Add attributes to the files

   fnc = ncdf.Dataset(filename, mode = 'a')
   fnc.history = "Created " + DT.datetime.today().strftime("%Y%m%d_%H%M")
   fnc.version = "Version 1.0a by Lou Wicker and Thomas Jones (NSSL)"
   if volume_name != None:
       fnc.version = "Created from the WSR88D radar volume:  %s" % volume_name

   for key in list(attributes.keys()):
       fnc.variables[key].units = attributes[key][0]
       fnc.variables[key].description = attributes[key][1]

   fnc.sync()
   fnc.close()

#######################################################################
def clock_string():
    local_time = timeit.localtime()  # get this so we know when script was submitted...
    return "%s%2.2d%2.2d_%2.2d%2.2d" % (local_time.tm_year, \
                                         local_time.tm_mon,  \
                                         local_time.tm_mday, \
                                         local_time.tm_hour, \
                                         local_time.tm_min)


def processVolume(volume, unfold_type, options, cLatLon, sweep_num, out_filename, fname):
    # Modern level-II files need to be mapped to figure out where the super-res velocity and reflectivity fields are located in file

    ret = volume_mapping(volume)

    # Now we do QC

    tim0 = timeit.time()

    if options.qc == "None":
        print("\n No quality control will be done on data")
        gatefilter = volume_prep(volume, QC_type = options.qc, thres_vr_from_ref = False, \
                                max_range = _radar_parameters['max_range'])
    else:
        print("\n QC type:  %s " % options.qc)
        gatefilter = volume_prep(volume, QC_type = options.qc, thres_vr_from_ref = _thres_vr_from_ref, \
                                max_range = _radar_parameters['max_range'])


    opaws2D_QC_cpu = timeit.time() - tim0
    
    print("\n Time for quality controling the data: {} seconds".format(opaws2D_QC_cpu))
    print('\n ================================================================================')
    
            
    # For some reason, you need to do velocity unfolding first....then QC the rest of the data

    tim0 = timeit.time()      

    print('\n ================================================================================')

    if unfold_type == None:
        vr_field = "velocity"
        vr_label = "Radial Velocity"
    else:
        vr_field, vr_label = velocity_unfold(volume, unfold_type=unfold_type, 
                                            gatefilter=gatefilter, 
                                            interval_splits=_radar_parameters['region_interval_splits'],
                                            wind_profile = None)

    opaws2D_unfold_cpu = timeit.time() - tim0

    print("\n Time for unfolding velocity: {} seconds".format(opaws2D_unfold_cpu))
    print('\n ================================================================================')

    # Now grid the reflectivity (embedded call) and then mask it off based on parameters set at top

    ref = dbz_masking(grid_data(volume, "reflectivity", LatLon=cLatLon), thin_zeros=_grid_dict['thin_zeros'])

    # Finally, regrid the radial velocity

    vel = grid_data(volume, vr_field, LatLon=cLatLon)
    
    # Mask it off based on dictionary parameters set at top

    if _grid_dict['mask_vr_with_dbz']:
        vel = vel_masking(vel, ref, volume)

    opaws2D_regrid_cpu = timeit.time() - tim0

    print("\n Time for gridding fields: {} seconds".format(opaws2D_regrid_cpu))
    
    print('\n ================================================================================')

    if options.write == True:      
        print('\n WRITING XARRAY: {}\n'.format(out_filename))
        ret = write_obs_seq_xarray(vel, filename=out_filename, obs_error= _obs_errors['velocity'], \
                                    volume_name=os.path.basename(fname))

        print('\n WRITING DART: {}\n'.format(out_filename))
        ret = opaws_write_DART_ascii(vel, filename=out_filename, grid_dict=_grid_dict, \
                                obs_error=[_obs_errors['velocity']] )

        if options.onlyVR != True:
            print('\n WRITING DART ONLY VR: {}\n'.format(out_filename))
            ret = opaws_write_DART_ascii(ref, filename=out_filename+"_RF", grid_dict=_grid_dict, \
                                obs_error=[_obs_errors['reflectivity'], _obs_errors['0reflectivity']])
        
    if len(sweep_num) > 0:
        fplotname = os.path.basename(out_filename)
        for pl in sweep_num:
            plottime = plot_gridded(ref, vel, pl, fsuffix=fplotname, dir=options.out_dir, \
                        shapefiles=options.shapefiles, interactive=options.interactive, LatLon=cLatLon)


def processVolumes(radar, run_time, options):
    if options.unfold == "phase":
        print("\n opaws2D dealias_unwrap_phase unfolding will be used\n")
        unfold_type = "phase"
    elif options.unfold == "region":
        print("\n opaws2D dealias_region_based unfolding will be used\n")
        unfold_type = "region"
    else:
        print("\n ***** INVALID OR NO VELOCITY DEALIASING METHOD SPECIFIED *****")
        print("\n          NO VELOCITY UNFOLDING DONE...\n\n")
        unfold_type = None

    if options.newse:
        print(" \n now processing NEWSe radar file....\n ")
        cLatLon = parse_NEWSe_radar_file(options.newse, getLatLon=True)
    else:
        cLatLon = None

    if options.method:
        _grid_dict['anal_method'] = options.method

    if options.dx:
        _grid_dict['grid_spacing_xy'] = options.dx
        _grid_dict['ROI'] = options.dx / 0.707
        
    if options.roi:
        _grid_dict['ROI'] = options.roi

    if options.plot == 0:
        sweep_num = []
    elif options.plot > 0:
        sweep_num = [options.plot]
        if not os.path.exists("images"):
            os.mkdir("images")
    else:
        sweep_num = _plevels
        if not os.path.exists("images"):
            os.mkdir("images")

    # Read input file and create radar object

    t0 = timeit.time()

    for volume in getRadarProducts(radar, run_time):
        out_file = os.path.join(options.out_dir, "%s_VR_%s" % (radar, run_time.strftime("%Y%m%d_%H%M")))
        processVolume(volume, unfold_type, options, cLatLon, sweep_num, out_file)


########################################################################
# Main function

def run(options):
    print(' ================================================================================')
    print('')
    print('                   BEGIN PROGRAM opaws2D                     ')
    print('')
    print('')
    print(' ================================================================================')

    # Create directory for output files

    if not os.path.exists(options.out_dir):
        os.mkdir(options.out_dir)

    out_filenames = []
    in_filenames  = []

    if options.dname == None: 
        if options.fname == None:
            print("\n\n ***** USER MUST SPECIFY NEXRAD LEVEL II (MESSAGE 31) FILE! *****")
            print("\n\n *****                     OR                               *****")
            print("\n\n *****               CFRADIAL FILE!                         *****")
            print("\n                         EXITING!\n\n")
            parser.print_help()
            print()
            sys.exit(1)
        else:
            in_filenames.append(os.path.abspath(options.fname))
            strng = os.path.basename(in_filenames[0]).split("_V06")[0]
            strng = strng[0:4] + "_" + strng[4:]
            strng = os.path.join(options.out_dir, strng)
            out_filenames.append(strng) 
    else:
        if options.window:
            ttime      = DT.datetime.strptime(options.window, "%Y,%m,%d,%H,%M")
            start_time = DT.datetime.strptime(options.window, "%Y,%m,%d,%H,%M") + DT.timedelta(minutes=_window_param[0])
            stop_time  = DT.datetime.strptime(options.window, "%Y,%m,%d,%H,%M") + DT.timedelta(minutes=_window_param[1])

            if _AWS_L2Files:
                in_filenames = glob.glob(_AWS_L2Name_Style % (os.path.abspath(options.dname),start_time.strftime("%Y%m%d_%H"))) \
                            + glob.glob(_AWS_L2Name_Style % (os.path.abspath(options.dname),stop_time.strftime("%Y%m%d_%H")))
            else:
                in_filenames = glob.glob(_LDM_L2Name_Style % (os.path.abspath(options.dname),start_time.strftime("%Y%m%d_%H"))) \
                            + glob.glob(_LDM_L2Name_Style % (os.path.abspath(options.dname),stop_time.strftime("%Y%m%d_%H")))

            if len(in_filenames) == 0:
                print("\n COULD NOT find any files for radar %s between %s and %s, EXITING" \
                        % (os.path.abspath(options.dname),start_time.strftime("%Y%m%d_%H"),stop_time.strftime("%Y%m%d_%H")))
                sys.exit(0)
            else:
                print("\n WINDOW IS SUPPLIED, WILL LOOK FOR AN INDIVIDUAL FILE.... \n ")
                print("\n WINDOW_START:  %s" % start_time.strftime("%Y,%m,%d,%H,%M") )
                print(" WINDOW_END:    %s, will search %d files for closest time " \
                        % (stop_time.strftime("%Y,%m,%d,%H,%M"), len(in_filenames)) )
        else:
            in_filenames = glob.glob("%s/*" % os.path.abspath(options.dname))
            if len(in_filenames) == 0:
                print("\n COULD NOT find any files for radar: %s, EXITING" % (os.path.abspath(options.dname)))
                sys.exit(0)
            else:
                print("\n NO WINDOW SUPPLIED, PROCESSING WHOLE DIRECTORY.... \n ")
                print("\n opaws2D:  Processing %d files in the directory:  %s\n" % (len(in_filenames), options.dname))
                print("\n opaws2D:  First file is %s" % (in_filenames[0]))
                print("\n opaws2D:  Last  file is %s" % (in_filenames[-1]))

        if debug:  
            print(in_filenames)

        # if cfradial files....
        if in_filenames[0][-3:] == ".nc":
            for item in in_filenames:
                strng = os.path.basename(item).split(".")[0:2]
                strng = strng[0] + "_" + strng[1]
                strng = os.path.join(options.out_dir, strng)
                out_filenames.append(strng) 
        # WSR88D files
        else:
            for item in in_filenames:
                if options.window:   # for real time processing, we will timestamp the file with the analysis time
                    strng = "%s_VR_%s" % (os.path.basename(item)[0:4], ttime.strftime("%Y%m%d_%H%M"))
                    strng = os.path.join(options.out_dir, strng)
                    out_filenames.append(strng)
                else:
                    strng = os.path.basename(item)[0:18]
                    strng = os.path.join(options.out_dir, strng)
                    out_filenames.append(strng)

    if options.unfold == "phase":
        print("\n opaws2D dealias_unwrap_phase unfolding will be used\n")
        unfold_type = "phase"
    elif options.unfold == "region":
        print("\n opaws2D dealias_region_based unfolding will be used\n")
        unfold_type = "region"
    else:
        print("\n ***** INVALID OR NO VELOCITY DEALIASING METHOD SPECIFIED *****")
        print("\n          NO VELOCITY UNFOLDING DONE...\n\n")
        unfold_type = None

    if options.newse:
        print(" \n now processing NEWSe radar file....\n ")
        cLatLon = parse_NEWSe_radar_file(options.newse, getLatLon=True)
    else:
        cLatLon = None

    if options.method:
        _grid_dict['anal_method'] = options.method

    if options.dx:
        _grid_dict['grid_spacing_xy'] = options.dx
        _grid_dict['ROI'] = options.dx / 0.707
        
    if options.roi:
        _grid_dict['ROI'] = options.roi

    if options.plot == 0:
        sweep_num = []
    elif options.plot > 0:
        sweep_num = [options.plot]
        if not os.path.exists("images"):
            os.mkdir("images")
    else:
        sweep_num = _plevels
        if not os.path.exists("images"):
            os.mkdir("images")

    # Read input file and create radar object

    t0 = timeit.time()

    # Preprocessing to find closest file....

    if options.window:
        try:
            analysisT     = DT.datetime.strptime(options.window, "%Y,%m,%d,%H,%M")
            xfiles        = [os.path.basename(f) for f in in_filenames]
            if _AWS_L2Files:
                xfiles_DT     = [DT.datetime.strptime("%s" % f[4:19], "%Y%m%d_%H%M%S") for f in xfiles]
            else:
                xfiles_DT     = [DT.datetime.strptime("%s" % f[5:20], "%Y%m%d_%H%M%S") for f in xfiles]
            in_filenames  = [in_filenames[xfiles_DT.index(min(xfiles_DT, key=lambda d:  abs(d - analysisT)))]]
            out_filenames = [out_filenames[xfiles_DT.index(min(xfiles_DT, key=lambda d:  abs(d - analysisT)))]]
            print("\n FOUND CLOSEST FILE:   %s" % in_filenames[0] )
        except:
            print("\n COULD NOT FILE CLOSEST FILE, exiting: %s <----> %s" % (in_filenames[0], in_filenames[-1]) )
            sys.exit(0)

    for n, fname in enumerate(in_filenames):

        # the check for file size is to make sure there is data in the LVL2 file
        try:
            if os.path.getsize(fname) < 2048000:
                print('\n File {} is less than 2 mb, skipping...'.format(fname))
                continue
        except:
            continue

        tim0 = timeit.time() 

        print('\n READING: {}\n'.format(fname))

        if fname[-3:] == ".nc":
            if _radar_parameters['field_label_trans'][0] == True:
                REF_LABEL = _radar_parameters['field_label_trans'][1]
                VEL_LABEL = _radar_parameters['field_label_trans'][2]
                volume = pyart.io.read_cfradial(fname, field_names={REF_LABEL:"reflectivity", VEL_LABEL:"velocity"})
            else:
                volume = pyart.io.read_cfradial(fname)
        else:
            try:
                volume = pyart.io.read_nexrad_archive(fname, field_names=None, 
                                                        additional_metadata=None, file_field_names=False, 
                                                        delay_field_loading=False, 
                                                        station=None, scans=None, linear_interp=True)
            except:
                print('\n File {} cannot be read, skipping...\n'.format(fname))
                continue

        opaws2D_io_cpu = timeit.time() - tim0

        print("\n Time for reading in LVL2: {} seconds".format(opaws2D_io_cpu))
        
        processVolume(volume, unfold_type, options, cLatLon, sweep_num, out_filenames[n], fname)

    opaws2D_cpu_time = timeit.time() - t0

    print("\n Time for opaws2D operations: {} seconds".format(opaws2D_cpu_time))

    print("\n PROGRAM opaws2D COMPLETED\n")

if __name__ == "__main__":
   parser = OptionParser()
   parser.add_option("-d", "--dir",       dest="dname",     default=None,  type="string", \
           help = "Directory of files to process")
               
   parser.add_option("-o", "--out",       dest="out_dir",     default="opaws_files",  type="string", \
           help = "Directory to place output files in")
                     
   parser.add_option("-f", "--file",      dest="fname",     default=None,  type="string", \
           help = "filename of NEXRAD level II volume or cfradial file to process")

   parser.add_option(      "--window",    dest="window",    type="string", default=None,  \
                                    help = "Time of window location in YYYY,MM,DD,HH,MM")

   parser.add_option("-u", "--unfold",    dest="unfold",    default="region",  type="string", \
           help = "dealiasing method to use (phase or region, default = region)")
                     
   parser.add_option("-w", "--write",     dest="write",   default=False, \
           help = "Boolean flag to write DART ascii file", action="store_true")
                     
   parser.add_option(      "--onlyVR",     dest="onlyVR",   default=False, \
           help = "Boolean flag to only write VR to DART ascii file", action="store_true")
                     
   parser.add_option(     "--method",     dest="method",   default=None, type="string", \
           help = "Function to use for the weight process, valid strings are:  Cressman or Barnes")
          
   parser.add_option("-q", "--qc", dest="qc", default="Minimal",  type="string",     \
           help = "Type of QC corrections on reflectivity or velocity.  Valid:  None, Minimal, MetSignal, A1")  

   parser.add_option(     "--dx",     dest="dx",   default=None, type="float", \
           help = "Analysis grid spacing in meters for superob resolution")
          
   parser.add_option(     "--roi",     dest="roi",   default=None, type="float", \
           help = "Radius of influence in meters for superob regrid")

   parser.add_option("-p", "--plot",      dest="plot",      default=0,  type="int",      \
           help = "Specify a number between 0 and # elevations to plot ref and vr in that co-plane")
                     
   parser.add_option("-i", "--interactive", dest="interactive", default=False,  action="store_true",     \
           help = "Boolean flag to specify to plot image to screen (when plot > -1).")  
                     
   parser.add_option("-s", "--shapefiles", dest="shapefiles", default=None, type="string",    \
           help = "Name of system env shapefile you want to add to the plots.")

   parser.add_option(      "--newse",    dest="newse",    type="string", default=None, \
           help = "NEWSe radars description file to parse for model grid lat and lon" )

   (options, args) = parser.parse_args()
   run(options)