import unittest, types, sys, os, glob, time
import xarray as xr
import datetime as DT
import netCDF4 as ncdf

class TestOPAWSCombine(unittest.TestCase):
    def test_combine_outputs(self):

        search_pattern = "./out/20200302/obs_seq_K*.nc"
        output_dir = "./out"

        files = sorted(glob.glob(search_pattern), key = lambda file: os.path.getmtime(file))
        
        print("\n Obs_seq.final files sorted by modification time\n")
        for file in files:
            print(" {} - {}".format(file, time.ctime(os.path.getmtime(file))) )
        
        tmp         = os.path.basename(files[0])
        netcdf_file = os.path.join(output_dir, "%s%s" % (tmp[0:7], tmp[12:]))

        dataset = []
        nobs_total = 0

        begin_time = time.time()
            
        for file in files:
            try:
                infile = xr.open_dataset(file)
                nobs_total = nobs_total + len(infile.index)
                print("%s has %d observations, total is now %d" % (file, len(infile.index), nobs_total))
            except:
                continue

            if len(infile.index) > 0:
                dataset.append(infile)
            infile.close()
                
        end_time = time.time()

        print("\n Reading took {0} seconds since the loop started \n".format(end_time - begin_time))

        # Create an xarray dataset for file I/O
        xa = xr.concat(dataset, dim='index')

        # Write the xarray file out (this is all there is, very nice guys!)
        xa.to_netcdf(netcdf_file, mode='w')
        xa.close()

        #   Add attributes to the files
        fnc = ncdf.Dataset(netcdf_file, mode = 'a')
        fnc.history = "Created " + DT.datetime.today().strftime("%Y%m%d_%H:%M:%S")

        fnc.sync()  
        fnc.close()

if __name__ == '__main__':
    unittest.main()