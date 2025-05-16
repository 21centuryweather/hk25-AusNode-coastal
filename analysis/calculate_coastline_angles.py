import xarray as xr
import numpy as np
import healpy as hp
from functools import partial
import easygems.healpix as egh
from dask.distributed import Client
import os
import sys

if __name__=="__main__":

    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])    

    #Load functions from the sea_breeze_code repo
    user_id = os.environ['USER']
    #from distributed.diagnostics.plugin import UploadDirectory
    #client.register_plugin(UploadDirectory(
    #    f'/home/548/{user_id}/working/hk25-AusNode-coastal/sea_breeze_code',)
    #        )    
    sys.path.append(f'/home/548/{user_id}/working/hk25-AusNode-coastal/')
    from sea_breeze_code.utils import get_nn_lon_lat_index
    from sea_breeze_code.load_model_data import get_coastline_angle_kernel

    # Load the model data
    #ds = xr.open_zarr("/g/data/qx55/germany_node/d3hp003.zarr/PT1H_point_z10_atm.zarr/")
    #ds = xr.open_zarr("/g/data/qx55/uk_node/glm.n2560_RAL3p3/data.healpix.PT1H.z10.zarr/")
    ds = xr.open_zarr("/g/data/nf33/hk25_AusNode_coastal/sftlf_um_z8.zarr/")

    #Get the "nside", which is used for regridding
    nside = hp.get_nside(ds.sftlf)

    #Set up target latitude and longitude (regular grid) to interpolate onto
    ncells = ds.cell.shape[0]
    nlats = int(np.round(np.sqrt(ncells/2)))
    nlons = int(np.round(2*np.sqrt(ncells/2)))
    target_lats = np.linspace(
        -90,
        90,
        nlats
    )
    target_lons = np.linspace(
        0,
        360,
        nlons,
    )
    #TODO: Compare these lat lons with assigning lat lons with the easygems function
    #ds = egh.attach_coords(ds)

    #Do the interpolation and return the healpix cell indices
    target_inds = get_nn_lon_lat_index(nside, target_lons, target_lats)

    #Define the regridded land sea mask
    lsm = ((ds.sftlf.isel(cell=target_inds))>0.5)
    #lsm = lsm.sel(lon=slice(80,170),lat=slice(-25,25)).persist()

    #From the land sea mask, define dominant coastline angles through the whole domain.
    #R specifies the smoothing of nearby points, in the inverse distance weighting function (see get_weights())
    angle_ds = get_coastline_angle_kernel(lsm,R=100,latlon_chunk_size=10)      

    #Code from Zhe Feng || zhe.feng@pnnl.gov
    #https://github.com/digital-earths-global-hackathon/hk25-teams/blob/main/hk25-MCS/remap_mcsmask_to_healpix_fullglobe.ipynb

    #To re-map to healpix format
    ds = ds.pipe(partial(egh.attach_coords))
    lon_hp = ds.lon.assign_coords(cell=ds.cell, lon_hp=lambda da: da)
    lat_hp = ds.lat.assign_coords(cell=ds.cell, lat_hp=lambda da: da)

    angle_ds_hp = angle_ds.sel(lon=lon_hp, lat=lat_hp, method="nearest")
    angle_ds_hp = angle_ds_hp.drop_vars(["lat_hp", "lon_hp"]) 

    #Save the healpix angles to disk
    #angle_ds_hp.to_zarr("/g/data/nf33/hk25_AusNode_coastal/icon_z10.zarr")     
    angle_ds_hp.to_zarr("/g/data/nf33/hk25_AusNode_coastal/um_angles_z8.zarr")     