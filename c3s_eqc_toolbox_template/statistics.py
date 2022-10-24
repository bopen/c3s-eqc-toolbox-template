from typing import Union

import numpy as np
import xarray as xr


def spatial_mean(
    ds: Union[xr.Dataset, xr.DataArray], lon: str = "longitude", lat: str = "latitude"
) -> Union[xr.Dataset, xr.DataArray]:
    """
    Calculate spatial mean of ds with latitude weighting.

    Parameters
    ----------
    ds: xr.Dataset or xr.DataArray
        Input data on which to apply the spatial mean
    lon: str, optional
        Name of longitude coordinate
    lat: str, optional
        Name of latitude coordinate

    Returns
    -------
    ds spatially mean
    """
    cos = np.cos(np.deg2rad(ds[lat]))
    weights = cos / (cos.sum(lat) * len(ds[lon]))
    return (ds * weights).sum(dim=[lon, lat])  # type: ignore
