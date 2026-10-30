"""Small local rasters; tests never acquire national input data."""

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin


@pytest.fixture
def write_raster():
    def write(path: Path, values, crs=28992, transform=None, tags=None):
        data = np.asarray(values, dtype=np.float32)
        path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=data.shape[1],
            height=data.shape[0],
            count=1,
            dtype="float32",
            crs=crs,
            transform=transform or from_origin(85000, 448000, 5, 5),
            nodata=-9999,
        ) as raster:
            raster.write(data, 1)
            if tags:
                raster.update_tags(**tags)
        return path

    return write
