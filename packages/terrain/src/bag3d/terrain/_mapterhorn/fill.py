# Derived from Mapterhorn, Copyright (c) 2025, mapterhorn. BSD-3-Clause.
# See LICENSE and the package MAPTERHORN_PROVENANCE.json.
import math
import os
from contextlib import ExitStack

import numpy as np
import rasterio
from rasterio.fill import fillnodata
from rasterio.merge import merge


def fill_file(task):
    source, output, neighbors, distance, fingerprint = task
    tags = dict(
        HEIGHT_REFERENCE="NAP",
        FILL_RADIUS_M=str(distance),
        FILL_INPUT_SHA256=fingerprint,
    )
    if output.exists():
        with rasterio.open(output) as existing:
            if all(existing.tags().get(k) == v for k, v in tags.items()):
                return 0
        raise ValueError(f"Existing output has different fill settings: {output}")
    temporary = output.with_name(f".{output.stem}.{os.getpid()}.tmp.tif")
    try:
        with rasterio.Env(GDAL_CACHEMAX=64 * 1024 * 1024), ExitStack() as stack:
            original = stack.enter_context(rasterio.open(source))
            if (
                original.crs.to_epsg() != 28992
                or original.transform.b
                or original.transform.d
            ):
                raise ValueError(
                    "This meter-based fill requires north-up RD New AHN rasters"
                )
            rx, ry = original.res
            padding = math.ceil(distance / min(rx, ry)) + 2
            bbox = (
                original.bounds.left - padding * rx,
                original.bounds.bottom - padding * ry,
                original.bounds.right + padding * rx,
                original.bounds.top + padding * ry,
            )
            datasets = [stack.enter_context(rasterio.open(path)) for path in neighbors]
            mosaic, affine = merge(
                datasets,
                bounds=bbox,
                res=(rx, ry),
                indexes=[1],
                nodata=-9999,
                dtype="float32",
                method="last",
            )
            data = mosaic[0]
            valid = np.isfinite(data) & (data != -9999)
            data[~valid] = -9999
            if valid.any() and not valid.all():
                data = fillnodata(
                    data,
                    mask=valid.astype("uint8"),
                    max_search_distance=math.ceil(distance / min(rx, ry)),
                )
            window = rasterio.windows.from_bounds(*original.bounds, transform=affine)
            row, col = round(window.row_off), round(window.col_off)
            filled = data[
                row : row + original.height, col : col + original.width
            ].copy()
            raw = original.read(1, masked=True)
            raw_valid = ~np.ma.getmaskarray(raw) & np.isfinite(raw.data)
            # Filling never changes a valid measurement in the original raster.
            filled[raw_valid] = raw.data[raw_valid]
            filled[~np.isfinite(filled)] = -9999
            missing = int(np.count_nonzero(filled == -9999))
            profile = dict(
                driver="COG",
                width=original.width,
                height=original.height,
                count=1,
                dtype="float32",
                crs=original.crs,
                transform=original.transform,
                nodata=-9999,
                BLOCKSIZE=512,
                OVERVIEWS="NONE",
                BIGTIFF="IF_NEEDED",
                COMPRESS="LERC",
                MAX_Z_ERROR=0.001,
                NUM_THREADS=1,
            )
            with rasterio.open(temporary, "w", **profile) as dst:
                dst.write(filled, 1)
                dst.update_tags(**tags, REMAINING_NODATA_PIXELS=str(missing))
        temporary.replace(output)
        return missing
    finally:
        temporary.unlink(missing_ok=True)
