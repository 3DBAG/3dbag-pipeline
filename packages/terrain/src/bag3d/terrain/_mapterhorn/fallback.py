# Derived from Mapterhorn, Copyright (c) 2025, mapterhorn. BSD-3-Clause.
# See LICENSE and the package MAPTERHORN_PROVENANCE.json.
import os

import numpy as np
from pyproj import Transformer, network
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window

GRID = "us_nga_egm08_25.tif"

TRANSFORMER = None


def init_worker(grid):
    global TRANSFORMER
    network.set_network_enabled(False)
    TRANSFORMER = Transformer.from_pipeline(
        "+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad "
        f'+step +proj=vgridshift +grids="{grid}" +multiplier=1 '
        "+step +proj=unitconvert +xy_in=rad +xy_out=deg"
    )


def convert_cell(task):
    lat, lon, source, output, grid_hash = task
    tags = dict(
        HEIGHT_REFERENCE="ellipsoidal",
        HEIGHT_CRS="EPSG:4979",
        INPUT_HEIGHT_REFERENCE="EGM2008 (EPSG:3855)",
        HEIGHT_CONVERSION="egm2008-ellipsoid-v1",
        HEIGHT_GRID_SHA256=grid_hash,
        SOURCE="GLO-30" if source else "Copernicus ocean convention",
    )
    if source:
        stat = source.stat()
        tags.update(
            HEIGHT_INPUT_SIZE=str(stat.st_size),
            HEIGHT_INPUT_MTIME_NS=str(stat.st_mtime_ns),
        )
    if output.exists():
        with rasterio.open(output) as existing:
            if all(existing.tags().get(k) == v for k, v in tags.items()):
                return output.name
        raise ValueError(f"Fallback output signature changed: {output}")
    src = rasterio.open(source) if source else None
    # AWS GLO-30 uses 1.5 arcsecond longitude / 1 arcsecond latitude in NL.
    # Ocean-only cells are omitted by the publisher and have EGM2008 height zero.
    width, height = (src.width, src.height) if src else (2400, 3600)
    transform = (
        src.transform
        if src
        else from_origin(lon - 1 / 4800, lat + 1 + 1 / 7200, 1 / 2400, 1 / 3600)
    )
    profile = dict(
        driver="COG",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999,
        BLOCKSIZE=512,
        OVERVIEWS="NONE",
        COMPRESS="LERC",
        MAX_Z_ERROR=0.001,
        NUM_THREADS=1,
        BIGTIFF="IF_NEEDED",
    )
    temporary = output.with_name(f".{output.stem}.{os.getpid()}.tmp.tif")
    try:
        with (
            rasterio.Env(GDAL_CACHEMAX=64 * 1024 * 1024),
            rasterio.open(temporary, "w", **profile) as dst,
        ):
            for row in range(0, height, 512):
                for col in range(0, width, 512):
                    window = Window(
                        col, row, min(512, width - col), min(512, height - row)
                    )
                    values = (
                        src.read(1, window=window, masked=True)
                        if src
                        else np.zeros(
                            (int(window.height), int(window.width)), dtype="float32"
                        )
                    )
                    if (
                        np.ma.getmaskarray(values).any()
                        or not np.isfinite(values).all()
                    ):
                        raise ValueError(
                            f"Unexpected internal Copernicus void: {source}"
                        )
                    rr, cc = np.indices(values.shape, dtype="float64")
                    cc += col + 0.5
                    rr += row + 0.5
                    xs = transform.a * cc + transform.b * rr + transform.c
                    ys = transform.d * cc + transform.e * rr + transform.f
                    _, _, hs = TRANSFORMER.transform(
                        xs.ravel(),
                        ys.ravel(),
                        np.asarray(values).ravel().astype("float64"),
                        errcheck=True,
                    )
                    if not np.isfinite(hs).all():
                        raise ValueError("Nonfinite EGM2008 correction")
                    dst.write(
                        np.asarray(hs, dtype="float32").reshape(values.shape),
                        1,
                        window=window,
                    )
            dst.update_tags(**tags)
        temporary.replace(output)
    finally:
        if src:
            src.close()
        temporary.unlink(missing_ok=True)
    return output.name
