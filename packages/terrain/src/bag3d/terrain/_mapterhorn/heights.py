# Derived from Mapterhorn, Copyright (c) 2025, mapterhorn. BSD-3-Clause.
# See LICENSE and the package MAPTERHORN_PROVENANCE.json.
import json
import os
from pathlib import Path

import numpy as np
import rasterio
from pyproj import Transformer, network
from rasterio.windows import Window

GRIDS = ("nl_nsgi_rdtrans2018.tif", "nl_nsgi_nlgeo2018.tif")

VERSION = "rdnaptrans2018-ellipsoid-v1"

TRANSFORMER = None


def make_transformer(folder):
    # Explicit EPSG operation: inverse RD New + Amersfoort to ETRS89 (9)
    # + inverse ETRS89 to NAP height (2). EPSG's ETRS89 -> WGS84 (1)
    # is an ensemble identity, so the ellipsoidal height is unchanged.
    # Mandatory grid paths (no @ / null fallback) prevent silent NAP output.
    network.set_network_enabled(False)
    return Transformer.from_pipeline(
        "+proj=pipeline "
        "+step +inv +proj=sterea +lat_0=52.1561605555556 "
        "+lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel "
        f'+step +proj=hgridshift +grids="{folder / GRIDS[0]}" '
        f'+step +proj=vgridshift +grids="{folder / GRIDS[1]}" +multiplier=1 '
        "+step +proj=unitconvert +xy_in=rad +xy_out=deg"
    )


def init_worker(folder):
    global TRANSFORMER
    TRANSFORMER = make_transformer(Path(folder))


def convert_file(task):
    source, output, grid_hashes, overwrite = task
    stat = source.stat()
    tags = {
        "HEIGHT_REFERENCE": "ellipsoidal",
        "HEIGHT_CRS": "EPSG:4979",
        "HEIGHT_REALIZATION": "ETRS89 (WGS84 ensemble approximation)",
        "INPUT_HEIGHT_REFERENCE": "NAP (EPSG:5709)",
        "HEIGHT_CONVERSION": VERSION,
        "HEIGHT_GRID_SHA256": json.dumps(grid_hashes, sort_keys=True),
        "HEIGHT_INPUT": str(source.resolve()),
        "HEIGHT_INPUT_SIZE": str(stat.st_size),
        "HEIGHT_INPUT_MTIME_NS": str(stat.st_mtime_ns),
    }
    with rasterio.open(source) as src:
        if src.crs is None or src.crs.to_epsg() not in (28992, 7415):
            raise ValueError(f"Expected RD New / RD+NAP input: {source}: {src.crs}")
        reference = src.tags().get("HEIGHT_REFERENCE", "").lower()
        if reference and reference != "nap":
            raise ValueError(
                f"Input already has another height reference: {source}: {reference}"
            )
        if output.exists() and not overwrite:
            with rasterio.open(output) as existing:
                if (
                    all(existing.tags().get(k) == v for k, v in tags.items())
                    and existing.transform == src.transform
                    and existing.shape == src.shape
                ):
                    return f"Already converted: {output.name}"
            raise ValueError(
                f"Output does not match this input/conversion: {output}; use --overwrite"
            )
        profile = {
            "driver": "COG",
            "width": src.width,
            "height": src.height,
            "count": 1,
            "dtype": "float32",
            "crs": "EPSG:28992",
            "transform": src.transform,
            "nodata": -9999,
            "BLOCKSIZE": 512,
            "OVERVIEWS": "NONE",
            "BIGTIFF": "IF_NEEDED",
            "COMPRESS": "LERC",
            "MAX_Z_ERROR": 0.001,
            "NUM_THREADS": 1,
        }
        temporary = output.with_name(f".{output.stem}.{os.getpid()}.tmp.tif")
        try:
            with (
                rasterio.Env(GDAL_CACHEMAX=64 * 1024 * 1024),
                rasterio.open(temporary, "w", **profile) as dst,
            ):
                # Arbitrary source block layouts still use bounded memory.
                for row in range(0, src.height, 512):
                    for col in range(0, src.width, 512):
                        window = Window(
                            col,
                            row,
                            min(512, src.width - col),
                            min(512, src.height - row),
                        )
                        data = src.read(1, window=window, masked=True)
                        valid = ~np.ma.getmaskarray(data) & np.isfinite(data.data)
                        heights = np.full(data.shape, -9999, dtype=np.float32)
                        if valid.any():
                            rows, cols = np.nonzero(valid)
                            rows = rows + row + 0.5
                            cols = cols + col + 0.5
                            affine = src.transform
                            xs = affine.a * cols + affine.b * rows + affine.c
                            ys = affine.d * cols + affine.e * rows + affine.f
                            _, _, zs = TRANSFORMER.transform(
                                xs,
                                ys,
                                data.data[valid].astype(np.float64),
                                errcheck=True,
                            )
                            if not np.isfinite(zs).all():
                                raise ValueError(
                                    f"Nonfinite height transformation in {source}"
                                )
                            heights[valid] = zs
                        dst.write(heights, 1, window=window)
                dst.update_tags(**tags)
                dst.set_band_description(1, "Ellipsoidal terrain elevation (m)")
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok=True)
    return f"Converted: {output.name}"
