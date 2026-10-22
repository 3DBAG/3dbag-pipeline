"""Offline AHN preparation and height conversion using Mapterhorn algorithms."""

import math
import multiprocessing
import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import mercantile
import rasterio
from rasterio.warp import transform_bounds

from ._mapterhorn import fallback, fill, heights
from ._mapterhorn.covering import bounds_intersect, get_intersecting_tiles_dfs
from .state import (
    checkpoint,
    code_version,
    digest_file,
    fingerprint,
    read_manifest,
    save_manifest,
    verify_inputs,
)


def parallel_map(function, tasks, workers, initializer=None, initargs=()):
    """Bounded process pools; a one-worker run is easy to debug and test."""
    if workers == 1:
        if initializer:
            initializer(*initargs)
        return [function(task) for task in tasks]
    with ProcessPoolExecutor(
        workers,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=initializer,
        initargs=initargs,
    ) as executor:
        return list(executor.map(function, tasks, chunksize=1))


def stage_directory(
    root: Path, name: str, manifests: list[Path], settings=None
) -> tuple[Path, list[dict]]:
    loaded = [read_manifest(path) for path in manifests]
    for manifest in loaded:
        verify_inputs(manifest)
    # Worker counts and output URLs intentionally do not affect pixel identity.
    signature = {
        "stage": name,
        "inputs": [digest_file(p) for p in manifests],
        "settings": settings,
        "code": code_version(),
        "rasterio": rasterio.__version__,
        "gdal": rasterio.__gdal_version__,
    }
    directory = root / "builds" / fingerprint(signature) / name
    directory.mkdir(parents=True, exist_ok=True)
    return directory, loaded


def raster_record(record: dict) -> dict:
    with rasterio.Env(PROJ_NETWORK="OFF"), rasterio.open(record["path"]) as raster:
        return {
            **record,
            "name": Path(record["path"]).name,
            "bounds_3857": list(transform_bounds(raster.crs, 3857, *raster.bounds)),
            "width": raster.width,
            "height": raster.height,
            "height_reference": raster.tags().get("HEIGHT_REFERENCE"),
        }


def finish(directory: Path, records: list[dict], **metadata) -> Path:
    return save_manifest(directory / "manifest.json", records, **metadata)


def validate_selection(manifest_path: Path, bounds, radius: float) -> None:
    manifest = read_manifest(manifest_path)
    if "requested_bounds" not in manifest:
        return  # Locally supplied fixtures have no acquisition selection.
    if manifest["requested_bounds"] != bounds or (
        bounds is not None and radius > manifest["selection_radius_m"]
    ):
        raise ValueError(
            "Rematerialize terrain_download with the current bounds and radius before processing"
        )


def normalize_file(task):
    source, output = task

    def produce():
        temporary = output.with_suffix(".tmp.tif")
        try:
            with rasterio.open(source["path"]) as raster:
                reference = raster.tags().get("HEIGHT_REFERENCE", "nap").lower()
                if raster.crs.to_epsg() not in (28992, 7415) or reference != "nap":
                    raise ValueError(
                        "AHN normalization requires original RD/NAP rasters"
                    )
                if (
                    any(abs(v - 5) > 0.01 for v in raster.res)
                    or raster.transform.b
                    or raster.transform.d
                ):
                    raise ValueError("AHN normalization requires north-up 5 m rasters")
                import numpy as np

                profile: dict[str, Any] = {
                    "driver": "COG",
                    "width": raster.width,
                    "height": raster.height,
                    "count": 1,
                    "dtype": "float32",
                    "crs": "EPSG:28992",
                    "transform": raster.transform,
                    "nodata": -9999,
                    "BLOCKSIZE": 512,
                    "OVERVIEWS": "NONE",
                    "COMPRESS": "LERC",
                    "MAX_Z_ERROR": 0.001,
                    "NUM_THREADS": 1,
                    "BIGTIFF": "IF_NEEDED",
                }
                # Only horizontal metadata is assigned; valid NAP values are unchanged.
                with rasterio.open(temporary, "w", **profile) as destination:
                    for _, window in raster.block_windows(1):
                        values = (
                            raster.read(1, window=window, masked=True)
                            .astype("float32")
                            .filled(-9999)
                        )
                        values[~np.isfinite(values)] = -9999
                        destination.write(values, 1, window=window)
                    destination.update_tags(HEIGHT_REFERENCE="NAP")
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok=True)
        return [output]

    records = checkpoint(output.with_suffix(".json"), source["sha256"], produce)
    return raster_record(records[0])


def normalize_ahn(root: Path, manifest_path: Path, workers: int) -> Path:
    directory, (manifest,) = stage_directory(root, "ahn_rasters", [manifest_path])
    tasks = []
    for record in manifest["files"]:
        name = re.sub(
            r"[^a-zA-Z0-9_.-]",
            "_",
            Path(urlsplit(record.get("url", record["path"])).path).stem,
        )
        tasks.append(
            (
                record,
                directory
                / f"{name}-{fingerprint(record.get('url', record['path']))[:12]}.tif",
            )
        )
    files = parallel_map(normalize_file, tasks, workers)
    acquisition = {
        key: manifest[key]
        for key in ("requested_bounds", "selection_radius_m")
        if key in manifest
    }
    return finish(
        directory,
        files,
        bounds=manifest["bounds"],
        height_reference="NAP",
        **acquisition,
    )


def fill_one(task):
    source, output, neighbors, radius, signature = task

    def produce():
        # Imported functions have their own legacy signature check. A missing or
        # corrupt checkpoint means its owned output must be rebuilt as well.
        output.unlink(missing_ok=True)
        fill.fill_file((source, output, neighbors, radius, signature))
        return [output]

    records = checkpoint(output.with_suffix(".json"), signature, produce)
    return raster_record(records[0])


def fill_ahn(root: Path, manifest_path: Path, radius: float, workers: int) -> Path:
    directory, (manifest,) = stage_directory(
        root, "ahn_filled", [manifest_path], {"radius_m": radius}
    )
    index = {}
    entries = []
    for record in manifest["files"]:
        bounds = record["bounds_3857"]
        # Conservative Mercator lookup; actual padding/fill uses RD ground meters.
        buffer = (radius + 10) / math.cos(math.atan(math.sinh(bounds[3] / 6378137)))
        expanded = (
            bounds[0] - buffer,
            bounds[1] - buffer,
            bounds[2] + buffer,
            bounds[3] + buffer,
        )
        tiles = get_intersecting_tiles_dfs(expanded, mercantile.Tile(0, 0, 0), 10)
        item = len(entries)
        entries.append((record, expanded, tiles))
        for tile in tiles:
            index.setdefault(tile, set()).add(item)
    signature = fingerprint(
        {"files": [r["sha256"] for r in manifest["files"]], "radius": radius}
    )
    tasks = []
    for record, expanded, tiles in entries:
        candidates = set().union(*(index[tile] for tile in tiles))
        neighbors = sorted(
            Path(entries[i][0]["path"])
            for i in candidates
            if bounds_intersect(expanded, entries[i][1])
        )
        source = Path(record["path"])
        tasks.append((source, directory / source.name, neighbors, radius, signature))
    files = parallel_map(fill_one, tasks, workers)
    remaining = 0
    for record in files:
        with rasterio.open(record["path"]) as raster:
            remaining += int(raster.tags()["REMAINING_NODATA_PIXELS"])
    return finish(
        directory,
        files,
        bounds=manifest["bounds"],
        height_reference="NAP",
        remaining_nodata_pixels=remaining,
    )


def prepare_local_grids(directory: Path, manifest: dict) -> tuple[Path, dict]:
    grids = directory / "grids"
    grids.mkdir(exist_ok=True)
    hashes = {}
    for record in manifest["files"]:
        target = grids / record["name"]
        if not target.exists():
            target.symlink_to(Path(record["path"]).resolve())
        if digest_file(target) != record["sha256"]:
            raise ValueError(f"Grid mismatch: {target}")
        hashes[record["name"]] = record["sha256"]
    return grids, hashes


def convert_ahn_one(task):
    record, output, hashes = task

    def produce():
        heights.convert_file((Path(record["path"]), output, hashes, True))
        return [output]

    records = checkpoint(
        output.with_suffix(".json"),
        {"source": record["sha256"], "grids": hashes},
        produce,
    )
    return raster_record(records[0])


def convert_ahn(
    root: Path, manifest_path: Path, grids_path: Path, workers: int
) -> Path:
    directory, (manifest, grids_manifest) = stage_directory(
        root, "ahn_ellipsoid", [manifest_path, grids_path]
    )
    grids, hashes = prepare_local_grids(directory, grids_manifest)
    for name in heights.GRIDS:
        if name not in hashes:
            raise ValueError(f"Missing mandatory grid: {name}")
    heights.make_transformer(grids.resolve())
    tasks = [
        (r, directory / Path(r["path"]).name, {n: hashes[n] for n in heights.GRIDS})
        for r in manifest["files"]
    ]
    files = parallel_map(
        convert_ahn_one, tasks, workers, heights.init_worker, (str(grids.resolve()),)
    )
    return finish(
        directory, files, bounds=manifest["bounds"], height_reference="ellipsoidal"
    )


def convert_glo_one(task):
    lat, lon, source, output, grid_hash = task

    def produce():
        output.unlink(missing_ok=True)
        fallback.convert_cell(task)
        return [output]

    records = checkpoint(
        output.with_suffix(".json"),
        {
            "lat": lat,
            "lon": lon,
            "source": digest_file(source) if source else "ocean",
            "grid": grid_hash,
        },
        produce,
    )
    return raster_record(records[0])


def convert_glo(
    root: Path, manifest_path: Path, grids_path: Path, workers: int
) -> Path:
    directory, (manifest, grids_manifest) = stage_directory(
        root, "glo30_ellipsoid", [manifest_path, grids_path]
    )
    grids, hashes = prepare_local_grids(directory, grids_manifest)
    if fallback.GRID not in hashes:
        raise ValueError(f"Missing mandatory grid: {fallback.GRID}")
    paths = {r["url"]: Path(r["path"]) for r in manifest["files"]}
    tasks = [
        (
            c["lat"],
            c["lon"],
            paths[c["url"]] if c["url"] else None,
            directory / f"GLO30_N{c['lat']:02d}_E{c['lon']:03d}.tif",
            hashes[fallback.GRID],
        )
        for c in manifest["cells"]
    ]
    files = parallel_map(
        convert_glo_one,
        tasks,
        workers,
        fallback.init_worker,
        (str((grids / fallback.GRID).resolve()),),
    )
    return finish(
        directory,
        files,
        bounds=manifest["bounds"],
        height_reference="ellipsoidal",
        ocean_cells=sum(c["url"] is None for c in manifest["cells"]),
    )
