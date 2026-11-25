"""Crop raw AHN from the live terrain acquisition into a portable snapshot."""

import fcntl
import json
import math
import tempfile
from pathlib import Path

from bag3d.common.terrain import (
    DEFAULT_RADIUS_M,
    contains,
    padded_bounds,
    sha256_file,
    validate_terrain_catalog,
)


def crop_terrain(
    manifest_path: Path,
    terrain_root: Path,
    target: Path,
    aoi_rd: tuple[float, float, float, float],
) -> Path:
    # Core needs rasterio only while generating the raster snapshot.
    import rasterio
    from rasterio.warp import transform_bounds
    from rasterio.windows import Window, from_bounds

    manifest_path = manifest_path.resolve()
    if not manifest_path.is_relative_to(terrain_root.resolve()):
        raise ValueError("Live AHN manifest must be inside terrain_store")
    manifest = json.loads(manifest_path.read_text())
    # Include the existing integration validation buffer for downstream sampling.
    west, south, east, north = aoi_rd
    bounds_rd = (west - 10, south - 10, east + 10, north + 10)
    bounds = list(transform_bounds(28992, 4326, *bounds_rd))
    selection = padded_bounds(bounds_rd, DEFAULT_RADIUS_M)
    available = manifest.get("requested_bounds") or manifest["bounds"]
    if not contains(available, bounds) or (
        manifest.get("requested_bounds") is not None
        and manifest.get("selection_radius_m", 0) < DEFAULT_RADIUS_M
    ):
        raise ValueError(
            "Live AHN acquisition does not cover the integration AOI and neighbors; "
            "materialize terrain_download with covering bounds and a 500 m radius first"
        )
    # A bounded acquisition selected complete tiles using its own padded extent.
    if manifest.get("requested_bounds") is not None:
        acquired = transform_bounds(4326, 28992, *available)
        acquired = padded_bounds(acquired, manifest["selection_radius_m"])
        if not contains(acquired, selection):
            raise ValueError(
                "Live AHN acquisition has insufficient interpolation neighbors"
            )
    if manifest.get("version") != 1 or not manifest.get("files"):
        raise ValueError("Expected a nonempty version-1 raw AHN acquisition manifest")

    target.parent.mkdir(parents=True, exist_ok=True)
    with (
        (target.parent / ".terrain.lock").open("w") as lock,
        tempfile.TemporaryDirectory(dir=target.parent) as temporary,
        rasterio.Env(PROJ_NETWORK="OFF", GDAL_TIFF_INTERNAL_MASK=True),
    ):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        working = Path(temporary) / "terrain"
        working.mkdir()
        files = []
        for record in manifest["files"]:
            source = Path(record["path"]).resolve()
            if not source.is_relative_to(terrain_root.resolve()):
                raise ValueError(f"AHN input is outside terrain_store: {source}")
            with rasterio.open(source) as raster:
                extent = raster.bounds
                if not (
                    extent.left < selection[2]
                    and extent.right > selection[0]
                    and extent.bottom < selection[3]
                    and extent.top > selection[1]
                ):
                    continue
                if (
                    not raster.crs
                    or raster.crs.to_epsg() not in (28992, 7415)
                    or raster.tags().get("HEIGHT_REFERENCE", "NAP").upper() != "NAP"
                    or raster.count != 1
                    or raster.transform.b
                    or raster.transform.d
                    or any(abs(v - 5) > 0.01 for v in raster.res)
                ):
                    raise ValueError(
                        f"Expected north-up raw 5 m RD/NAP raster: {source}"
                    )
                if (
                    source.stat().st_size != record["bytes"]
                    or sha256_file(source) != record["sha256"]
                ):
                    raise ValueError(f"Live AHN source is missing or changed: {source}")
                window = from_bounds(*selection, transform=raster.transform)
                col = max(0, math.floor(window.col_off))
                row = max(0, math.floor(window.row_off))
                right = min(raster.width, math.ceil(window.col_off + window.width))
                bottom = min(raster.height, math.ceil(window.row_off + window.height))
                window = Window.from_slices((row, bottom), (col, right))
                destination = working / f"{len(files):04d}-{record['sha256'][:16]}.tif"
                profile = raster.profile.copy()
                profile.update(
                    driver="GTiff",
                    width=right - col,
                    height=bottom - row,
                    transform=raster.window_transform(window),
                    compress="deflate",
                )
                with rasterio.open(destination, "w", **profile) as output:
                    output.write(raster.read(window=window))
                    output.write_mask(raster.dataset_mask(window=window))
                    output.update_tags(**{**raster.tags(), "HEIGHT_REFERENCE": "NAP"})
                files.append(
                    {
                        "path": f"terrain/{destination.name}",
                        "bytes": destination.stat().st_size,
                        "sha256": sha256_file(destination),
                        "url": record.get("url"),
                        "source_sha256": record["sha256"],
                        "bounds_rd": list(output.bounds),
                    }
                )
        catalog = {
            "version": 1,
            "height_reference": "NAP",
            "crs": "EPSG:28992",
            "resolution_m": 5,
            "bounds": bounds,
            "bounds_rd": list(bounds_rd),
            "selection_bounds_rd": list(selection),
            "selection_radius_m": DEFAULT_RADIUS_M,
            "source_manifest_sha256": sha256_file(manifest_path),
            "files": files,
        }
        validate_terrain_catalog(catalog)
        (working / "catalog.json").write_text(
            json.dumps(catalog, indent=2, sort_keys=True)
        )
        # Recheck selected live inputs before publishing a replacement subtree.
        selected_hashes = {record["source_sha256"] for record in files}
        for record in manifest["files"]:
            if (
                record["sha256"] in selected_hashes
                and sha256_file(Path(record["path"])) != record["sha256"]
            ):
                raise ValueError("Live terrain changed while generating the snapshot")
        backup = Path(temporary) / "previous"
        if target.exists():
            target.rename(backup)
        try:
            working.rename(target)
        except BaseException:
            if backup.exists():
                backup.rename(target)
            raise
    return target / "catalog.json"
