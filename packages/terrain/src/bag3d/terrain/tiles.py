"""Offline Mapterhorn covering, reprojection, terrain encoding and overviews."""

from contextlib import ExitStack
from io import BytesIO
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import imagecodecs
import mercantile
import numpy as np
from PIL import Image
from pmtiles.reader import MmapSource, Reader, all_tiles
from pmtiles.tile import Compression, TileType, tileid_to_zxy, zxy_to_tileid
from pmtiles.writer import Writer
import rasterio
from rasterio.windows import Window

from ._mapterhorn import covering
from ._mapterhorn.codec import get_rounded_elevation_data
from ._mapterhorn.merge import merge
from .processing import finish, parallel_map, stage_directory
from .state import atomic_json, checkpoint, read_manifest

ATTRIBUTION = "AHN.nl (CC BY 4.0) | Copernicus DEM © DLR e.V. 2010–2014, © Airbus Defence and Space GmbH 2014–2018, provided under COPERNICUS by the European Union and ESA; accessed via AWS Open Data"


def intersects(a, b):
    return a[2] > b[0] and a[0] < b[2] and a[3] > b[1] and a[1] < b[3]


def encode(data: np.ndarray, zoom: int) -> bytes:
    values = get_rounded_elevation_data(np.asarray(data, dtype=np.float64), zoom)
    packed = np.rint((values + 32768) * 256)
    if not np.isfinite(packed).all() or packed.min() < 0 or packed.max() > 16777215:
        raise ValueError("Terrain elevations cannot be encoded as Terrarium")
    packed = packed.astype(np.uint32)
    rgb = np.stack(
        ((packed >> 16) & 255, (packed >> 8) & 255, packed & 255), axis=-1
    ).astype(np.uint8)
    return bytes(imagecodecs.webp_encode(rgb, lossless=True))


def decode(data: bytes) -> np.ndarray:
    rgb = np.asarray(Image.open(BytesIO(data)).convert("RGB"), dtype=np.float32)
    return rgb[..., 0] * 256 + rgb[..., 1] + rgb[..., 2] / 256 - 32768


def write_archive(path: Path, tiles, bounds=None) -> None:
    """Use PMTiles' supported Writer API, avoiding the missing finalize_header API."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp.pmtiles")
    try:
        with temporary.open("wb") as stream:
            writer = Writer(stream)
            minimum = 32
            maximum = 0
            coverage = None
            previous_id = -1
            for tile_id, data in tiles:
                if tile_id <= previous_id:
                    raise ValueError(
                        "Archive tiles must have unique, ascending PMTiles IDs"
                    )
                previous_id = tile_id
                z, x, y = tileid_to_zxy(tile_id)
                current = mercantile.bounds(x, y, z)
                coverage = (
                    list(current)
                    if coverage is None
                    else [
                        min(coverage[0], current.west),
                        min(coverage[1], current.south),
                        max(coverage[2], current.east),
                        max(coverage[3], current.north),
                    ]
                )
                minimum, maximum = min(minimum, z), max(maximum, z)
                writer.write_tile(tile_id, data)
            if coverage is None:
                raise ValueError(f"No tiles for archive: {path}")
            w, s, e, n = bounds or coverage
            writer.finalize(
                {
                    "tile_type": TileType.WEBP,
                    "tile_compression": Compression.NONE,
                    "min_zoom": minimum,
                    "max_zoom": maximum,
                    "min_lon_e7": int(w * 1e7),
                    "min_lat_e7": int(s * 1e7),
                    "max_lon_e7": int(e * 1e7),
                    "max_lat_e7": int(n * 1e7),
                    "center_zoom": minimum,
                    "center_lon_e7": int((w + e) * 5e6),
                    "center_lat_e7": int((s + n) * 5e6),
                },
                {"encoding": "terrarium", "tileSize": 512, "attribution": ATTRIBUTION},
            )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def make_plan(root: Path, ahn_path: Path, glo_path: Path) -> Path:
    directory, (ahn, glo) = stage_directory(
        root, "aggregation_plan", [ahn_path, glo_path]
    )
    bounds = ahn["bounds"]
    if any(m["height_reference"] != "ellipsoidal" for m in (ahn, glo)):
        raise ValueError("Both terrain sources must have ellipsoidal heights")
    west, south = mercantile.xy(bounds[0], bounds[1])
    east, north = mercantile.xy(bounds[2], bounds[3])
    mercator_bounds = (west, south, east, north)
    resolutions = covering.get_mercator_resolutions(0, 32)
    macrotiles = {}
    records = {}
    for name, manifest in [("nl5mwaterellipsoid", ahn), ("glo30nlellipsoid", glo)]:
        for record in manifest["files"]:
            records[name, record["name"]] = record
            left, bottom, right, top = record["bounds_3857"]
            zoom = max(
                12,
                covering.get_smallest_overzoom(
                    left,
                    bottom,
                    right,
                    top,
                    record["width"],
                    record["height"],
                    resolutions,
                ),
            )
            if name == "nl5mwaterellipsoid" and zoom > 14:
                raise ValueError("Expected AHN 5 m input with maximum zoom 14")
            buffered = left - 300, bottom - 300, right + 300, top + 300
            for tile in covering.get_intersecting_tiles_dfs(
                buffered, mercantile.Tile(0, 0, 0), 12
            ):
                if intersects(mercantile.xy_bounds(tile), mercator_bounds):
                    macrotiles.setdefault((tile.x, tile.y), {"sources": {}})[
                        "sources"
                    ].setdefault(name, []).append(
                        {"filename": record["name"], "maxzoom": zoom}
                    )
    covering.add_group_ids(macrotiles)
    items = []
    for extent in sorted(covering.get_aggregation_tiles(macrotiles)):
        sources = set()
        for tile in mercantile.children(extent, zoom=12):
            for source, values in (
                macrotiles.get((tile.x, tile.y), {}).get("sources", {}).items()
            ):
                sources.update((v["maxzoom"], source, v["filename"]) for v in values)
        groups = []
        for zoom, source in sorted(
            {(z, s) for z, s, _ in sources}, key=lambda v: (-v[0], v[1])
        ):
            groups.append(
                {
                    "source": source,
                    "maxzoom": zoom,
                    "files": [
                        records[source, filename]
                        for z, s, filename in sorted(sources)
                        if (z, s) == (zoom, source)
                    ],
                }
            )
        maximum = max(g["maxzoom"] for g in groups)
        items.append(
            {
                "id": f"{extent.z}-{extent.x}-{extent.y}-{maximum}",
                "extent": [extent.z, extent.x, extent.y],
                "maxzoom": maximum,
                "groups": groups,
            }
        )
    if not items:
        raise ValueError("No terrain work intersects the requested bounds")
    return finish(
        directory,
        ahn["files"] + glo["files"],
        bounds=bounds,
        mercator_bounds=mercator_bounds,
        items=items,
    )


def command(arguments: list[str]) -> str:
    env = {**os.environ, "PROJ_NETWORK": "OFF", "GDAL_NUM_THREADS": "1"}
    result = subprocess.run(
        arguments, capture_output=True, text=True, check=True, env=env
    )
    return result.stdout


def gdal_executable(name: str, bin_dir: str | None) -> str:
    executable = str(Path(bin_dir) / name) if bin_dir else shutil.which(name)
    if not executable or not Path(executable).is_file():
        raise ValueError(
            f"Missing {name}; install GDAL with COG/LERC or configure terrain.gdal_bin_dir"
        )
    return executable


def aggregate_one(task):
    item, directory, output_bounds, gdal_bin_dir = task
    target = directory / f"{item['id']}.pmtiles"

    def produce():
        z, x, y = item["extent"]
        extent = mercantile.Tile(x, y, z)
        maximum = item["maxzoom"]
        resolution = 40075016.68557849 / (512 * 2**maximum)
        buffer_pixels = int(150 / resolution) if len(item["groups"]) > 1 else 0
        buffer = buffer_pixels * resolution
        left, bottom, right, top = mercantile.xy_bounds(extent)
        with tempfile.TemporaryDirectory(
            prefix=f"{item['id']}-", dir=directory
        ) as work_name:
            work = Path(work_name)
            rasters = []
            for i, group in enumerate(item["groups"]):
                file_list = work / f"{i}.txt"
                file_list.write_text(
                    "\n".join(r["path"] for r in group["files"]) + "\n"
                )
                vrt = work / f"{i}.vrt"
                command(
                    [
                        gdal_executable("gdalbuildvrt", gdal_bin_dir),
                        "-overwrite",
                        "-input_file_list",
                        str(file_list),
                        str(vrt),
                    ]
                )
                destination = work / f"{i}.tif"
                warped = work / f"{i}-3857.vrt"
                command(
                    [
                        gdal_executable("gdalwarp", gdal_bin_dir),
                        "--config",
                        "GDAL_CACHEMAX",
                        "128",
                        "-of",
                        "VRT",
                        "-overwrite",
                        "-t_srs",
                        "EPSG:3857",
                        "-tr",
                        str(resolution),
                        str(resolution),
                        "-te",
                        str(left - buffer),
                        str(bottom - buffer),
                        str(right + buffer),
                        str(top + buffer),
                        "-r",
                        "cubicspline",
                        "-dstnodata",
                        "-9999",
                        str(vrt),
                        str(warped),
                    ]
                )
                # Match Mapterhorn's VRT -> tiled COG evaluation, including its
                # warp approximation. Direct COG warping changes some rounded
                # pixels at source transitions by one Terrarium quantization step.
                command(
                    [
                        gdal_executable("gdal_translate", gdal_bin_dir),
                        "--config",
                        "GDAL_CACHEMAX",
                        "64",
                        "--config",
                        "GDAL_MAX_DATASET_POOL_SIZE",
                        "1",
                        "-of",
                        "COG",
                        "-co",
                        "BIGTIFF=IF_NEEDED",
                        "-co",
                        "SPARSE_OK=YES",
                        "-co",
                        "BLOCKSIZE=512",
                        "-co",
                        "COMPRESS=LERC",
                        "-co",
                        "MAX_Z_ERROR=0.001",
                        "-co",
                        "OVERVIEWS=NONE",
                        "-co",
                        "NUM_THREADS=1",
                        str(warped),
                        str(destination),
                    ]
                )
                rasters.append(destination)
            merged = work / "merged.tif"
            merge(rasters, merged, buffer_pixels)
            tile_paths = []
            fallback_pixels = 0
            with rasterio.open(merged) as raster:
                count = 2 ** (maximum - z)
                if (
                    raster.width != count * 512 + 2 * buffer_pixels
                    or raster.height != raster.width
                ):
                    raise ValueError("Unexpected aggregation grid alignment")
                for xx in range(count):
                    for yy in range(count):
                        tile = mercantile.Tile(x * count + xx, y * count + yy, maximum)
                        if not intersects(mercantile.bounds(tile), output_bounds):
                            continue
                        col_off = xx * 512 + buffer_pixels
                        row_off = yy * 512 + buffer_pixels
                        # Rasterio's generated attrs constructor lacks a typed signature.
                        window = Window(col_off, row_off, 512, 512)  # pyright: ignore[reportCallIssue]
                        values = raster.read(indexes=1, window=window)
                        invalid = ~np.isfinite(values) | (values == -9999)
                        # Reject holes inside the domain instead of encoding ellipsoid zero.
                        w, s = mercantile.xy(output_bounds[0], output_bounds[1])
                        e, n = mercantile.xy(output_bounds[2], output_bounds[3])
                        transform = raster.window_transform(window)
                        xs = transform.c + (np.arange(512) + 0.5) * transform.a
                        ys = transform.f + (np.arange(512) + 0.5) * transform.e
                        domain = (
                            (ys[:, None] >= s)
                            & (ys[:, None] <= n)
                            & (xs[None, :] >= w)
                            & (xs[None, :] <= e)
                        )
                        if np.any(invalid & domain):
                            raise ValueError(
                                f"Unresolved terrain nodata inside output bounds: {tile}"
                            )
                        values[invalid] = 0
                        path = work / f"{maximum}-{tile.x}-{tile.y}.webp"
                        path.write_bytes(encode(values, maximum))
                        tile_paths.append(
                            (zxy_to_tileid(*[maximum, tile.x, tile.y]), path)
                        )
                        if len(rasters) > 1:
                            with rasterio.open(rasters[0]) as ahn_raster:
                                ahn_values = ahn_raster.read(1, window=window)
                                fallback_pixels += int(
                                    np.count_nonzero((ahn_values == -9999) & domain)
                                )
            write_archive(
                target,
                ((tile_id, path.read_bytes()) for tile_id, path in sorted(tile_paths)),
                output_bounds,
            )
            atomic_json(
                target.with_suffix(".coverage.json"),
                {"fallback_pixels": fallback_pixels, "tiles": len(tile_paths)},
            )
        return [target, target.with_suffix(".coverage.json")]

    return checkpoint(
        target.with_suffix(".json"), {"item": item, "bounds": output_bounds}, produce
    )


def aggregate(
    root: Path, plan_path: Path, workers: int, gdal_bin_dir: str | None = None
) -> Path:
    version = command([gdal_executable("gdalwarp", gdal_bin_dir), "--version"]).strip()
    directory, (plan,) = stage_directory(
        root, "terrain_tiles", [plan_path], {"gdal_cli": version}
    )
    groups = parallel_map(
        aggregate_one,
        [(item, directory, plan["bounds"], gdal_bin_dir) for item in plan["items"]],
        workers,
    )
    records = [r for group in groups for r in group if r["path"].endswith(".pmtiles")]
    coverage = [read_manifest(Path(group[1]["path"])) for group in groups]
    return finish(
        directory,
        records,
        bounds=plan["bounds"],
        fallback_pixels=sum(c["fallback_pixels"] for c in coverage),
        detailed_tiles=sum(c["tiles"] for c in coverage),
    )


def tile_index(files: list[dict]) -> dict:
    index = {}
    for record in files:
        with Path(record["path"]).open("rb") as stream:
            for coordinates, _ in all_tiles(MmapSource(stream)):
                if coordinates in index:
                    raise ValueError(f"Duplicate terrain tile: {coordinates}")
                index[coordinates] = record["path"]
    return index


def overview_one(task):
    zoom, coordinates, index, target = task

    def produce():
        def tiles():
            for tile in sorted(coordinates, key=lambda t: zxy_to_tileid(t.z, t.x, t.y)):
                values = np.zeros((1024, 1024), dtype=np.float32)
                for dy in range(2):
                    for dx in range(2):
                        child = (zoom + 1, tile.x * 2 + dx, tile.y * 2 + dy)
                        if child in index:
                            with Path(index[child]).open("rb") as stream:
                                data = Reader(MmapSource(stream)).get(*child)
                            if data is None:
                                raise ValueError(f"Missing indexed tile {child}")
                            values[
                                dy * 512 : (dy + 1) * 512, dx * 512 : (dx + 1) * 512
                            ] = decode(data)
                parent = values.reshape((512, 2, 512, 2)).mean(axis=(1, 3))
                yield zxy_to_tileid(zoom, tile.x, tile.y), encode(parent, zoom)

        write_archive(target, tiles())
        return [target]

    return checkpoint(
        target.with_suffix(".json"),
        {
            "zoom": zoom,
            "coordinates": [list(t) for t in coordinates],
            "inputs": sorted((list(k), v) for k, v in index.items()),
        },
        produce,
    )[0]


def overviews(root: Path, tiles_path: Path, workers: int) -> Path:
    directory, (manifest,) = stage_directory(root, "terrain_overviews", [tiles_path])
    files = list(manifest["files"])
    index = tile_index(files)
    maximum = max(z for z, _, _ in index)
    for zoom in reversed(range(maximum)):
        parents: set[mercantile.Tile] = set()
        for z, x, y in index:
            if z == zoom + 1:
                parent = mercantile.parent(mercantile.Tile(x, y, z))
                if parent is None:
                    raise ValueError("A terrain overview requires a parent tile")
                parents.add(parent)
        parents = {p for p in parents if (p.z, p.x, p.y) not in index}
        # Preserve Mapterhorn's bounded work areas: at most 64x64 tiles.
        groups = {}
        for parent in parents:
            area = mercantile.parent(parent, zoom=max(0, zoom - 6)) if zoom else parent
            groups.setdefault(area, []).append(parent)
        tasks = []
        for area, members in sorted(groups.items()):
            subset = {}
            for p in members:
                for child in mercantile.children(p):
                    key = child.z, child.x, child.y
                    if key in index:
                        subset[key] = index[key]
            tasks.append(
                (
                    zoom,
                    sorted(members),
                    subset,
                    directory / f"{area.z}-{area.x}-{area.y}-{zoom}.pmtiles",
                )
            )
        produced = parallel_map(overview_one, tasks, workers)
        files.extend(produced)
        index.update(tile_index(produced))
    return finish(
        directory, files, bounds=manifest["bounds"], minzoom=0, maxzoom=maximum
    )


def bundle(root: Path, overviews_path: Path) -> Path:
    directory, (manifest,) = stage_directory(root, "terrain_bundles", [overviews_path])
    index = tile_index(manifest["files"])
    groups = {}
    for z, x, y in index:
        parent = mercantile.parent(mercantile.Tile(x, y, z), zoom=6) if z > 12 else None
        name = f"{parent.z}-{parent.x}-{parent.y}" if parent else "planet"
        groups.setdefault(name, []).append((z, x, y))
    records = []
    for name, members in sorted(groups.items()):
        target = directory / f"{name}.pmtiles"

        def produce():
            def tiles():
                with ExitStack() as stack:
                    readers = {}
                    for coordinates in sorted(members, key=lambda c: zxy_to_tileid(*c)):
                        source = index[coordinates]
                        if source not in readers:
                            stream = stack.enter_context(Path(source).open("rb"))
                            readers[source] = Reader(MmapSource(stream))
                        yield (
                            zxy_to_tileid(*coordinates),
                            readers[source].get(*coordinates),
                        )

            write_archive(target, tiles(), manifest["bounds"])
            return [target]

        records.extend(
            checkpoint(
                target.with_suffix(".json"),
                {
                    "members": sorted(members),
                    "inputs": [f["sha256"] for f in manifest["files"]],
                },
                produce,
            )
        )
    return finish(
        directory,
        records,
        bounds=manifest["bounds"],
        minzoom=manifest["minzoom"],
        maxzoom=manifest["maxzoom"],
    )
