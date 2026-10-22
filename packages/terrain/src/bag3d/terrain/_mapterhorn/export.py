# Derived from Mapterhorn, Copyright (c) 2025, mapterhorn. BSD-3-Clause.
# See LICENSE and the package MAPTERHORN_PROVENANCE.json.
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import fcntl
import gzip
import json
import mmap
import os

import mercantile
import numpy as np
from PIL import Image
from pmtiles.reader import Reader, all_tiles
from pmtiles.tile import Compression, TileType


def atomic_write(path, data):
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as dst:
            dst.write(data)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def merge_bounds(existing, bounds):
    if existing is None:
        return list(bounds)
    return [
        min(existing[0], bounds[0]),
        min(existing[1], bounds[1]),
        max(existing[2], bounds[2]),
        max(existing[3], bounds[3]),
    ]


def overzoom_parent(task):
    parent_path, children, extension, encoding = task
    with Image.open(parent_path) as image:
        rgb = np.asarray(image.convert("RGB"), dtype="float32")
    if encoding == "terrarium":
        heights = rgb[..., 0] * 256 + rgb[..., 1] + rgb[..., 2] / 256 - 32768
    else:
        heights = (rgb[..., 0] * 65536 + rgb[..., 1] * 256 + rgb[..., 2]) * 0.1 - 10000
    height_image = Image.fromarray(heights)
    for z, x, y, target in children:
        left, top = (x % 2) * 256, (y % 2) * 256
        # Interpolate elevations, never encoded RGB channels (which have carries).
        values = np.asarray(
            height_image.resize(
                (512, 512),
                Image.Resampling.BILINEAR,
                box=(left, top, left + 256, top + 256),
            ),
            dtype="float64",
        )
        packed = np.rint(
            (values + 32768) * 256 if encoding == "terrarium" else (values + 10000) * 10
        )
        if not np.isfinite(packed).all() or packed.min() < 0 or packed.max() > 16777215:
            raise ValueError(f"Cannot encode overzoomed elevations in {target}")
        packed = packed.astype("uint32")
        output = np.stack(
            ((packed >> 16) & 255, (packed >> 8) & 255, packed & 255), axis=-1
        ).astype("uint8")
        buffer = BytesIO()
        Image.fromarray(output).save(
            buffer, format="WEBP" if extension == "webp" else "PNG", lossless=True
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(target, buffer.getvalue())
    return [(z, x, y) for z, x, y, target in children]


def complete_xyz_pyramid(
    output_dir, seen, bounds, minzoom, maxzoom, extension, encoding, workers=4
):
    """Materialize coarse-only areas at every zoom for a static MapLibre source."""
    added = written = 0

    for zoom in range(minzoom + 1, maxzoom + 1):
        by_parent = {}
        resumed = 0
        for tile in mercantile.tiles(*bounds, zooms=zoom):
            coordinates = (tile.z, tile.x, tile.y)
            if coordinates in seen:
                continue
            target = output_dir / str(zoom) / str(tile.x) / f"{tile.y}.{extension}"
            if target.exists():
                seen.add(coordinates)
                resumed += 1
                continue
            parent = mercantile.parent(tile)
            if (parent.z, parent.x, parent.y) not in seen:
                raise ValueError(f"Missing parent for static tile {coordinates}")
            parent_path = (
                output_dir / str(parent.z) / str(parent.x) / f"{parent.y}.{extension}"
            )
            by_parent.setdefault(parent_path, []).append((*coordinates, target))
        jobs = [
            (parent, children, extension, encoding)
            for parent, children in by_parent.items()
        ]
        if jobs:
            print(
                f"Completing static zoom {zoom}: {sum(len(children) for children in by_parent.values()):,} overzoomed tiles",
                flush=True,
            )
            with ThreadPoolExecutor(workers) as executor:
                for coordinates in executor.map(overzoom_parent, jobs):
                    seen.update(coordinates)
                    added += len(coordinates)
                    written += len(coordinates)
        added += resumed
    return added, written


def export(
    input_dir, output_dir, url, name, attribution_override, clip_bounds=None, workers=4
):
    archives = sorted(input_dir.glob("*.pmtiles"))
    if not archives:
        raise ValueError(f"No bundled PMTiles archives found in {input_dir}")
    signatures = [
        dict(
            path=str(path.resolve()),
            size=path.stat().st_size,
            mtime_ns=path.stat().st_mtime_ns,
        )
        for path in archives
    ]

    # Validate every archive before writing output, including older bundles whose
    # metadata predates the encoding/tileSize fields in our bundler.
    extensions = {TileType.WEBP: "webp", TileType.PNG: "png"}
    extension = encoding = tile_size = None
    attributions = []
    for path in archives:
        with (
            path.open("rb") as src,
            mmap.mmap(src.fileno(), 0, access=mmap.ACCESS_READ) as mapping,
        ):
            reader = Reader(lambda offset, length: mapping[offset : offset + length])
            header, metadata = reader.header(), reader.metadata()
            current_extension = extensions.get(header["tile_type"])
            current_encoding = metadata.get("encoding", "terrarium")
            current_tile_size = metadata.get("tileSize", 512)
            if current_extension is None or header["tile_compression"] not in (
                Compression.NONE,
                Compression.GZIP,
            ):
                raise ValueError(f"Unsupported raster type/compression in {path}")
            if (
                current_encoding not in ("terrarium", "mapbox")
                or current_tile_size != 512
            ):
                raise ValueError(f"Expected a 512 px terrain raster in {path}")
            if extension is not None and (extension, encoding, tile_size) != (
                current_extension,
                current_encoding,
                current_tile_size,
            ):
                raise ValueError(
                    "All archives must use the same raster format, encoding and tile size"
                )
            extension, encoding, tile_size = (
                current_extension,
                current_encoding,
                current_tile_size,
            )
            attribution = metadata.get("attribution", "")
            if attribution and attribution not in attributions:
                attributions.append(attribution)

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / ".tile-export.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = output_dir / ".tile-export.json"
        fingerprint = dict(version=1, archives=signatures)
        if clip_bounds is not None:
            fingerprint["bounds"] = clip_bounds
            fingerprint["complete_static_pyramid"] = 1
        if state_path.exists():
            if json.loads(state_path.read_text()) != fingerprint:
                raise ValueError(
                    "Input bundles changed; use a fresh --output directory to avoid stale XYZ tiles"
                )
        else:
            if (output_dir / "tilejson.json").exists() or any(
                path.is_dir() and path.name.isdigit() for path in output_dir.iterdir()
            ):
                raise ValueError(
                    "Output already contains another tileset; use a fresh --output directory"
                )
            atomic_write(
                state_path, (json.dumps(fingerprint, indent=2) + "\n").encode()
            )

        seen = set()
        bounds_by_zoom = {}
        count = written = 0
        for path in archives:
            print(f"Exporting {path.name}...", flush=True)
            with (
                path.open("rb") as src,
                mmap.mmap(src.fileno(), 0, access=mmap.ACCESS_READ) as mapping,
            ):

                def get_bytes(offset, length):
                    return mapping[offset : offset + length]

                compression = Reader(get_bytes).header()["tile_compression"]
                for (z, x, y), data in all_tiles(get_bytes):
                    tile_bounds = mercantile.bounds(x, y, z)
                    if clip_bounds is not None and (
                        tile_bounds.east <= clip_bounds[0]
                        or tile_bounds.west >= clip_bounds[2]
                        or tile_bounds.north <= clip_bounds[1]
                        or tile_bounds.south >= clip_bounds[3]
                    ):
                        continue
                    coordinates = (z, x, y)
                    if coordinates in seen:
                        raise ValueError(
                            f"Overlapping bundle archives contain duplicate tile {z}/{x}/{y}"
                        )
                    seen.add(coordinates)
                    if compression == Compression.GZIP:
                        data = gzip.decompress(data)
                    directory = output_dir / str(z) / str(x)
                    directory.mkdir(parents=True, exist_ok=True)
                    target = directory / f"{y}.{extension}"
                    # Identical files from an interrupted/repeated export need no rewrite.
                    if not target.exists() or target.read_bytes() != data:
                        atomic_write(target, data)
                        written += 1
                    bounds = mercantile.bounds(x, y, z)
                    bounds_by_zoom[z] = merge_bounds(bounds_by_zoom.get(z), bounds)
                    count += 1
                    if count % 10000 == 0:
                        print(
                            f"{count:,} XYZ tiles exported ({written:,} written)",
                            flush=True,
                        )
        if not count:
            raise ValueError("The bundles contain no tiles")
        # Detect input changes during extraction before publishing TileJSON.
        for path, signature in zip(archives, signatures):
            stat = path.stat()
            if (
                stat.st_size != signature["size"]
                or stat.st_mtime_ns != signature["mtime_ns"]
            ):
                raise ValueError(
                    f"Input changed during export: {path}; use a fresh output directory"
                )

        minzoom, maxzoom = min(bounds_by_zoom), max(bounds_by_zoom)
        if clip_bounds is not None:
            overzoomed, synthesized = complete_xyz_pyramid(
                output_dir,
                seen,
                clip_bounds,
                minzoom,
                maxzoom,
                extension,
                encoding,
                workers,
            )
            count += overzoomed
            written += synthesized
            print(f"Coarse-only areas: {overzoomed:,} overzoomed XYZ tiles", flush=True)
        # Low zoom overview tiles can cover most of the world. Advertise the
        # union of detailed coverage, including differing local maximum zooms.
        detail_zoom = min(13, maxzoom)
        bounds = None
        for zoom, zoom_bounds in bounds_by_zoom.items():
            if zoom >= detail_zoom:
                bounds = merge_bounds(bounds, zoom_bounds)
        if clip_bounds is not None:
            bounds = list(clip_bounds)
        tilejson = dict(
            tilejson="3.0.0",
            name=name,
            scheme="xyz",
            encoding=encoding,
            tileSize=tile_size,
            minzoom=minzoom,
            maxzoom=maxzoom,
            bounds=bounds,
            center=[
                (bounds[0] + bounds[2]) / 2,
                (bounds[1] + bounds[3]) / 2,
                min(10, maxzoom),
            ],
            tiles=[f"{url}/{{z}}/{{x}}/{{y}}.{extension}"],
            attribution=attribution_override
            if attribution_override is not None
            else " | ".join(attributions),
        )
        atomic_write(
            output_dir / "tilejson.json",
            (json.dumps(tilejson, indent=2) + "\n").encode(),
        )
        print(
            f"Finished: {count:,} tiles, zoom {minzoom}–{maxzoom}, {written:,} files written",
            flush=True,
        )
        print(f"TileJSON: {url}/tilejson.json", flush=True)
        print(f"Serve directory: {output_dir.resolve()}", flush=True)
