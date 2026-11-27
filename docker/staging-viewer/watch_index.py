"""Publish the staging viewer's RD New tile index when its export CSV changes."""

import csv
import os
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory

RELEASE = os.environ["RELEASE_VERSION"]
EXPORT = Path("/pipeline/stages/export") / RELEASE
INDEX = Path("/index") / f"{RELEASE}.fgb"
HASHES = ["cj_sha256", "gpkg_sha256", "obj_sha256", "ifc_sha256"]
OGR = "/opt/3dbag-pipeline/tools/bin/ogr2ogr"


def source_state(export):
    state = []
    for name in ("export_index.csv", "validate_compressed_files.csv"):
        try:
            stat = (export / name).stat()
            state.append((stat.st_ino, stat.st_size, stat.st_mtime_ns))
        except FileNotFoundError:
            state.append(None)
    return state


def build(export, dest):
    hashes = {}
    validation = export / "validate_compressed_files.csv"
    if validation.exists():
        with validation.open(newline="") as stream:
            for row in csv.DictReader(stream):
                hashes[row["tile_id"]] = {key: row[key] for key in HASHES}

    with TemporaryDirectory(dir=dest.parent) as tmp:
        source = Path(tmp) / "tiles.csv"
        with source.open("w", newline="") as out, (export / "export_index.csv").open(newline="") as stream:
            writer = csv.DictWriter(out, ["tile_id", "wkt", *HASHES])
            writer.writeheader()
            count = 0
            for row in csv.DictReader(stream):
                writer.writerow({"tile_id": row["tile_id"], "wkt": row["wkt"], **hashes.get(row["tile_id"], {})})
                count += 1
        if not count:
            raise ValueError("No tiles in export_index.csv")
        pending = Path(tmp) / dest.name
        subprocess.run([
            OGR, "-f", "FlatGeobuf", "-a_srs", "EPSG:28992", "-nlt", "POLYGON",
            "-oo", "GEOM_POSSIBLE_NAMES=wkt", "-oo", "KEEP_GEOM_COLUMNS=NO",
            "-lco", "SPATIAL_INDEX=YES", str(pending), str(source),
        ], check=True)
        pending.replace(dest)
        print(f"Published {count} tiles from {export}")


def watch():
    print(f"Waiting for {EXPORT}/export_index.csv")
    published = previous = None
    while True:
        current = source_state(EXPORT)
        if current[0] and current == previous and current != published:
            try:
                build(EXPORT, INDEX)
                published = current
            except (ValueError, OSError, csv.Error, KeyError, subprocess.CalledProcessError) as err:
                print(f"Index build will retry: {err}")
        elif not current[0] and INDEX.exists():
            INDEX.unlink()
            published = None
        previous = current
        time.sleep(30)


if __name__ == "__main__":
    watch()
