"""Working with file inputs and outputs"""

import csv
from collections.abc import Iterator
from pathlib import Path
from zipfile import ZipFile

from dagster import get_dagster_logger

from bag3d.common.resources.files import DEFAULT_TILES_DIR_NAME
from bag3d.common.types import ExportResult


class BadArchiveError(OSError):
    """The archive contains a bad file"""


def tile_basename(tiles_dir: Path, tile_id: str) -> Path:
    """Return the Tyler basename, for example `0/0/0-0-0` for tile `0/0/0`."""
    level, x, y = tile_id.split("/")
    return tiles_dir / level / x / f"{level}-{x}-{y}"


def tile_sidecars(basename: Path, *suffixes: str) -> tuple[Path, ...]:
    """Find the files with this tile basename, including LoD-specific files."""
    prefixes = (f"{basename.name}.", f"{basename.name}-")
    return tuple(
        path
        for path in basename.parent.glob(f"{basename.name}*")
        if path.name.startswith(prefixes) and path.suffix in suffixes and path.is_file()
    )


def export_tile_path(
    export_dir: Path,
    tile_id: str,
    suffix: str,
    *,
    tiles_dir_name: str = DEFAULT_TILES_DIR_NAME,
) -> Path:
    """Return the path of a Tyler tile output."""
    return tile_basename(export_dir / tiles_dir_name, tile_id).with_suffix(suffix)


def unzip(file: Path, dest: Path, remove: bool = True) -> None:
    """Uncompress the whole zip archive and optionally delete the zip.

    Args:
        file: The Path to the zip.
        dest: The Path to the destination directory.
        remove: Whether to remove the zip.

    Raises:
        BadArchiveError: The archive contains at least one bad file
    """
    logger = get_dagster_logger()
    logger.info(f"Uncompressing {file} to {dest}")
    with ZipFile(file, "r") as ezip:
        first_bad_file = ezip.testzip()
        if first_bad_file:
            raise BadArchiveError(
                f"The archive contains at least one bad file: {first_bad_file}"
            )
        ezip.extractall(path=dest)
    if remove:
        logger.info(f"Deleting {file}")
        file.unlink()


def check_export_results(
    path_quadtree_tsv: Path,
    export_dir: Path,
    *,
    tiles_dir_name: str = DEFAULT_TILES_DIR_NAME,
) -> Iterator[ExportResult]:
    """Parse the `quadtree.tsv` written by *tyler*, check if all formats exists for each
    tile, add the tile WKT.

    Returns:
         Generator of ExportResult
    """
    with path_quadtree_tsv.open("r") as fo:
        csvreader = csv.DictReader(fo, delimiter="\t")
        for row in csvreader:
            if row["leaf"] == "true" and int(row["nr_items"]) > 0:
                leaf_id = row["id"]
                basename = export_tile_path(
                    export_dir, leaf_id, "", tiles_dir_name=tiles_dir_name
                )
                cityjson_path = basename.with_suffix(".city.json")
                gpkg_path = basename.with_suffix(".gpkg")
                ifc_path = basename.with_name(f"{basename.name}-ifc.zip")
                obj_paths = tile_sidecars(basename, ".obj")
                if obj_paths or any(
                    path.is_file() for path in (cityjson_path, gpkg_path, ifc_path)
                ):
                    yield ExportResult(
                        tile_id=leaf_id,
                        cityjson_path=cityjson_path,
                        gpkg_path=gpkg_path,
                        ifc_path=ifc_path,
                        obj_paths=obj_paths,
                        wkt=row["wkt"],
                    )
