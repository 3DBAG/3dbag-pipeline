"""Working with file inputs and outputs"""
import os
from typing import Tuple, Sequence, Iterator
import csv
from pathlib import Path
from zipfile import ZipFile

from dagster import get_dagster_logger

from bag3d.common.types import ExportResult


class BadArchiveError(OSError):
    """The archive contains a bad file"""
    pass


def unzip(file: Path, dest: Path) -> None:
    """Uncompress the whole zip archive and delete the zip.

    Args:
        file: The Path to the zip.
        dest: The Path to the destination directory.

    Raises:
        BadArchiveError: The archive contains at least one bad file
    """
    logger = get_dagster_logger()
    logger.info(f"Uncompressing {file} to {dest}")
    with ZipFile(file, 'r') as ezip:
        first_bad_file = ezip.testzip()
        if first_bad_file:
            raise BadArchiveError(
                f"The archive contains at least one bad file: {first_bad_file}")
        ezip.extractall(path=dest)
    logger.info(f"Deleting {file}")
    file.unlink()


def bag3d_dir(root_dir: os.PathLike) -> Path:
    """The 3D BAG data directory"""
    return Path(root_dir) / "3DBAG"


def geoflow_crop_dir(root_dir: os.PathLike) -> Path:
    """Directory for the Geoflow crop-reconstruct output"""
    return bag3d_dir(root_dir) / "crop_reconstruct"


def bag3d_export_dir(root_dir: os.PathLike) -> Path:
    """Create the 3DBAG export directory if does not exist"""
    export_dir = bag3d_dir(root_dir) / "export"
    export_dir.mkdir(exist_ok=True)
    return export_dir


def check_export_results(path_quadtree_tsv: Path, path_tiles_dir: Path) -> Iterator[
    ExportResult]:
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
                leaf_id_in_filename = leaf_id.replace("/", "-")
                leaf_dir = path_tiles_dir.joinpath(leaf_id)
                if leaf_dir.exists():
                    obj_paths = tuple(p for p in leaf_dir.iterdir()
                                      if p.suffix == ".obj")
                    basename = path_tiles_dir.joinpath(leaf_id, leaf_id_in_filename)
                    yield ExportResult(
                        tile_id=leaf_id,
                        cityjson_path=basename.with_suffix(".city.json"),
                        gpkg_path=basename.with_suffix(".gpkg"),
                        obj_paths=obj_paths,
                        wkt=row["wkt"]
                    )


def get_export_tile_ids() -> Sequence[str]:
    """Get the IDs of the distribution tiles from the file system.
    It reads the `quadtree.tsv` output from *tyler* and extracts the IDs of the
    leaf tiles.

    Fixme:
        * path is hardcoded for gilfoyle

    Returns:
        List of tile IDs
    """
    tileids = []
    if Path("/data").exists():
        HARDCODED_PATH_GILFOYLE = "/data"
        output_dir = bag3d_export_dir(HARDCODED_PATH_GILFOYLE)
        path_tiles_dir = output_dir.joinpath("tiles")
        path_quadtree_tsv = output_dir.joinpath("quadtree.tsv")
        if path_quadtree_tsv.exists():
            tileids = [er.tile_id for er in
                       check_export_results(path_quadtree_tsv, path_tiles_dir)]
    return tileids
