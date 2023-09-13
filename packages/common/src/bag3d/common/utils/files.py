"""Working with files"""
from typing import Tuple, Sequence, Iterator
import csv
from pathlib import Path
from zipfile import ZipFile

from dagster import get_dagster_logger


class BadArchiveError(OSError):
    pass


def unzip(file: Path, dest: Path):
    """Uncompress the whole zip archive and delete the zip.

    Args:
        file: The Path to the zip.
        dest: The Path to the destination directory.

    Returns:
        None
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


def bag3d_dir(root_dir: Path):
    return Path(root_dir) / "3DBAG"


def geoflow_crop_dir(root_dir):
    return bag3d_dir(root_dir) / "crop_reconstruct"


def bag3d_export_dir(root_dir):
    """Create the 3DBAG export directory"""
    export_dir = bag3d_dir(root_dir) / "export"
    export_dir.mkdir(exist_ok=True)
    return export_dir


def check_export_results(path_quadtree_tsv: Path, path_tiles_dir: Path) -> Iterator[
    Tuple[str, bool, bool, bool, str]]:
    """Parse the quadtree.tsv written by tyler, check if all formats exists for each
    tile, add the tile WKT.

    Returns:
         Generator of leaf_id, has_cityjson, has_all_gpkg, has_all_obj, wkt
    """
    with path_quadtree_tsv.open("r") as fo:
        csvreader = csv.reader(fo, delimiter="\t")
        # skip header, which is [id, level, nr_items, leaf, wkt]
        next(csvreader)
        for row in csvreader:
            if row[3] == "true" and int(row[2]) > 0:
                leaf_id = row[0]
                leaf_id_in_filename = leaf_id.replace("/", "-")
                has_cityjson = path_tiles_dir.joinpath(leaf_id,
                                                       f"{leaf_id_in_filename}.city.json").exists()
                gpkg_cnt = sum(1 for f in path_tiles_dir.joinpath(leaf_id).iterdir()
                               if f.suffix == ".gpkg")
                has_all_gpkg = gpkg_cnt == 1
                obj_cnt = sum(1 for f in path_tiles_dir.joinpath(leaf_id).iterdir()
                              if f.suffix == ".obj")
                has_all_obj = obj_cnt == 6
                yield leaf_id, has_cityjson, has_all_gpkg, has_all_obj, row[4]


def get_export_tile_ids() -> Sequence[str]:
    """Get the IDs of the distribution tiles from the file system."""
    # FIXME: hardcoded for gilfoyle
    tileids = []
    if Path("/data").exists():
        HARDCODED_PATH_GILFOYLE = "/data"
        output_dir = bag3d_export_dir(HARDCODED_PATH_GILFOYLE)
        path_tiles_dir = output_dir.joinpath("tiles")
        path_quadtree_tsv = output_dir.joinpath("quadtree.tsv")
        if path_quadtree_tsv.exists():
            tileids = [row[0] for row in
                       check_export_results(path_quadtree_tsv, path_tiles_dir)]
    return tileids
