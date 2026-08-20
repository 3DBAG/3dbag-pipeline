"""Working with file inputs and outputs"""

import csv
import os
from collections.abc import Iterator, Sequence
from pathlib import Path
from zipfile import ZipFile

from dagster import get_dagster_logger

from bag3d.common.resources import DagsterDeployment, FileStoreResource
from bag3d.common.types import ExportResult


class BadArchiveError(OSError):
    """The archive contains a bad file"""


def export_tile_path(export_dir: Path, tile_id: str, suffix: str) -> Path:
    """Return the path of a Tyler tile output."""
    return export_dir.joinpath("t", tile_id).with_suffix(suffix)


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
    path_quadtree_tsv: Path, export_dir: Path
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
                basename = export_tile_path(export_dir, leaf_id, "")
                obj_paths = tuple(basename.parent.glob(f"{basename.name}*.obj"))
                expected_paths = (
                    basename.with_suffix(".city.json"),
                    basename.with_suffix(".gpkg"),
                    *obj_paths,
                )
                if any(path.exists() for path in expected_paths):
                    yield ExportResult(
                        tile_id=leaf_id,
                        cityjson_path=basename.with_suffix(".city.json"),
                        gpkg_path=basename.with_suffix(".gpkg"),
                        obj_paths=obj_paths,
                        wkt=row["wkt"],
                    )


def get_export_tile_ids() -> Sequence[str]:
    """Get the IDs of the distribution tiles from the file system.
    It reads the `quadtree.tsv` output from *tyler* and extracts the IDs of the
    leaf tiles.

    Returns:
        List of tile IDs
    """
    tileids = []

    deployment = os.getenv("DAGSTER_DEPLOYMENT", "default")
    version = os.getenv("BAG3D_RELEASE_VERSION", "test_version")

    # Only pytest runs use the integration_party_walls/file_store subdirectory
    if deployment.lower() == DagsterDeployment.PYTEST:
        root_dir = (
            Path(os.getenv("BAG3D_FILESTORE", "/data/volume"))
            / "integration_party_walls/file_store"
        )
    else:
        root_dir = Path(os.getenv("BAG3D_FILESTORE", "/data"))

    file_resource = FileStoreResource(root_dir=str(root_dir))
    export_dir = file_resource.stage_dir("export") / version

    path_quadtree_tsv = export_dir.joinpath("debug", "quadtree.tsv")
    if path_quadtree_tsv.exists():
        tileids = [
            er.tile_id for er in check_export_results(path_quadtree_tsv, export_dir)
        ]
    else:
        raise FileNotFoundError(f"File not found: {path_quadtree_tsv}")

    return tileids
