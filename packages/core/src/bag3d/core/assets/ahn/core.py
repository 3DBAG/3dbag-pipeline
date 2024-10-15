from pathlib import Path
from typing import Tuple
from math import ceil

from dagster import StaticPartitionsDefinition

from bag3d.core.assets.ahn import TILES


class PartitionDefinitionAHN(StaticPartitionsDefinition):
    def __init__(self):
        super().__init__(partition_keys=sorted(list(TILES)))


def format_laz_log(fpath: Path, msg: str) -> str:
    """Formats a message as <file path>.....<msg>"""
    return f"{fpath.stem}{'.' * 5}{msg}"


def ahn_filename(tile_id: str) -> str:
    """Creates an AHN LAZ file name from an AHN tile ID."""
    return f"C_{tile_id.upper()}.LAZ"


def ahn_dir(root_dir: Path, ahn_version: int) -> Path:
    """Create a directory path where to store the AHN LAZ files for the given AHN
    version."""
    return Path(root_dir) / "pointcloud" / f"AHN{ahn_version}"


def ahn_laz_dir(root_dir: Path, ahn_version: int) -> Path:
    """Create a directory path where to store the AHN LAZ files for the given AHN
    version."""
    return ahn_dir(root_dir, ahn_version) / "as_downloaded" / "LAZ"


def generate_grid(bbox: Tuple[float, float, float, float], cellsize: int):
    """Generates a grid of fixed cell-size for a BBOX.
    The origin of the grid is the BBOX min coordinates.

    Args:
        bbox: (minx, miny, maxx, maxy)
        cellsize: Cell size.

    Returns:
        The bbox of the generated grid, nr. of cells in X-direction,
        nr. of cells in Y-direction.
    """
    origin = bbox[:2]
    nr_cells_x = ceil((bbox[2] - bbox[0]) / cellsize)
    nr_cells_y = ceil((bbox[3] - bbox[1]) / cellsize)
    bbox_new = (
        *origin,
        origin[0] + nr_cells_x * cellsize,
        origin[1] + nr_cells_y * cellsize,
    )
    return bbox_new, nr_cells_x, nr_cells_y
