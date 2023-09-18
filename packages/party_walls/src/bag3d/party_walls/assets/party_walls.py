from pathlib import Path
from dataclasses import dataclass

from dagster import asset, Config
from shapely import STRtree, from_wkt
import numpy as np
from numpy.typing import NDArray
from pandas import DataFrame
from urban_morphology_3d.cityStats import city_stats

from bag3d.common.utils.dagster import PartitionDefinition3DBagDistribution
from bag3d.common.utils.files import check_export_results
from bag3d.common.types import ExportResult


class TileExportConfig(Config):
    """Location of the multi-tiles export output of *tyler*"""
    tiles_dir_path: Path
    quadtree_tsv_path: Path


@dataclass
class TilesFilesIndex:
    """A collection type, storing the ExportResults per tile, the R-Tree of the tiles
    and a path-array of the CityJSON files."""
    export_results: dict[str, ExportResult]
    tree: STRtree
    paths_array: NDArray


@asset
def distribution_tiles_files_index(context, config: TileExportConfig) -> TilesFilesIndex:
    """An index of the distribution tiles and the CityJSON file paths for each tile,
    that has an existing CityJSON file.
    The index is a [Shapely RTree index](https://shapely.readthedocs.io/en/stable/strtree.html).

    Returns:
        A collection type, storing the ExportResults per tile, the R-Tree of the tiles
    and a path-array of the CityJSON files (TilesFilesIndex)
    """
    export_results_gen = filter(
        lambda t: t.has_cityjson,
        check_export_results(
            path_quadtree_tsv=config.quadtree_tsv_path,
            path_tiles_dir=config.tiles_dir_path
        )
    )
    export_results = dict((t.tile_id, t) for t in export_results_gen)
    tree = STRtree(tuple(from_wkt(t.wkt) for t in export_results.values()))
    paths_array = np.array(t.cityjson_path for t in export_results.values())
    return TilesFilesIndex(
        export_results=export_results,
        tree=tree,
        paths_array=paths_array
    )


@asset(
    partitions_def=PartitionDefinition3DBagDistribution(),
)
def party_walls_nl(context, distribution_tiles_files_index: TilesFilesIndex) -> DataFrame:
    """Party walls calculation from the exported CityJSON tiles."""
    tile_id = context.asset_partition_key_for_output()
    export_result = distribution_tiles_files_index.export_results[tile_id]

    tile_shapley_poly = from_wkt(export_result.wkt)
    paths_neighbours = []
    for nbr_tile_idx in distribution_tiles_files_index.tree.query(tile_shapley_poly):
        neighbour_path = distribution_tiles_files_index.paths_array.take(nbr_tile_idx)
        if neighbour_path != export_result.cityjson_path:
            paths_neighbours.append(neighbour_path)

    df = city_stats(
        inputs=[export_result.cityjson_path, ] + paths_neighbours,
        dsn="\"dbname=baseregisters\"",
        break_on_error=True
    )
    return df
