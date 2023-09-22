from pathlib import Path
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable
import json

from dagster import asset, Config, MetadataValue
from shapely import STRtree, from_wkt
import numpy as np
from numpy.typing import NDArray
from pandas import DataFrame
from urban_morphology_3d.cityStats import city_stats

from bag3d.common.utils.dagster import PartitionDefinition3DBagDistribution
from bag3d.common.utils.files import check_export_results, geoflow_crop_dir, \
    bag3d_export_dir
from bag3d.common.types import ExportResult


class TileExportConfig(Config):
    """Location of the multi-tiles export output of *tyler*"""
    tiles_dir_path: str
    quadtree_tsv_path: str


@dataclass
class TilesFilesIndex:
    """A collection type, storing the ExportResults per tile, the R-Tree of the tiles
    and a path-array of the CityJSON files."""
    export_results: dict[str, ExportResult]
    tree: STRtree
    paths_array: NDArray


@asset()
def distribution_tiles_files_index(context,
                                   config: TileExportConfig) -> TilesFilesIndex:
    """An index of the distribution tiles and the CityJSON file paths for each tile,
    that has an existing CityJSON file.

    The index is a [Shapely RTree index](https://shapely.readthedocs.io/en/stable/strtree.html).

    Args:
        context: asset execution context
        config: asset configuration

    Returns a collection type, storing the ExportResults per tile, the R-Tree of the tiles
    and a path-array of the CityJSON files (TilesFilesIndex)
    """
    export_results_gen = filter(
        lambda t: t.has_cityjson,
        check_export_results(
            path_quadtree_tsv=Path(config.quadtree_tsv_path),
            path_tiles_dir=Path(config.tiles_dir_path)
        )
    )
    export_results = dict((t.tile_id, t) for t in export_results_gen)
    tree = STRtree(tuple(from_wkt(t.wkt) for t in export_results.values()))
    paths_array = np.array(tuple(t.cityjson_path for t in export_results.values()))
    return TilesFilesIndex(
        export_results=export_results,
        tree=tree,
        paths_array=paths_array
    )


@asset(
    partitions_def=PartitionDefinition3DBagDistribution(),
    required_resource_keys={"db_connection"}
)
def party_walls_nl(context,
                   distribution_tiles_files_index: TilesFilesIndex) -> DataFrame:
    """Party walls calculation from the exported CityJSON tiles.

    Computes a DataFrame of statistics for a given tile. The tile-boundary problem is
    resolved by including all the tile neighbours in the calculation.
    The statistics calculation is done with the [CityStats.city_stats](https://github.com/balazsdukai/urban-morphology-3d/blob/f145a784225b668b936abda1505c322c5e33b5ca/src/urban_morphology_3d/cityStats.py#L583C1-L714C1)
    function.
    """
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
        dsn=context.resources.db_connection.dsn,
        break_on_error=True
    )
    context.add_output_metadata(
        metadata={
            "Rows": len(df),
            "Head": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df


def visit_directory(z_level: Path) -> Iterable[tuple[str, Path]]:
    for x_level in z_level.iterdir():
        for y_level in x_level.iterdir():
            for identificatie in y_level.joinpath("objects").iterdir():
                # cannot use Path methods here, because we have '.' in the file name
                feature_path = Path(
                    f"{identificatie / 'reconstruct'}/{identificatie.name}.city.jsonl")
                if feature_path.exists():
                    yield identificatie.name, feature_path


def features_file_index_generator(path_features: Path) -> Iterable[tuple[str, Path]]:
    # We are at the root dir of the reconstructed features
    dir_z = [d for d in path_features.iterdir()]
    with ThreadPoolExecutor() as executor:
        for g in executor.map(visit_directory, dir_z):
            for identificatie, path in g:
                yield identificatie, path


@asset(
    required_resource_keys={"file_store_fastssd"}
)
def features_file_index(context) -> dict[str, Path]:
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.data_dir)
    return dict(features_file_index_generator(reconstructed_root_dir))


@asset(
    required_resource_keys={"file_store"}
)
def cityjsonfeatures_with_party_walls_nl(context, party_walls_nl: DataFrame,
                                         features_file_index: dict[str, Path]) -> list[
    Path]:
    """Writes the content of the party walls DataFrame back to the reconstructed
    CityJSONFeatures. These CityJSONFeatures are the reconstruction output, not the
    CityJSON tiles that is created with *tyler*."""
    export_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    # For now, we do not overwrite the reconstructed features with the part walls
    # attributes, but save a new file
    output_dir = export_dir.joinpath("party_walls_features")
    files_written = []


    output_dir_tiles = []
    for tile in party_walls_nl["tile"].unique():
        output_dir_tile = output_dir.joinpath(tile)
        output_dir_tile.mkdir(parents=True, exist_ok=True)
        output_dir_tiles.append(str(output_dir_tile))
    for row in party_walls_nl.itertuples(name="CityStats"):
        # identificatie without building part
        identificatie_bag = row.identificatie
        try:
            feature_path = features_file_index[identificatie_bag]
        except KeyError as e:
            context.log.error(f"Did not find object {e} in the feature files")
            continue
        with feature_path.open(encoding="utf-8", mode="r") as fo:
            feature_json = json.load(fo)
        attributes = feature_json["CityObjects"][identificatie_bag]["attributes"]
        attributes["b3_area_ground"] = row.area_ground
        attributes["b3_area_roof_flat"] = row.area_roof_flat
        attributes["b3_area_roof_sloped"] = row.area_roof_sloped
        attributes["b3_area_party_wall"] = row.area_shared_wall
        attributes["b3_area_exterior_wall"] = row.area_exterior_wall

        output_dir_tile = output_dir.joinpath(row.tile)
        feature_party_wall_path = Path(f"{output_dir_tile}/{identificatie_bag}.city.jsonl")
        with feature_party_wall_path.open("w") as fo:
            json.dump(feature_json, fo, separators=(',', ':'))
        files_written.append(feature_party_wall_path)

    context.add_output_metadata(
        metadata={
            "Nr. features": len(files_written),
            "Path": output_dir_tiles,
        }
    )
    return files_written
