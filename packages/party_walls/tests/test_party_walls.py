from dagster import build_asset_context
from bag3d.party_walls.assets.party_walls import distribution_tiles_files_index, \
    TileExportConfig


def test_distribution_tiles_files_index(export_dir_uncompressed):
    """Can we parse the CityJSON tiles and return valid data?
    Currently hardcoded for Balázs' local test data.
    """
    tile_ids = ('10/564/624', '10/564/626', '10/566/624', '10/566/626', '9/560/624')
    result = distribution_tiles_files_index(
        context=build_asset_context(),
        config=TileExportConfig(tiles_dir_path=str(export_dir_uncompressed.joinpath("tiles")),
                                quadtree_tsv_path=str(
                                    export_dir_uncompressed.joinpath("quadtree.tsv"))))
    assert len(result.tree.geometries) == len(tile_ids)
    assert len(result.paths_array) == len(tile_ids)
    result_tile_ids = tuple(sorted(result.export_results.keys()))
    assert result_tile_ids == tile_ids