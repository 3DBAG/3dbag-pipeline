from dagster import build_asset_context
from bag3d.party_walls.assets.party_walls import distribution_tiles_files_index, \
    TileExportConfig


def test_distribution_tiles_files_index(export_dir):
    """Can we parse the CityJSON tiles and return valid data?
    Currently hardcoded for Balázs' local test data.
    """
    tile_ids = ('10/268/588', '10/268/590', '10/270/588', '9/268/572', '9/268/576')
    result = distribution_tiles_files_index(
        context=build_asset_context(),
        config=TileExportConfig(tiles_dir_path=str(export_dir.joinpath("tiles")),
                                quadtree_tsv_path=str(
                                    export_dir.joinpath("quadtree.tsv"))))
    assert len(result.tree.geometries) == len(tile_ids)
    assert len(result.paths_array) == len(tile_ids)
    result_tile_ids = tuple(sorted(result.export_results.keys()))
    assert result_tile_ids == tile_ids