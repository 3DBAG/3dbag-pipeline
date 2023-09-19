import pytest
import os
from dagster import build_asset_context, materialize_to_memory, RunConfig
from bag3d.party_walls.assets.party_walls import distribution_tiles_files_index, \
    TileExportConfig, party_walls_nl
from bag3d.common.resources.database import db_connection, container


@pytest.fixture(scope="function")
def tile_export_config(export_dir_uncompressed):
    return TileExportConfig(
        tiles_dir_path=str(export_dir_uncompressed.joinpath("tiles")),
        quadtree_tsv_path=str(
            export_dir_uncompressed.joinpath("quadtree.tsv")))


@pytest.fixture(scope="session")
def resource_container():
    return container.configured({"id": "testid"})


@pytest.fixture(scope="function")
def resource_db_connection():
    """db_connection resource"""
    return db_connection.configured({
        "host": "localhost",
        "port": 5432,
        "user": os.environ.get("PYTEST_DB_USER"),
        "password": os.environ.get("PYTEST_DB_PW"),
        "dbname": "baseregisters_test"
    })


def test_distribution_tiles_files_index(tile_export_config):
    """Can we parse the CityJSON tiles and return valid data?
    Currently hardcoded for Balázs' local test data.
    """
    tile_ids = ('10/564/624', '10/564/626', '10/566/624', '10/566/626', '9/560/624')
    result = distribution_tiles_files_index(
        context=build_asset_context(),
        config=tile_export_config)
    assert len(result.tree.geometries) == len(tile_ids)
    assert len(result.paths_array) == len(tile_ids)
    result_tile_ids = tuple(sorted(result.export_results.keys()))
    assert result_tile_ids == tile_ids


def test_party_walls(tile_export_config, resource_db_connection, resource_container,
                     export_dir_uncompressed):
    """Can we compute the party walls and other statistics?"""
    result = materialize_to_memory(
        [distribution_tiles_files_index, party_walls_nl],
        partition_key='10/564/624',
        run_config=RunConfig(
            ops={
                "distribution_tiles_files_index": tile_export_config
            }
        ),
        resources={"db_connection": resource_db_connection,
                   "container": resource_container}
    )
    df = result.asset_value("part_walls_nl")
    assert result.success
