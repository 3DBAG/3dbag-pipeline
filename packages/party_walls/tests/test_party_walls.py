import pytest
import os
from dagster import build_asset_context, materialize_to_memory, RunConfig
from bag3d.party_walls.assets.party_walls import distribution_tiles_files_index, \
    party_walls_nl, cityjsonfeatures_with_party_walls_nl, \
    features_file_index_generator
from bag3d.common.resources.database import db_connection, container
from bag3d.common.resources.files import file_store
import pandas as pd


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


@pytest.fixture(scope="session")
def resource_file_store(output_data_dir):
    """file_store resource pointing to the test output location"""
    return file_store.configured({"data_dir": str(output_data_dir), })


def test_distribution_tiles_files_index(export_dir_uncompressed):
    """Can we parse the CityJSON tiles and return valid data?
    Currently hardcoded for Balázs' local test data.
    """
    tile_ids = ('10/564/624', '10/564/626', '10/566/624', '10/566/626', '9/560/624')

    result = distribution_tiles_files_index(
        context=build_asset_context(
            resources={"file_store": file_store.configured(
                {"data_dir": str(export_dir_uncompressed), })}
        )
    )
    assert len(result.tree.geometries) == len(tile_ids)
    assert len(result.paths_array) == len(tile_ids)
    result_tile_ids = tuple(sorted(result.export_results.keys()))
    assert result_tile_ids == tile_ids


def test_party_walls(resource_db_connection, resource_container,
                     export_dir_uncompressed, output_data_dir):
    """Can we compute the party walls and other statistics?"""
    result = materialize_to_memory(
        [distribution_tiles_files_index, party_walls_nl],
        partition_key='10/564/624',
        resources={"db_connection": resource_db_connection,
                   "container": resource_container}
    )
    df = result.asset_value("party_walls_nl")
    df.to_csv(str(output_data_dir / "party_walls_nl.csv"))
    assert result.success


def test_features_file_index(crop_reconstruct_data_dir):
    """Can we find and map all the 5800 citjsonfeature files of the test data?"""
    result = dict(features_file_index_generator(crop_reconstruct_data_dir))
    assert 5800 == len(result)


def test_cityjsonfeatures_with_party_walls_nl(output_data_dir,
                                              crop_reconstruct_data_dir,
                                              resource_file_store):
    """Can we create cityjsonfeatures with the party wall data?
    Currently, this test uses the csv that is created by `test_party_walls`.
    """
    features_file_index_dict = dict(
        features_file_index_generator(crop_reconstruct_data_dir))
    party_walls_nl_df = pd.read_csv(output_data_dir / "party_walls_nl.csv")
    result = cityjsonfeatures_with_party_walls_nl(
        context=build_asset_context(
            resources={"file_store": resource_file_store}
        ),
        party_walls_nl=party_walls_nl_df,
        features_file_index=features_file_index_dict
    )
    print(result)
