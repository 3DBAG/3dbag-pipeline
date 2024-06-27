import os

import pandas as pd
import pytest
from bag3d.common.resources.files import file_store
from bag3d.party_walls.assets.party_walls import (
    cityjsonfeatures_with_party_walls_nl, distribution_tiles_files_index,
    features_file_index, features_file_index_generator, party_walls_nl)
from dagster import (materialize_to_memory)


def test_distribution_tiles_files_index(context):
    """Can we parse the CityJSON tiles and return valid data? """
    tile_ids = ('10/564/624', '10/564/626', '10/566/624', '10/566/626', '9/560/624')

    result = distribution_tiles_files_index(
        context
    )
    assert len(result.tree.geometries) == len(tile_ids)
    assert len(result.paths_array) == len(tile_ids)
    result_tile_ids = tuple(sorted(result.export_results.keys()))
    assert result_tile_ids == tile_ids

@pytest.mark.slow
def test_party_walls(database, resource_container,
                     output_data_dir, export_dir_uncompressed):
    """Can we compute the party walls and other statistics?"""
    result = materialize_to_memory(
        [party_walls_nl, distribution_tiles_files_index],
        partition_key='10/564/624',
        resources={"db_connection": database,
                   "container": resource_container,
                   "file_store": file_store.configured(
                 {"data_dir": str(export_dir_uncompressed), }),}
    )
    df = result.asset_value("party_walls_nl")
    df.to_csv(str(output_data_dir / "party_walls_nl.csv"))
    assert not df.empty
    assert result.success


def test_features_file_index_generator(crop_reconstruct_data_dir):
    """Can we find and map all the 5800 citjsonfeature files of the test data?"""
    result = dict(features_file_index_generator(crop_reconstruct_data_dir))
    assert len(result) == 5825


def test_features_file_index(context):
    """Can we find and map all the 5800 citjsonfeature files of the test data?"""
    result = features_file_index(
         context=context
    )
    assert len(result) == 5825


def test_cityjsonfeatures_with_party_walls_nl(context,
                                              crop_reconstruct_data_dir,
                                              output_data_dir):
    """Can we create cityjsonfeatures with the party wall data?
    Currently, this test uses the csv that is created by `test_party_walls`.
    """
    features_file_index_dict = dict(
        features_file_index_generator(crop_reconstruct_data_dir))
    party_walls_nl_df = pd.read_csv(output_data_dir / "party_walls_nl.csv")
    result = cityjsonfeatures_with_party_walls_nl(
        context=context,
        party_walls_nl=party_walls_nl_df,
        features_file_index=features_file_index_dict
    )
    assert result[0].stem == 'NL.IMBAG.Pand.0307100000308298.city'
    assert result[0].suffix == '.jsonl'
