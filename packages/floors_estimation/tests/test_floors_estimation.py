from pathlib import Path
from typing import Dict 
import json
from bag3d.floors_estimation.assets.floors_estimation import (features_file_index,
                                                              make_chunks,
                                                              bag3d_features,
                                                              external_features,
                                                              all_features,
                                                              preprocessed_features,
                                                              inferenced_floors)
from dagster import asset
import pickle
from bag3d.common.utils.database import drop_table, table_exists
from bag3d.common.types import PostgresTableIdentifier
import pickle
from pandas import DataFrame
def test_features_file_index(context, output_data_dir):
    """"""
    result = features_file_index(
         context=context
    )
    assert len(result) == 412
    assert 'NL.IMBAG.Pand.0307100000377456' in result.keys()
    assert 'party_walls_features' in str(result['NL.IMBAG.Pand.0307100000377456'])
    pickle.dump(result, open(output_data_dir / "features_file_index_floors_estimation.pkl", "wb"))


def test_make_chunks():
    """Can we make data chunks from a dictionary of id:path pairs?"""
    data = {"id1":Path("path1"),
            "id2":Path("path2"),
            "id3":Path("path3"),
            "id4":Path("path4"),
            "id5":Path("path5"),
            "id6":Path("path6")}
    
    chunks = make_chunks(data, 3)
    assert next(chunks) == {'id1': Path('path1'),
                            'id2': Path('path2'),
                            'id3': Path('path3')}
    assert next(chunks) == {'id4': Path('path4'),
                            'id5': Path('path5'),
                            'id6': Path('path6')}

    chunks2 = make_chunks(data, 4)

    assert next(chunks2) == {'id1': Path('path1'),
                             'id2': Path('path2'),
                             'id3': Path('path3'),
                             'id4': Path('path4')}
    assert next(chunks2) == {'id5': Path('path5'),
                             'id6': Path('path6')}

@asset(name="features_file_index")
def mock_features_file_index(output_data_dir)  -> dict[str, Path]:
    return pickle.load(open(output_data_dir / "features_file_index_floors_estimation.pkl", "rb"))


def test_bag3d_features(context, output_data_dir):
    res = bag3d_features(context,
                         features_file_index=mock_features_file_index(output_data_dir))

    assert res.value is not None
    building_feature_table = PostgresTableIdentifier("floors_estimation", "building_features_bag3d")
    assert table_exists(context, building_feature_table) is True
    # drop_table(context, building_feature_table)
    # assert table_exists(context, building_feature_table) is False


def test_external_features(context):
    res = external_features(context)

    assert res.value is not None
    external_features_table = PostgresTableIdentifier("floors_estimation", "building_features_external")
    assert table_exists(context, external_features_table) is True


def test_all_features(context):
    external_features_table = PostgresTableIdentifier("floors_estimation", "building_features_external")
    building_feature_table = PostgresTableIdentifier("floors_estimation", "building_features_bag3d")
    res = all_features(context, external_features_table, building_feature_table)

    assert res.value is not None
    all_features_table = PostgresTableIdentifier("floors_estimation", "building_features_all")
    assert table_exists(context, all_features_table) is True

def test_preprocessed_features(context, output_data_dir):
    all_features_table = PostgresTableIdentifier("floors_estimation", "building_features_all")
    assert table_exists(context, all_features_table) is True
    data = preprocessed_features(context, all_features_table)
    assert data is not None
    assert data.shape[0] == 274
    pickle.dump(data, open(output_data_dir / "preprocessed_features.pkl", "wb"))

@asset(name="preprocessed_features")
def mock_preprocessed_features(output_data_dir)  -> DataFrame:
    return pickle.load(open(output_data_dir / "preprocessed_features.pkl", "rb"))

def test_inferenced_floors(context, output_data_dir):
    d = mock_preprocessed_features(output_data_dir)
    print(d.shape[0])
    res = inferenced_floors(context, preprocessed_features=mock_preprocessed_features(output_data_dir))
    assert res is not None
    assert "floors" in res.columns
    assert "floors_int" in res.columns