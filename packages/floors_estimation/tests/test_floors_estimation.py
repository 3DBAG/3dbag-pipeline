from pathlib import Path

import pandas as pd
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import table_exists
from bag3d.floors_estimation.assets.floors_estimation import (
    FloorsEstimationConfig,
    FloorsEstimationIOConfig,
    all_features,
    bag3d_features,
    external_features,
    features_file_index,
    inferenced_floors,
    make_chunks,
    predictions_table,
    preprocessed_features,
    save_cjfiles,
)
from dagster import Output


def test_features_file_index(file_store_fastssd):
    """"""
    result = features_file_index(
        FloorsEstimationConfig(),
        file_store_fastssd,
    )
    assert isinstance(result, dict)
    assert len(result) == 413
    assert "NL.IMBAG.Pand.0307100000377456" in result.keys()
    assert "party_walls_features" in str(result["NL.IMBAG.Pand.0307100000377456"])


def test_make_chunks():
    """Can we make data chunks from a dictionary of id:path pairs?"""
    data = {
        "id1": Path("path1"),
        "id2": Path("path2"),
        "id3": Path("path3"),
        "id4": Path("path4"),
        "id5": Path("path5"),
        "id6": Path("path6"),
    }

    chunks = make_chunks(data, 3)
    assert next(chunks) == {
        "id1": Path("path1"),
        "id2": Path("path2"),
        "id3": Path("path3"),
    }
    assert next(chunks) == {
        "id4": Path("path4"),
        "id5": Path("path5"),
        "id6": Path("path6"),
    }

    chunks2 = make_chunks(data, 4)

    assert next(chunks2) == {
        "id1": Path("path1"),
        "id2": Path("path2"),
        "id3": Path("path3"),
        "id4": Path("path4"),
    }
    assert next(chunks2) == {"id5": Path("path5"), "id6": Path("path6")}


def test_bag3d_features(database, mock_features_file_index):
    res = bag3d_features(
        FloorsEstimationConfig(),
        mock_features_file_index,
        database,
    )

    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    building_feature_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_bag3d"
    )
    assert table_exists(database, building_feature_table) is True


def test_external_features(database):
    res = external_features(
        database,
    )

    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    external_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_external"
    )
    assert table_exists(database, external_features_table) is True


def test_all_features(database):
    external_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_external"
    )
    building_feature_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_bag3d"
    )
    res = all_features(
        external_features_table,
        building_feature_table,
        database,
    )

    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    all_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_all"
    )
    assert table_exists(database, all_features_table) is True


def test_preprocessed_features(database):
    all_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_all"
    )
    assert table_exists(database, all_features_table) is True
    data = preprocessed_features(
        all_features_table,
        database,
    )
    assert isinstance(data, pd.DataFrame)
    assert data.shape[0] == 6


def test_inferenced_floors(model_store, mock_preprocessed_features):
    res = inferenced_floors(mock_preprocessed_features, model_store)
    assert isinstance(res, pd.DataFrame)
    assert "floors" in res.columns
    assert "floors_int" in res.columns


def test_predictions_table(database, mock_inferenced_floors):
    res = predictions_table(
        mock_inferenced_floors,
        database,
    )
    assert isinstance(res, Output)
    assert isinstance(res.value, PostgresTableIdentifier)
    pred_table = PostgresTableIdentifier("floors_estimation", "predictions")
    assert table_exists(database, pred_table) is True


def test_save_cjfiles(
    file_store_tmp,
    mock_inferenced_floors,
    mock_features_file_index,
):
    from bag3d.common.resources.files import FileStoreResource

    file_store_resource = FileStoreResource(data_dir=str(file_store_tmp))
    save_cjfiles(
        FloorsEstimationIOConfig(),
        mock_inferenced_floors,
        mock_features_file_index,
        file_store_resource,
    )
    assert (
        file_store_tmp
        / "3DBAG/bouwlagen_features/0/0/0/NL.IMBAG.Pand.0307100000364333.city.jsonl"
    ).exists()
