import json
import pickle
from pathlib import Path
from typing import Dict

from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import drop_table, table_exists
from bag3d.floors_estimation.assets.floors_estimation import (
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
from dagster import asset
from pandas import DataFrame


def test_features_file_index(context, output_data_dir):
    """"""
    result = features_file_index(context=context)
    assert len(result) == 412
    assert "NL.IMBAG.Pand.0307100000377456" in result.keys()
    assert "party_walls_features" in str(result["NL.IMBAG.Pand.0307100000377456"])
    pickle.dump(
        result,
        open(output_data_dir / "features_file_index_floors_estimation.pkl", "wb"),
    )


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


@asset(name="features_file_index")
def mock_features_file_index(output_data_dir) -> dict[str, Path]:
    return pickle.load(
        open(output_data_dir / "features_file_index_floors_estimation.pkl", "rb")
    )


def test_bag3d_features(context, output_data_dir):
    res = bag3d_features(
        context, features_file_index=mock_features_file_index(output_data_dir)
    )

    assert res.value is not None
    building_feature_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_bag3d"
    )
    assert table_exists(context, building_feature_table) is True


def test_external_features(context):
    res = external_features(context)

    assert res.value is not None
    external_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_external"
    )
    assert table_exists(context, external_features_table) is True


def test_all_features(context):
    external_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_external"
    )
    building_feature_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_bag3d"
    )
    res = all_features(context, external_features_table, building_feature_table)

    assert res.value is not None
    all_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_all"
    )
    assert table_exists(context, all_features_table) is True


def test_preprocessed_features(context, output_data_dir):
    all_features_table = PostgresTableIdentifier(
        "floors_estimation", "building_features_all"
    )
    assert table_exists(context, all_features_table) is True
    data = preprocessed_features(context, all_features_table)
    assert data is not None
    assert data.shape[0] == 274


@asset(name="preprocessed_features")
def mock_preprocessed_features(output_data_dir) -> DataFrame:
    return pickle.load(open(output_data_dir / "preprocessed_features.pkl", "rb"))


def test_inferenced_floors(context, output_data_dir):
    res = inferenced_floors(
        context, preprocessed_features=mock_preprocessed_features(output_data_dir)
    )
    assert res is not None
    assert "floors" in res.columns
    assert "floors_int" in res.columns


@asset(name="inferenced_floors")
def mock_inferenced_floors(output_data_dir) -> DataFrame:
    return pickle.load(open(output_data_dir / "inferenced_floors.pkl", "rb"))


def test_predictions_table(context, output_data_dir):
    res = predictions_table(
        context, inferenced_floors=mock_inferenced_floors(output_data_dir)
    )
    assert res.value is not None
    pred_table = PostgresTableIdentifier("floors_estimation", "predictions")
    assert table_exists(context, pred_table) is True


def test_save_cjfiles(context, output_data_dir, input_data_dir):
    save_cjfiles(
        context,
        mock_inferenced_floors(output_data_dir),
        mock_features_file_index(output_data_dir),
    )
    assert (
        input_data_dir
        / "3DBAG/bouwlagen_features/10/564/624/NL.IMBAG.Pand.0307100000308298.city.jsonl"
    ).exists()
