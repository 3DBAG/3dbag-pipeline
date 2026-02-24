import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from os import getenv
from pathlib import Path
from typing import Dict, Iterable

import numpy as np
import pandas as pd
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (
    create_schema,
    load_sql,
    postgrestable_from_query,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.database import DatabaseResource
from bag3d.floors_estimation.resources import ModelStoreResource
from dagster import Config, Output, asset, get_dagster_logger
from joblib import load
from pgutils import inject_parameters, PostgresConnection
from psycopg import connect
from psycopg.sql import SQL
from pydantic import Field

SCHEMA = "floors_estimation"
CHUNK_SIZE = 1000

logger = get_dagster_logger("floors_estimation")


class FloorsEstimationConfig(Config):
    """Configuration for floors_estimation assets."""

    concurrency: int = Field(
        default_factory=lambda: int(
            getenv("BAG3D_CONCURRENCY_TOOL_FLOORS_ESTIMATION", "4")
        ),
        description="Number of threads for parallel processing.",
    )


class FloorsEstimationIOConfig(Config):
    """Configuration for I/O-bound floors_estimation assets."""

    concurrency: int = Field(
        default_factory=lambda: int(
            getenv("BAG3D_CONCURRENCY_TOOL_FLOORS_ESTIMATION_IO", "8")
        ),
        description="Number of threads for parallel file writing.",
    )


def extract_attributes_from_path(path: str, pand_id: str) -> Dict:
    with Path(path).open(encoding="utf-8", mode="r") as fo:
        feature_json = json.load(fo)
    attributes = feature_json["CityObjects"][pand_id]["attributes"]
    return attributes


def process_chunk(
    conn: PostgresConnection,
    chunk_files: Dict[str, Path],
    chunk_id: int,
    table: PostgresTableIdentifier,
    logger,
):
    chunk_features = [
        extract_attributes_from_path(str(path), ex_id)
        for ex_id, path in chunk_files.items()
    ]
    required_attributes = [
        "identificatie",
        "oorspronkelijkbouwjaar",
        "b3_dak_type",
        "b3_h_dak_50p",
        "b3_h_dak_70p",
        "b3_h_dak_max",
        "b3_h_dak_min",
        "b3_opp_dak_plat",
        "b3_opp_dak_schuin",
        "b3_opp_buitenmuur",
        "b3_opp_scheidingsmuur",
        "b3_opp_grond",
        "b3_volume_lod22",
        "b3_volume_lod12",
    ]

    data = []
    for attr_dict in chunk_features:
        # Check for missing attributes
        missing_attrs = [attr for attr in required_attributes if attr not in attr_dict]
        if missing_attrs:
            raise KeyError(f"Missing required attributes: {missing_attrs}")

        opp_dak_plat = attr_dict.get("b3_opp_dak_plat") or 0
        opp_dak_schuin = attr_dict.get("b3_opp_dak_schuin") or 0
        row = (
            attr_dict["identificatie"],
            attr_dict["oorspronkelijkbouwjaar"],
            attr_dict["b3_dak_type"],
            attr_dict["b3_h_dak_50p"],
            attr_dict["b3_h_dak_70p"],
            attr_dict["b3_h_dak_max"],
            attr_dict["b3_h_dak_min"],
            opp_dak_plat + opp_dak_schuin,
            attr_dict["b3_opp_buitenmuur"],
            attr_dict["b3_opp_scheidingsmuur"],
            attr_dict["b3_opp_grond"],
            attr_dict["b3_volume_lod22"],
            attr_dict["b3_volume_lod12"],
        )
        data.append(row)

    query = SQL("""
        INSERT INTO {}
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;""").format(table.id)

    with connect(conn.dsn) as connection:
        with connection.cursor() as cur:
            cur.executemany(query, data, returning=True)
            connection.commit()

    logger.info(f"Chunk {chunk_id} done.")


def visit_directory(z_level: Path) -> Iterable[tuple[str, Path]]:
    for x_level in z_level.iterdir():
        for y_level in x_level.iterdir():
            for feature_path in y_level.iterdir():
                yield feature_path.with_suffix("").stem, feature_path


def features_file_index_generator(
    path_features: Path, max_workers: int = 4
) -> Iterable[tuple[str, Path]]:
    dir_z = [d for d in path_features.iterdir()]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for g in executor.map(visit_directory, dir_z):
            for identificatie, path in g:
                yield identificatie, path


def make_chunks(data: dict[str, Path], SIZE: int = 1000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}


@asset
def features_file_index(
    config: FloorsEstimationConfig, file_store_fastssd: FileStoreResource
) -> dict[str, Path]:
    """
    Returns a dict of {feature ID: feature file path}.
    """
    reconstructed_root_dir = file_store_fastssd.geoflow_crop_dir

    reconstructed_with_party_walls_dir = reconstructed_root_dir.parent.joinpath(
        "party_walls_features"
    )

    res = dict(
        features_file_index_generator(
            reconstructed_with_party_walls_dir, config.concurrency
        )
    )
    logger.info(f"Retrieved {len(res)} features.")
    return res


@asset(op_tags={"compute_kind": "sql"})
def bag3d_features(
    config: FloorsEstimationConfig,
    features_file_index: dict[str, Path],
    db_connection: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_bag3d` table.
    Extracts 3DBAG features from the cityJSONL files,
    which already contain the party walls information."""
    logger.info("Extracting 3DBAG features.")
    table_name = "building_features_bag3d"
    bag3d_features_table = PostgresTableIdentifier(SCHEMA, table_name)
    logger.info(f"Creating the {table_name} table.")
    query = load_sql(query_params={"bag3d_features": bag3d_features_table})
    metadata = postgrestable_from_query(
        db_connection, query, bag3d_features_table, logger
    )
    logger.info(f"Extracting 3DBAG features for {len(features_file_index)} buildings.")
    chunks = list(make_chunks(features_file_index, CHUNK_SIZE))
    logger.info(f"Processing {len(chunks)} chunks.")

    with ThreadPoolExecutor(max_workers=config.concurrency) as pool:
        processing = {
            pool.submit(
                process_chunk,
                db_connection.connection,
                chunk,
                cid,
                bag3d_features_table,
                logger,
            ): cid
            for cid, chunk in enumerate(chunks)
        }
        for i, future in enumerate(as_completed(processing)):
            try:
                _ = future.result()
            except Exception as e:  # pragma: no cover
                logger.error(f"Error in chunk {i} raised an exception: {e}")

    return Output(bag3d_features_table, metadata=metadata)


@asset(op_tags={"compute_kind": "sql"})
def external_features(
    db_connection: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_external` table.
    In contains features from CBS, ESRI and BAG."""
    logger.info("Extracting external features, from CBS, ESRI and BAG.")
    create_schema(db_connection, SCHEMA, logger)
    table_name = "building_features_external"
    external_features_table = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(query_params={"external_features": external_features_table})
    metadata = postgrestable_from_query(
        db_connection, query, external_features_table, logger
    )
    return Output(external_features_table, metadata=metadata)


@asset(op_tags={"compute_kind": "sql"})
def all_features(
    external_features: PostgresTableIdentifier,
    bag3d_features: PostgresTableIdentifier,
    db_connection: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_all` table."""
    create_schema(db_connection, SCHEMA, logger)
    table_name = "building_features_all"
    all_features = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(
        query_params={
            "all_features": all_features,
            "external_features": external_features,
            "bag3d_features": bag3d_features,
        }
    )
    metadata = postgrestable_from_query(db_connection, query, all_features, logger)
    return Output(all_features, metadata=metadata)


@asset
def preprocessed_features(
    all_features: PostgresTableIdentifier,
    db_connection: DatabaseResource,
) -> pd.DataFrame:
    """Runs the inference on the features."""
    logger.info("Querying the features.")
    query = SQL("""
        SELECT *
        FROM {all_features}
        WHERE  construction_year > 1005
        AND construction_year < 2025
        AND h_roof_max < 300;
        """)

    query_params = {
        "all_features": all_features,
    }

    query = inject_parameters(query, query_params)
    res = db_connection.connection.get_dict(query)
    data = pd.DataFrame.from_records(res)
    logger.info(f"Retrieved {len(data)} buildings.")
    data.set_index("identificatie", inplace=True, drop=True)
    # rejecting all buildings with missing 70th percentile roof height
    data.dropna(subset=["h_roof_70p"], inplace=True)
    logger.debug(f"Dataframe columns: {data.columns}")
    logger.info(f"Processed features for {len(data)} buildings.")
    return data


@asset
def inferenced_floors(
    preprocessed_features: pd.DataFrame, model_store: ModelStoreResource
) -> pd.DataFrame:
    """Runs the inference on the features."""
    logger.info(f"Loading model from {model_store.model_path}")
    pipeline = load(model_store.model_path)
    logger.info("Running the inference.")
    labels = pipeline.predict(preprocessed_features)
    preprocessed_features["floors"] = labels
    preprocessed_features["floors_int"] = preprocessed_features["floors"].apply(np.rint)
    logger.debug(preprocessed_features.head(5))
    return preprocessed_features


@asset
def predictions_table(
    inferenced_floors: pd.DataFrame, db_connection: DatabaseResource
) -> Output[PostgresTableIdentifier]:
    """Saves the floor predictions to the
    'floors_estimation.predictions' table."""

    logger.info("Saving to the 'floors_estimation.predictions'.")
    table_name = "predictions"
    predictions_table = PostgresTableIdentifier(SCHEMA, table_name)
    logger.info(f"Creating the {table_name} table.")
    query = load_sql(query_params={"predictions_table": predictions_table})
    metadata = postgrestable_from_query(db_connection, query, predictions_table, logger)

    inferenced_floors.reset_index(inplace=True)
    data = [tuple(v) for v in inferenced_floors[["identificatie", "floors"]].to_numpy()]

    query = SQL("""INSERT INTO {}
                VALUES (%s, %s);""").format(predictions_table.id)

    with connect(db_connection.connection.dsn) as connection:
        with connection.cursor() as cur:
            cur.executemany(query, data, returning=True)
            connection.commit()

    return Output(predictions_table, metadata=metadata)


def save_cjfile(
    path: Path, pand_id: str, inferenced_floors: pd.DataFrame, output_dir: Path
):
    with path.open(encoding="utf-8", mode="r") as fo:
        feature_json = json.load(fo)
    attributes = feature_json["CityObjects"][pand_id]["attributes"]

    if pand_id in inferenced_floors.index:
        num_floors = int(inferenced_floors.loc[pand_id, "floors_int"])
        if num_floors <= 5:
            attributes["b3_bouwlagen"] = num_floors
        else:
            attributes["b3_bouwlagen"] = None
    else:
        attributes["b3_bouwlagen"] = None

    output_path = output_dir.joinpath(
        path.parents[2].name, path.parents[1].name, path.parents[0].name, path.name
    )

    with output_path.open("w") as fo:
        json.dump(feature_json, fo, separators=(",", ":"))


@asset
def save_cjfiles(
    config: FloorsEstimationIOConfig,
    inferenced_floors: pd.DataFrame,
    features_file_index: dict[str, Path],
    file_store_fastssd: FileStoreResource,
) -> None:
    """Saves the new cj files."""
    reconstructed_root_dir = file_store_fastssd.geoflow_crop_dir
    reconstructed_with_floors_estimation_dir = reconstructed_root_dir.parent.joinpath(
        "bouwlagen_features"
    )
    logger.info("Creating directories for the new files.")
    tile_paths = set([f.parent for f in list(features_file_index.values())])
    for tile_path in tile_paths:
        new_tile = reconstructed_with_floors_estimation_dir.joinpath(
            tile_path.parents[1].name, tile_path.parents[0].name, tile_path.name
        )
        new_tile.mkdir(parents=True, exist_ok=True)

    logger.info(f"Saving to {reconstructed_with_floors_estimation_dir}")

    with ThreadPoolExecutor(max_workers=config.concurrency) as pool:
        processing = {
            pool.submit(
                save_cjfile,
                path,
                pand_id,
                inferenced_floors,
                reconstructed_with_floors_estimation_dir,
            ): pand_id
            for pand_id, path in features_file_index.items()
        }
        for i, future in enumerate(as_completed(processing)):
            try:
                _ = future.result()
            except Exception as e:  # pragma: no cover
                logger.error(f"Error in file {i} raised an exception: {e}")

    logger.info(f"""Saved {len(features_file_index)} files
                     to {reconstructed_with_floors_estimation_dir}""")
