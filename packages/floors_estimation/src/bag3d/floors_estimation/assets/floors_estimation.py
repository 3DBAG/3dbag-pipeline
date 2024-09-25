import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (
    create_schema,
    load_sql,
    postgrestable_from_query,
)
from bag3d.common.utils.files import geoflow_crop_dir
from bag3d.floors_estimation.assets.Attributes import Attributes
from dagster import Output, asset
from joblib import load
from pgutils import inject_parameters
from psycopg import connect
from psycopg.sql import SQL

SCHEMA = "floors_estimation"
CHUNK_SIZE = 1000


def extract_attributes_from_path(path: str, pand_id: str) -> Dict:
    with Path(path).open(encoding="utf-8", mode="r") as fo:
        feature_json = json.load(fo)
    attributes = feature_json["CityObjects"][pand_id]["attributes"]
    return attributes


def process_chunk(
    conn, chunk_files: List[str], chunk_id: int, table: PostgresTableIdentifier, logger
):
    chunk_features = [
        Attributes(**extract_attributes_from_path(path, ex_id))
        for ex_id, path in chunk_files.items()
    ]
    data = [
        (
            attr.identificatie,
            attr.oorspronkelijkbouwjaar,
            attr.b3_dak_type,
            attr.b3_h_dak_50p,
            attr.b3_h_dak_70p,
            attr.b3_h_dak_max,
            attr.b3_h_dak_min,
            attr.b3_opp_dak_plat + attr.b3_opp_dak_schuin,
            attr.b3_opp_buitenmuur,
            attr.b3_opp_scheidingsmuur,
            attr.b3_opp_grond,
            attr.b3_volume_lod22,
            attr.b3_volume_lod12,
        )
        for attr in chunk_features
    ]

    query = f"""
        INSERT INTO {table}
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;"""

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


def features_file_index_generator(path_features: Path) -> Iterable[tuple[str, Path]]:
    dir_z = [d for d in path_features.iterdir()]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for g in executor.map(visit_directory, dir_z):
            for identificatie, path in g:
                yield identificatie, path


def make_chunks(data: dict[str, Path], SIZE: int = 1000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}


@asset(required_resource_keys={"file_store_fastssd"})
def features_file_index(context) -> dict[str, Path]:
    """
    Returns a dict of {feature ID: feature file path}.
    """
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.data_dir
    )

    reconstructed_with_party_walls_dir = reconstructed_root_dir.parent.joinpath(
        "party_walls_features"
    )

    res = dict(features_file_index_generator(reconstructed_with_party_walls_dir))
    context.log.info(f"Retrieved {len(res)} features.")
    return res


@asset(required_resource_keys={"db_connection"}, op_tags={"compute_kind": "sql"})
def bag3d_features(
    context, features_file_index: dict[str, Path]
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_bag3d` table.
    Extracts 3DBAG features from the cityJSONL files,
    which already contain the party walls information."""
    context.log.info("Extracting 3DBAG features.")
    table_name = "building_features_bag3d"
    bag3d_features_table = PostgresTableIdentifier(SCHEMA, table_name)
    context.log.info(f"Creating the {table_name} table.")
    query = load_sql(query_params={"bag3d_features": bag3d_features_table})
    metadata = postgrestable_from_query(context, query, bag3d_features_table)
    context.log.info(
        f"Extracting 3DBAG features for {len(features_file_index)} buildings."
    )
    chunks = list(make_chunks(features_file_index, CHUNK_SIZE))
    context.log.info(f"Processing {len(chunks)} chunks.")

    with ThreadPoolExecutor(max_workers=4) as pool:
        processing = {
            pool.submit(
                process_chunk,
                context.resources.db_connection,
                chunk,
                cid,
                bag3d_features_table,
                context.log,
            ): cid
            for cid, chunk in enumerate(chunks)
        }
        for i, future in enumerate(as_completed(processing)):
            try:
                _ = future.result()
            except Exception as e:  # pragma: no cover
                context.log.error(f"Error in chunk {i} raised an exception: {e}")

    return Output(bag3d_features_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"compute_kind": "sql"})
def external_features(context) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_external` table.
    In contains features from CBS, ESRI and BAG."""
    context.log.info("Extracting external features, from CBS, ESRI and BAG.")
    create_schema(context, SCHEMA)
    table_name = "building_features_external"
    external_features_table = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(query_params={"external_features": external_features_table})
    metadata = postgrestable_from_query(context, query, external_features_table)
    return Output(external_features_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"compute_kind": "sql"})
def all_features(
    context,
    external_features: PostgresTableIdentifier,
    bag3d_features: PostgresTableIdentifier,
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_all` table."""
    create_schema(context, SCHEMA)
    table_name = "building_features_all"
    all_features = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(
        query_params={
            "all_features": all_features,
            "external_features": external_features,
            "bag3d_features": bag3d_features,
        }
    )
    metadata = postgrestable_from_query(context, query, all_features)
    return Output(all_features, metadata=metadata)


@asset(required_resource_keys={"db_connection"})
def preprocessed_features(
    context, all_features: Output[PostgresTableIdentifier]
) -> pd.DataFrame:
    """Runs the inference on the features."""
    context.log.info("Querying the features.")
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
    res = context.resources.db_connection.get_dict(query)
    data = pd.DataFrame.from_records(res)
    context.log.info(len(data))
    data.set_index("identificatie", inplace=True, drop=True)
    # rejecting all buildings with missing 70th percentile roof height
    data.dropna(subset=["h_roof_70p"], inplace=True)
    context.log.debug(f"Dataframe columns: {data.columns}")
    context.log.info(f"Processed features for {len(data)} buildings.")
    return data


@asset(required_resource_keys={"model_store"})
def inferenced_floors(context, preprocessed_features: pd.DataFrame) -> pd.DataFrame:
    """Runs the inference on the features."""
    context.log.info(f"Loading model from {context.resources.model_store}")
    pipeline = load(context.resources.model_store)
    context.log.info("Running the inference.")
    labels = pipeline.predict(preprocessed_features)
    preprocessed_features["floors"] = labels
    preprocessed_features["floors_int"] = preprocessed_features["floors"].apply(np.rint)
    context.log.debug(preprocessed_features.head(5))
    return preprocessed_features


@asset(required_resource_keys={"db_connection"})
def predictions_table(
    context, inferenced_floors: pd.DataFrame
) -> Output[PostgresTableIdentifier]:
    """Saves the floor predictions to the
    'floors_estimation.predictions' table."""

    context.log.info("Saving to the 'floors_estimation.predictions'.")
    table_name = "predictions"
    predictions_table = PostgresTableIdentifier(SCHEMA, table_name)
    context.log.info(f"Creating the {table_name} table.")
    query = load_sql(query_params={"predictions_table": predictions_table})
    metadata = postgrestable_from_query(context, query, predictions_table)

    inferenced_floors.reset_index(inplace=True)
    data = [tuple(v) for v in inferenced_floors[["identificatie", "floors"]].to_numpy()]

    query = f"""INSERT INTO {predictions_table}
                VALUES (%s, %s);"""

    with connect(context.resources.db_connection.dsn) as connection:
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


@asset(required_resource_keys={"file_store_fastssd"})
def save_cjfiles(
    context, inferenced_floors: pd.DataFrame, features_file_index: dict[str, Path]
) -> None:
    """Saves the new cj files."""
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.data_dir
    )
    reconstructed_with_floors_estimation_dir = reconstructed_root_dir.parent.joinpath(
        "bouwlagen_features"
    )
    context.log.info("Creating directories for the new files.")
    tile_paths = set([f.parent for f in list(features_file_index.values())])
    for tile_path in tile_paths:
        new_tile = reconstructed_with_floors_estimation_dir.joinpath(
            tile_path.parents[1].name, tile_path.parents[0].name, tile_path.name
        )
        new_tile.mkdir(parents=True, exist_ok=True)

    context.log.info(f"Saving to {reconstructed_with_floors_estimation_dir}")

    with ThreadPoolExecutor(max_workers=8) as pool:
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
                context.log.error(f"Error in file {i} raised an exception: {e}")

    context.log.info(f"""Saved {len(features_file_index)} files
                     to {reconstructed_with_floors_estimation_dir}""")
