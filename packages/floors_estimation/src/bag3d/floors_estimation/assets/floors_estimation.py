from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getenv
from pathlib import Path
from typing import Any, Dict

import cityjson_index
import numpy as np
import pandas as pd
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (
    create_schema,
    load_sql,
    postgrestable_from_query,
)
from bag3d.common.resources.cjindex import (
    CityIndexResource,
    iter_package_refs,
    open_ready_index,
    read_package_feature_json,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.utils.cityjsonseq import (
    FeatureRecord,
    write_feature_records_as_cityjsonseq,
)
from bag3d.floors_estimation.resources import ModelStoreResource
from dagster import AssetKey, Config, Output, asset, get_dagster_logger
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


_REQUIRED_ATTRIBUTES = [
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


def _process_chunk(
    conn: PostgresConnection,
    chunk_attrs: list[Dict],
    chunk_id: int,
    table: PostgresTableIdentifier,
    logger,
):
    """Insert a chunk of attribute dicts into the bag3d_features table."""
    data = []
    for attr_dict in chunk_attrs:
        missing_attrs = [attr for attr in _REQUIRED_ATTRIBUTES if attr not in attr_dict]
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


@asset(
    deps=[AssetKey(["party_walls", "building_surfaces"])],
    op_tags={"compute_kind": "sql"},
)
def bag3d_features(
    config: FloorsEstimationConfig,
    party_walls_index: CityIndexResource,
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_bag3d` table.
    Extracts 3DBAG features from the party_walls index,
    which already contain the party walls information."""
    logger.info("Extracting 3DBAG features.")
    table_name = "building_features_bag3d"
    bag3d_features_table = PostgresTableIdentifier(SCHEMA, table_name)
    logger.info(f"Creating the {table_name} table.")
    query = load_sql(query_params={"bag3d_features": bag3d_features_table})
    metadata = postgrestable_from_query(
        computation_db, query, bag3d_features_table, logger
    )

    idx = open_ready_index(party_walls_index)
    total = idx.feature_bounds_summary().package_count
    logger.info(f"Extracting 3DBAG features for {total} buildings.")

    chunk_id = 0
    futures_map = {}

    with ThreadPoolExecutor(max_workers=config.concurrency) as pool:
        for refs in iter_package_refs(idx, CHUNK_SIZE):
            chunk_attrs = []
            for ref in refs:
                feature_json = read_package_feature_json(idx, ref)
                attributes = feature_json["CityObjects"][ref.model_id]["attributes"]
                chunk_attrs.append(attributes)

            future = pool.submit(
                _process_chunk,
                computation_db.connection,
                chunk_attrs,
                chunk_id,
                bag3d_features_table,
                logger,
            )
            futures_map[future] = chunk_id
            chunk_id += 1

        for i, future in enumerate(as_completed(futures_map)):
            try:
                _ = future.result()
            except Exception as e:  # pragma: no cover
                logger.error(f"Error in chunk {i} raised an exception: {e}")

    idx.close()
    logger.info(f"Processed {chunk_id} chunks.")
    return Output(bag3d_features_table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "sql"},
    deps=[
        AssetKey(["cbs", "cbs_key_figures"]),
        AssetKey(["cbs", "cbs_buurten"]),
    ],
)
def external_features(
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_external` table.
    In contains features from CBS, BAG and our own building type feature."""
    logger.info("Extracting external features, from CBS and BAG.")
    create_schema(computation_db, SCHEMA, logger)
    table_name = "building_features_external"
    external_features_table = PostgresTableIdentifier(SCHEMA, table_name)
    cbs_schema = "cbs"
    reconstructed_schema = "reconstruction_input"
    query = load_sql(
        query_params={
            "external_features": external_features_table,
            "cbs_key_figures": PostgresTableIdentifier(
                cbs_schema, "key_figures_districts_neighbourhoods"
            ),
            "cbs_buurten": PostgresTableIdentifier(cbs_schema, "buurten"),
            "building_type": PostgresTableIdentifier(
                reconstructed_schema, "woningtypen"
            ),
        }
    )
    metadata = postgrestable_from_query(
        computation_db, query, external_features_table, logger
    )
    return Output(external_features_table, metadata=metadata)


@asset(op_tags={"compute_kind": "sql"})
def all_features(
    external_features: PostgresTableIdentifier,
    bag3d_features: PostgresTableIdentifier,
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_all` table."""
    create_schema(computation_db, SCHEMA, logger)
    table_name = "building_features_all"
    all_features = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(
        query_params={
            "all_features": all_features,
            "external_features": external_features,
            "bag3d_features": bag3d_features,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, all_features, logger)
    return Output(all_features, metadata=metadata)


@asset
def preprocessed_features(
    all_features: PostgresTableIdentifier,
    computation_db: DatabaseResource,
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
    res = computation_db.connection.get_dict(query)
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
    inferenced_floors: pd.DataFrame, computation_db: DatabaseResource
) -> Output[PostgresTableIdentifier]:
    """Saves the floor predictions to the
    'floors_estimation.predictions' table."""

    logger.info("Saving to the 'floors_estimation.predictions'.")
    table_name = "predictions"
    predictions_table = PostgresTableIdentifier(SCHEMA, table_name)
    logger.info(f"Creating the {table_name} table.")
    query = load_sql(query_params={"predictions_table": predictions_table})
    metadata = postgrestable_from_query(
        computation_db, query, predictions_table, logger
    )

    inferenced_floors.reset_index(inplace=True)
    data = [tuple(v) for v in inferenced_floors[["identificatie", "floors"]].to_numpy()]

    query = SQL("""INSERT INTO {}
                VALUES (%s, %s);""").format(predictions_table.id)

    with connect(computation_db.connection.dsn) as connection:
        with connection.cursor() as cur:
            cur.executemany(query, data, returning=True)
            connection.commit()

    return Output(predictions_table, metadata=metadata)


def _inject_floors(
    ref: cityjson_index.PackageRef,
    feature_json: dict[str, Any],
    source_path: str,
    inferenced_floors: pd.DataFrame,
    party_walls_stage_dir: Path,
) -> tuple[str, dict[str, Any]]:
    """Inject b3_bouwlagen into a feature and return (tile_id, modified_feature)."""
    pand_id = ref.model_id
    attributes = feature_json["CityObjects"][pand_id]["attributes"]

    if pand_id in inferenced_floors.index:
        num_floors = int(inferenced_floors.loc[pand_id, "floors_int"])
        attributes["b3_bouwlagen"] = num_floors if num_floors <= 5 else None
    else:
        attributes["b3_bouwlagen"] = None

    source = Path(source_path)
    try:
        rel = source.relative_to(party_walls_stage_dir)
    except ValueError:
        rel = source
    tile_id = "/".join(rel.parts[:3])  # e.g. "10/434/716"
    return tile_id, feature_json


@asset(
    deps=[AssetKey(["party_walls", "building_surfaces"])],
)
def save_cjfiles(
    config: FloorsEstimationIOConfig,
    inferenced_floors: pd.DataFrame,
    party_walls_index: CityIndexResource,
    file_store: FileStoreResource,
) -> None:
    """Saves per-tile cityjsonseq files with b3_bouwlagen injected."""
    floors_estimation_dir = file_store.stage_dir("floors_estimation")
    party_walls_stage_dir = Path(party_walls_index.dataset_dir)
    logger.info(f"Saving to {floors_estimation_dir}")

    idx = open_ready_index(party_walls_index)
    total = idx.feature_bounds_summary().package_count

    # Resolve provenance in page order and pass it separately to the worker.
    tile_features: dict[str, list[FeatureRecord]] = defaultdict(list)
    with ThreadPoolExecutor(max_workers=config.concurrency) as pool:
        futures = {}
        for refs in iter_package_refs(idx, CHUNK_SIZE):
            paths = idx.package_source_paths(refs)
            for ref, source_path in zip(refs, paths, strict=True):
                feature_json = read_package_feature_json(idx, ref)
                future = pool.submit(
                    _inject_floors,
                    ref,
                    feature_json,
                    source_path,
                    inferenced_floors,
                    party_walls_stage_dir,
                )
                futures[future] = (ref.model_id, source_path)

        for future in as_completed(futures):
            try:
                tile_id, feature_json = future.result()
                _, source_path = futures[future]
                tile_features[tile_id].append(
                    FeatureRecord(feature=feature_json, source_path=source_path)
                )
            except Exception as e:  # pragma: no cover
                logger.error(f"Error processing feature: {e}")

    # Write per-tile cityjsonseq files
    files_written = 0
    for tile_id, features in tile_features.items():
        parts = tile_id.split("/")
        out_dir = floors_estimation_dir.joinpath(*parts)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{parts[-1]}.city.jsonl"
        write_feature_records_as_cityjsonseq(out_file, features)
        files_written += 1

    idx.close()
    logger.info(
        f"Saved {total} features across {files_written} tiles to {floors_estimation_dir}"
    )
