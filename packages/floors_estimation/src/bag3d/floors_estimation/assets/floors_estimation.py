import json
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path
from typing import Dict, Iterable, List

from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import (create_schema, load_sql,
                                         postgrestable_from_query)
from bag3d.common.utils.files import geoflow_crop_dir
from bag3d.floors_estimation.assets.Attributes import Attributes
from dagster import Output, asset
from psycopg import connect

SCHEMA = "floors_estimation"
CHUNK_SIZE = 1000


def extract_attributes_from_path(path: str, pand_id: str) -> Dict:
    with Path(path).open(encoding="utf-8", mode="r") as fo:
        feature_json = json.load(fo)
    attributes = feature_json[
        "CityObjects"][
            "NL.IMBAG.Pand." + pand_id]["attributes"]
    return attributes


def process_chunk(conn, chunk_files: List[str], chunk_id: int):
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

    query = """
        INSERT INTO floors_estimation.building_features_3dbag_test
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;"""

    with connect(conn.dsn) as connection:
        with connection.cursor() as cur:
            cur.executemany(query, data, returning=True)


def visit_directory(z_level: Path) -> Iterable[tuple[str, Path]]:
    for x_level in z_level.iterdir():
        for y_level in x_level.iterdir():
            for feature_path in y_level.iterdir():
                yield feature_path.with_suffix("").stem, feature_path


def features_file_index_generator(path_features: Path) \
                        -> Iterable[tuple[str, Path]]:
    dir_z = [d for d in path_features.iterdir()]
    with ThreadPoolExecutor() as executor:
        for g in executor.map(visit_directory, dir_z):
            for identificatie, path in g:
                yield identificatie, path


def make_chunks(data, SIZE=1000):
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
    reconstructed_with_party_walls_dir = \
        reconstructed_root_dir.parent.joinpath(
            "party_walls_features"
        )

    if not reconstructed_with_party_walls_dir.exists():
        reconstructed_with_party_walls_dir = Path(
            "/data/work/rypeters/bag_v20231008/crop_reconstruct"
        )
    res = dict(features_file_index_generator(
        reconstructed_with_party_walls_dir))
    context.log.info(res["NL.IMBAG.Pand.0664100000008035"])
    return res


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def extract_3DBAG_features(context, features_file_index: dict[str, Path]):
    """Extract 3DBAG features from the cityJSONL files on Gilfoyle.
    The 3DBAG data are extracted only for the buildings for which
    external features have already been extracted."""
    context.log.info("Extracting 3DBAG features.")
    table_name = "building_features_3dbag_test"
    new_table = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(query_params={"new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)

    chunks = list(make_chunks(features_file_index, CHUNK_SIZE))
    # Keep only the ones for the training data
    # for now only one tile in rotterdam
    pool = ThreadPoolExecutor(max_workers=8)
    with ThreadPoolExecutor(8) as pool:
        _ = {
            pool.submit(
                process_chunk,
                features_file_index,
                context.resources.db_connection,
                chunk,
                cid,
            ): cid
            for cid, chunk in enumerate(chunks)
        }

    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def extract_external_features(context) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features_external` table.
    In contains features from CBS, ESRI and BAG."""
    context.log.info("Extracting external features, from CBS, ESRI and BAG.")
    create_schema(context, context.resources.db_connection, SCHEMA)
    table_name = "building_features_external_test"
    new_table = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(query_params={"new_table": new_table,
                                   "table_name": table_name})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)


@asset(required_resource_keys={"db_connection"}, op_tags={"kind": "sql"})
def create_building_features_table(context) -> Output[PostgresTableIdentifier]:
    """Creates the `floors_estimation.building_features` table."""
    create_schema(context, context.resources.db_connection, SCHEMA)
    table_name = "building_features_test"
    new_table = PostgresTableIdentifier(SCHEMA, table_name)
    query = load_sql(query_params={"new_table": new_table})
    metadata = postgrestable_from_query(context, query, new_table)
    return Output(new_table, metadata=metadata)