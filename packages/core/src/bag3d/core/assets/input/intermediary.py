from dagster import asset, Output, AssetIn, get_dagster_logger, AutomationCondition
from psycopg.sql import SQL

from bag3d.common.utils.database import (
    create_schema,
    drop_table,
    load_sql,
    postgrestable_from_query,
)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.resources.database import DatabaseResource

INTERMEDIARY = "intermediary"
RECONSTRUCTION_INPUT_SCHEMA = "reconstruction_input"

logger = get_dagster_logger("input.intermediary")


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "top10nl_gebouw": AssetIn(key_prefix="top10nl"),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_kas_warenhuis(
    bag_pandactueelbestaand, top10nl_gebouw, computation_db: DatabaseResource
) -> Output[PostgresTableIdentifier]:
    """The BAG Pand labelled as greenhouse, warehouse (kas, warenhuis) using the
    TOP10NL."""
    create_schema(computation_db, RECONSTRUCTION_INPUT_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(
        RECONSTRUCTION_INPUT_SCHEMA, "bag_kas_warenhuis"
    )
    query = load_sql(
        query_params={
            "bag_cleaned": bag_pandactueelbestaand,
            "top10nl_gebouw": top10nl_gebouw,
            "new_table": new_table,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY (fid)").format(new_table.id)
    )
    return Output(new_table, metadata=metadata)


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_bag_overlap(
    bag_pandactueelbestaand, computation_db: DatabaseResource
) -> Output[PostgresTableIdentifier]:
    """The overlap between BAG polygons, in m2. For every object the
    total area of overlap is calculated."""
    create_schema(computation_db, RECONSTRUCTION_INPUT_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "bag_bag_overlap")
    query = load_sql(
        query_params={"bag_cleaned": bag_pandactueelbestaand, "new_table": new_table}
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY (fid)").format(new_table.id)
    )
    return Output(new_table, metadata=metadata)


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "bag_verblijfsobjectactueelbestaand": AssetIn(key_prefix="bag"),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_pand_vbo_views(
    bag_pandactueelbestaand,
    bag_verblijfsobjectactueelbestaand,
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Create views joining BAG pand with verblijfsobject (VBO) data.

    Creates three views in the reconstruction_input schema:

    - pand_vbo_single: pand with exactly 1 VBO with woonfunctie
    - pand_vbo_multi: pand with multiple VBOs with woonfunctie
    - pand_vbo_woonfunctie: all pand joined with VBOs with woonfunctie
    """
    create_schema(computation_db, RECONSTRUCTION_INPUT_SCHEMA, logger=logger)
    view_single = PostgresTableIdentifier(
        RECONSTRUCTION_INPUT_SCHEMA, "pand_vbo_single"
    )
    view_multi = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "pand_vbo_multi")
    view_woonfunctie = PostgresTableIdentifier(
        RECONSTRUCTION_INPUT_SCHEMA, "pand_vbo_woonfunctie"
    )
    query = load_sql(
        query_params={
            "view_single": view_single,
            "view_multi": view_multi,
            "view_woonfunctie": view_woonfunctie,
            "bag_pand": bag_pandactueelbestaand,
            "bag_vbo": bag_verblijfsobjectactueelbestaand,
        }
    )
    conn = computation_db.connection
    logger.info(conn.print_query(query))
    conn.send_query(query)
    return Output(
        view_single,
        metadata={
            "schema": RECONSTRUCTION_INPUT_SCHEMA,
            "views": "pand_vbo_single, pand_vbo_multi, pand_vbo_woonfunctie",
        },
    )


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "bag_pand_vbo_views": AssetIn(key_prefix=INTERMEDIARY),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_building_type(
    bag_pandactueelbestaand,
    bag_pand_vbo_views,
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Classify BAG buildings into dwelling types (woningtypen).

    Uses spatial clustering with a 0.1m buffer and adjacency counting to classify
    residential buildings into: vrijstaande woning, twee-onder-een-kap, hoekwoning,
    tussenwoning/geschakeld, appartement.

    Depends on the bag_pand_vbo_views asset for the pand_vbo_single and pand_vbo_multi views.
    """
    new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "woningtypen")
    drop_table(computation_db, new_table, logger=logger)
    pand_vbo_single = PostgresTableIdentifier(
        RECONSTRUCTION_INPUT_SCHEMA, "pand_vbo_single"
    )
    pand_vbo_multi = PostgresTableIdentifier(
        RECONSTRUCTION_INPUT_SCHEMA, "pand_vbo_multi"
    )
    query = load_sql(
        query_params={
            "new_table": new_table,
            "bag_pand": bag_pandactueelbestaand,
            "pand_vbo_single": pand_vbo_single,
            "pand_vbo_multi": pand_vbo_multi,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    return Output(new_table, metadata=metadata)


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "bgt_pandactueelbestaand": AssetIn(key_prefix="bgt"),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_bgt_join(
    bag_pandactueelbestaand,
    bgt_pandactueelbestaand,
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Spatial join of BAG Pand and BGT Pand, aggregating BGT polygon geometries per
    BAG building identification."""
    create_schema(computation_db, "bag", logger=logger)
    new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "bag_bgt_join")
    query = load_sql(
        query_params={
            "bag_pand": bag_pandactueelbestaand,
            "bgt_pand": bgt_pandactueelbestaand,
            "new_table": new_table,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    return Output(new_table, metadata=metadata)


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pandactueelbestaand": AssetIn(key_prefix="bag"),
        "bag_bgt_join": AssetIn(key_prefix=INTERMEDIARY),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_pand_filtered(
    bag_pandactueelbestaand,
    bag_bgt_join,
    computation_db: DatabaseResource,
) -> Output[PostgresTableIdentifier]:
    """Filtered BAG Pand table with problematic polygons removed.

    Removes:
    - BAG polygons with area > 1000 m2 that have no matching BGT geometry
    - BAG polygons where the BGT geometry area is less than 10% of the BAG geometry area
    """
    create_schema(computation_db, RECONSTRUCTION_INPUT_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(
        RECONSTRUCTION_INPUT_SCHEMA, "bag_pand_filtered"
    )
    query = load_sql(
        query_params={
            "bag_pand": bag_pandactueelbestaand,
            "bag_bgt_join": bag_bgt_join,
            "new_table": new_table,
        }
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY (fid)").format(new_table.id)
    )
    return Output(new_table, metadata=metadata)


@asset(
    key_prefix=INTERMEDIARY,
    ins={
        "bag_pand_filtered": AssetIn(key_prefix=INTERMEDIARY),
    },
    op_tags={"compute_kind": "sql"},
    automation_condition=AutomationCondition.eager(),
)
def bag_adjacency(
    bag_pand_filtered, computation_db: DatabaseResource
) -> Output[PostgresTableIdentifier]:
    """BAG polygon adjacency index.

    Stores one row per directed adjacency pair:
    (identificatie, adjacent_identificatie).

    Two polygons are adjacent when their geometries intersect or come within
    0.1 units of each other. Self-pairs are excluded.
    """
    create_schema(computation_db, RECONSTRUCTION_INPUT_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(RECONSTRUCTION_INPUT_SCHEMA, "bag_adjacency")
    query = load_sql(
        query_params={"bag_pand": bag_pand_filtered, "new_table": new_table}
    )
    metadata = postgrestable_from_query(computation_db, query, new_table, logger=logger)
    computation_db.connection.send_query(
        SQL(
            "ALTER TABLE {} ADD PRIMARY KEY (identificatie, adjacent_identificatie)"
        ).format(new_table.id)
    )
    return Output(new_table, metadata=metadata)
