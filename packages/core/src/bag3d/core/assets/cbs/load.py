"""Load assets for CBS data sources into PostgreSQL.

Loads CBS key figures and buurtkaart GeoPackage into the 'cbs' schema.
"""

from pathlib import Path

from dagster import asset, Output, get_dagster_logger, AutomationCondition
from psycopg import connect
from psycopg.sql import SQL, Identifier, Literal

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import GDALResource
from bag3d.common.utils.database import (
    create_schema,
    drop_table,
    postgrestable_metadata,
)
from bag3d.common.types import PostgresTableIdentifier
from bag3d.core.assets.cbs.download import CbsKeyFiguresConfig

logger = get_dagster_logger("cbs.load")

CBS_SCHEMA = "cbs"

PG_TYPE_MAP = {int: "INTEGER", float: "DOUBLE PRECISION", str: "TEXT", type(None): "TEXT"}


def _infer_pg_type(records: list[dict], col: str) -> str:
    """Infer PostgreSQL type from the first non-None value in a column."""
    for record in records:
        val = record.get(col)
        if val is not None:
            return PG_TYPE_MAP.get(type(val), "TEXT")
    return "TEXT"


def _load_records_to_postgres(
    computation_db: DatabaseResource,
    records: list[dict],
    table: PostgresTableIdentifier,
) -> None:
    """Load a list of dicts into a PostgreSQL table.

    Column types are inferred from Python types of the first non-None value
    per column. Data is inserted via psycopg executemany.
    """
    if not records:
        return

    conn = computation_db.connection
    headers = list(records[0].keys())

    col_idents = SQL(", ").join(
        Identifier(col) + SQL(f" {_infer_pg_type(records, col)}")
        for col in headers
    )
    create_q = SQL("CREATE TABLE {} ({})").format(table.id, col_idents)
    conn.send_query(create_q)

    col_placeholders = SQL(", ").join(SQL("%s") for _ in headers)
    insert_q = SQL("INSERT INTO {} ({}) VALUES ({})").format(
        table.id,
        SQL(", ").join(Identifier(c) for c in headers),
        col_placeholders,
    )
    with connect(conn.dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.executemany(
                insert_q,
                [tuple(record.get(c) for c in headers) for record in records],
            )
            pg_conn.commit()


@asset(
    op_tags={"compute_kind": "sql"}, automation_condition=AutomationCondition.eager()
)
def cbs_key_figures(
    config: CbsKeyFiguresConfig,
    computation_db: DatabaseResource,
    extract_cbs_key_figures,
) -> Output[PostgresTableIdentifier]:
    """CBS key figures (Kerncijfers wijken en buurten) loaded into PostgreSQL.

    Column types are inferred from the OData JSON response — integers remain
    INTEGER, floats become DOUBLE PRECISION, strings become TEXT.
    """
    create_schema(computation_db, CBS_SCHEMA, logger=logger)
    table_name = "key_figures_districts_neighbourhoods"
    table = PostgresTableIdentifier(CBS_SCHEMA, table_name)
    drop_table(computation_db, table, logger=logger)

    _load_records_to_postgres(computation_db, extract_cbs_key_figures, table)

    computation_db.connection.send_query(
        SQL('ALTER TABLE {} ADD PRIMARY KEY ("ID")').format(table.id)
    )
    computation_db.connection.send_query(
        SQL("COMMENT ON TABLE {} IS {}").format(
            table.id,
            Literal(
                f"CBS Key figures for districts and neighbourhoods {config.year}. "
                f"Source: https://opendata.cbs.nl"
            ),
        )
    )

    metadata = postgrestable_metadata(computation_db, table)
    return Output(table, metadata=metadata)


@asset(
    op_tags={"compute_kind": "sql"}, automation_condition=AutomationCondition.eager()
)
def cbs_buurten(
    computation_db: DatabaseResource,
    gdal: GDALResource,
    extract_cbs_buurtkaart,
) -> Output[PostgresTableIdentifier]:
    """CBS neighbourhood boundaries (buurten) loaded from GeoPackage into PostgreSQL.

    Loads the 'buurten' layer from the CBS Wijk- en buurtkaart GeoPackage.
    The geometry is in EPSG:28992 (Amersfoort / RD New).
    """
    create_schema(computation_db, CBS_SCHEMA, logger=logger)
    new_table = PostgresTableIdentifier(CBS_SCHEMA, "buurten")
    drop_table(computation_db, new_table, logger=logger)

    cmd = " ".join(
        [
            "{exe}",
            "--config PG_USE_COPY=YES",
            "-overwrite",
            "-nln {new_table}",
            "-lco UNLOGGED=ON",
            "-lco SPATIAL_INDEX=NONE",
            "-lco GEOMETRY_NAME=geom",
            '-f PostgreSQL PG:"{dsn}"',
            '"{local_path}" buurten',
        ]
    )

    result = gdal.runner.run(
        cmd,
        exe_name="ogr2ogr",
        kwargs={
            "new_table": new_table,
            "dsn": computation_db.connection.dsn,
        },
        local_path=extract_cbs_buurtkaart,
        logger=logger,
    )

    if not result.success:
        raise RuntimeError(f"ogr2ogr failed loading buurtkaart: {result.stderr}")

    # Add spatial index
    computation_db.connection.send_query(
        SQL("CREATE INDEX {} ON {} USING gist (geom)").format(
            Identifier("buurten_geom_idx"), new_table.id
        )
    )

    # Add table comment
    computation_db.connection.send_query(
        SQL("COMMENT ON TABLE {} IS {}").format(
            new_table.id,
            Literal(
                "CBS neighbourhood boundaries (buurten) from Wijk- en buurtkaart. "
                "Source: https://www.cbs.nl/nl-nl/dossier/nederland-regionaal/geografische-data"
            ),
        )
    )

    metadata = postgrestable_metadata(computation_db, new_table)
    return Output(new_table, metadata=metadata)
