"""Load assets for CBS data sources into PostgreSQL.

Loads CBS key figures CSVs, buurtkaart GeoPackage, and address mapping CSV
into the 'cbs' schema in PostgreSQL.
"""

import csv
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
from bag3d.core.assets.cbs.download import CbsAddressMappingConfig

logger = get_dagster_logger("cbs.load")

CBS_SCHEMA = "cbs"


def _load_csv_to_postgres(
    computation_db: DatabaseResource,
    csv_path: Path,
    table: PostgresTableIdentifier,
) -> None:
    """Load a CSV file into a PostgreSQL table.

    Reads the CSV header to dynamically create the table schema
    (all columns as TEXT), then uses psycopg's COPY for efficient loading.
    """
    conn = computation_db.connection

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

    # Create table with all TEXT columns
    columns_sql = ", ".join(f'"{col}" TEXT' for col in headers)
    create_q = SQL("CREATE TABLE {} ({})").format(table.id, SQL(columns_sql))
    conn.send_query(create_q)

    # COPY data from CSV
    copy_q = SQL(
        "COPY {} FROM STDIN WITH (FORMAT csv, HEADER true, QUOTE '\"')"
    ).format(table.id)
    with connect(conn.dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            with open(csv_path, "r", encoding="utf-8") as f:
                with cur.copy(copy_q.as_string(pg_conn)) as copy:
                    for line in f:
                        copy.write(line)


@asset(
    op_tags={"compute_kind": "sql"}, automation_condition=AutomationCondition.eager()
)
def cbs_key_figures(
    computation_db: DatabaseResource,
    extract_cbs_key_figures,
) -> Output[list[PostgresTableIdentifier]]:
    """CBS key figures (Kerncijfers wijken en buurten) loaded into PostgreSQL.

    Creates one table in the 'cbs' schema, e.g.
    cbs.key_figures_districts_neighbourhoods.
    """
    create_schema(computation_db, CBS_SCHEMA, logger=logger)
    tables: list[PostgresTableIdentifier] = []
    metadata: dict = {}

    for year, csv_path in extract_cbs_key_figures.items():
        table_name = f"key_figures_districts_neighbourhoods"
        new_table = PostgresTableIdentifier(CBS_SCHEMA, table_name)
        drop_table(computation_db, new_table, logger=logger)

        _load_csv_to_postgres(computation_db, csv_path, new_table)

        # Add primary key
        computation_db.connection.send_query(
            SQL('ALTER TABLE {} ADD PRIMARY KEY ("ID")').format(new_table.id)
        )
        # Add table comment
        computation_db.connection.send_query(
            SQL("COMMENT ON TABLE {} IS {}").format(
                new_table.id,
                Literal(
                    f"CBS Key figures for districts and neighbourhoods {year}. "
                    f"Source: https://opendata.cbs.nl"
                ),
            )
        )

        tbl_metadata = postgrestable_metadata(computation_db, new_table)
        metadata.update({f"{k} [{year}]": v for k, v in tbl_metadata.items()})
        tables.append(new_table)

    return Output(tables, metadata=metadata)


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
