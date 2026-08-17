import os

from bag3d.common.resources import tool_versions
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import TylerResource
from bag3d.common.types import PostgresTableIdentifier
from dagster import (
    AssetOut,
    AutomationCondition,
    Output,
    get_dagster_logger,
    multi_asset,
)
from pgutils import PostgresConnection
from psycopg import connect
from psycopg.errors import OperationalError, UndefinedTable
from psycopg.sql import SQL, Identifier, Literal

from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA

logger = get_dagster_logger("input.tile")


@multi_asset(
    outs={
        "tiles": AssetOut(automation_condition=AutomationCondition.eager()),
        "index": AssetOut(automation_condition=AutomationCondition.eager()),
    },
    code_version=tool_versions.get_version("tyler-db"),
)
def reconstruction_input_tiles(
    reconstruction_input, computation_db: DatabaseResource, tyler: TylerResource
) -> tuple[Output[PostgresTableIdentifier], Output[PostgresTableIdentifier]]:
    """The reconstruction input partitioned into tiles where a tile is produced in about
    20 minutes."""
    quadtree_capacity = 1200000
    grid_cellsize = 250
    output_schema = RECONSTRUCTION_INPUT_SCHEMA
    primary_key = "fid"
    geometry_column = "geometrie"

    conn = computation_db.connection
    conn.send_query(
        SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(output_schema))
    )

    with connect(conn.dsn) as database:
        row = database.execute(
            SQL("SELECT COUNT(*) FROM {}").format(reconstruction_input.id)
        ).fetchone()
    if row is None:
        raise RuntimeError("Could not read reconstruction input row count")
    row_count = row[0]
    if row_count == 0:
        tiles = Identifier(output_schema, "tiles")
        index = Identifier(output_schema, "index")
        conn.send_query(SQL("DROP TABLE IF EXISTS {}, {} CASCADE").format(index, tiles))
        conn.send_query(
            SQL(
                "CREATE TABLE {} (tile_id TEXT, boundary geometry(Polygon, 28992))"
            ).format(tiles)
        )
        conn.send_query(SQL("CREATE TABLE {} (fid BIGINT, tile_id TEXT)").format(index))
        return (
            Output(
                PostgresTableIdentifier(output_schema, "tiles"), output_name="tiles"
            ),
            Output(
                PostgresTableIdentifier(output_schema, "index"), output_name="index"
            ),
        )

    # todo: dirty hack just for now for removing sslmode, couz it's not implemented in tyler-db
    uri = conn.dsn.replace("sslmode=allow", "").strip()

    # tiler-db creates two tables. output_schema.index and output_schema.tiles
    # The tiles table has 'tile_id' and 'boundary' columns.
    cmd = [
        "RUST_LOG=info",
        "{exe}",
        "--drop-existing",
        f"--qtree-capacity {quadtree_capacity}",
        f"--grid-cellsize {grid_cellsize}",
        f'--uri "{uri}"',
        f"--table {reconstruction_input}",
        f"--geometry-column {geometry_column}",
        f"--primary-key {primary_key}",
        f"--output-schema {output_schema}",
    ]
    tyler.runner.run(
        " ".join(cmd),
        exe_name="tyler-db",
        logger=logger,
    )

    conn.send_query(
        SQL("ALTER TABLE {} ADD PRIMARY KEY (tile_id)").format(
            Identifier(output_schema, "tiles")
        )
    )
    conn.send_query(
        SQL("CREATE INDEX tiles_boundary_idx ON {} USING gist (boundary)").format(
            Identifier(output_schema, "tiles")
        )
    )

    conn.send_query(
        SQL("ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {} ({})").format(
            Identifier(output_schema, "index"),
            Identifier(primary_key),
            reconstruction_input.id,
            Identifier(primary_key),
        )
    )
    conn.send_query(
        SQL("ALTER TABLE {} ADD FOREIGN KEY (tile_id) REFERENCES {} (tile_id)").format(
            Identifier(output_schema, "index"),
            Identifier(output_schema, "tiles"),
        )
    )
    conn.send_query(
        SQL("CREATE INDEX index_tile_id_idx ON {} (tile_id)").format(
            Identifier(output_schema, "index")
        )
    )

    return Output(
        PostgresTableIdentifier(output_schema, "tiles"), output_name="tiles"
    ), Output(PostgresTableIdentifier(output_schema, "index"), output_name="index")


def get_tile_ids(schema: str, table_tiles: str, logger, wkt: str | None = None):
    """Get the input tile IDs from the database. If 'wkt' is provided, then get the
    tile IDs that intersect the wkt polygon. The SRID for the wkt is set to 28992."""
    if wkt:
        query = SQL(
            "SELECT tile_id FROM {} WHERE st_intersects(st_geometryfromtext({}), boundary)"
        ).format(Identifier(schema, table_tiles), Literal(f"SRID=28992;{wkt}"))
    else:
        query = SQL("SELECT tile_id FROM {}").format(Identifier(schema, table_tiles))
    try:
        conn = PostgresConnection(
            port=int(os.environ.get("BAG3D_PG_PORT", "5432")),
            user=os.environ.get("BAG3D_PG_USER"),
            password=os.environ.get("BAG3D_PG_PASSWORD"),
            dbname=os.environ.get("BAG3D_PG_DATABASE"),
            host=os.environ.get("BAG3D_PG_HOST"),
        )
        tile_ids = [row[0] for row in conn.get_query(query)]
    except OperationalError:
        logger.warning(
            "cannot establish database connection from the environment variables BAG3D_PG_*"
        )
        tile_ids = []
    except UndefinedTable:
        logger.warning(f"tiles table {schema}.{table_tiles} does not exist")
        tile_ids = []
    return tile_ids
