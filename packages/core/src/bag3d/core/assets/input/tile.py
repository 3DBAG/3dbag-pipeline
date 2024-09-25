import os

from dagster import AssetOut, multi_asset, Output
from pgutils import PostgresConnection
from psycopg.errors import OperationalError, UndefinedTable
from psycopg.sql import SQL

from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.resources import resource_defs
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA


@multi_asset(
    outs={"tiles": AssetOut(), "index": AssetOut()},
    required_resource_keys={"tyler", "db_connection"},
    code_version=resource_defs["tyler"].version("tyler-db"),
)
def reconstruction_input_tiles(context, reconstruction_input):
    """The reconstruction input partitioned into tiles where a tile is produced in about
    20 minutes."""
    quadtree_capacity = 1200000
    grid_cellsize = 250
    output_schema = RECONSTRUCTION_INPUT_SCHEMA
    primary_key = "fid"
    geometry_column = "geometrie"

    conn = context.resources.db_connection
    conn.send_query(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")

    # tiler-db creates two tables. output_schema.index and output_schema.tiles
    # The tiles table has 'tile_id' and 'boundary' columns.
    cmd = [
        "RUST_LOG=info",
        "{exe}",
        "--drop-existing",
        f"--qtree-capacity {quadtree_capacity}",
        f"--grid-cellsize {grid_cellsize}",
        f'--uri "{conn.dsn}"',
        f"--table {reconstruction_input}",
        f"--geometry-column {geometry_column}",
        f"--primary-key {primary_key}",
        f"--output-schema {output_schema}",
    ]
    context.resources.tyler.execute("tyler-db", " ".join(cmd))

    conn.send_query(f"ALTER TABLE {output_schema}.tiles ADD PRIMARY KEY (tile_id)")
    conn.send_query(
        f"CREATE INDEX tiles_boundary_idx ON {output_schema}.tiles USING gist (boundary)"
    )

    conn.send_query(
        f"ALTER TABLE {output_schema}.index ADD FOREIGN KEY ({primary_key}) REFERENCES {reconstruction_input} ({primary_key})"
    )
    conn.send_query(
        f"ALTER TABLE {output_schema}.index ADD FOREIGN KEY (tile_id) REFERENCES {output_schema}.tiles (tile_id)"
    )
    conn.send_query(
        f"CREATE INDEX index_tile_id_idx ON {output_schema}.index (tile_id)"
    )

    return Output(
        PostgresTableIdentifier(output_schema, "tiles"), output_name="tiles"
    ), Output(PostgresTableIdentifier(output_schema, "index"), output_name="index")


def get_tile_ids(schema: str, table_tiles: str, logger, wkt: str = None):
    """Get the input tile IDs from the database. If 'wkt' is provided, then get the
    tile IDs that intersect the wkt polygon. The SRID for the wkt is set to 28992."""
    if wkt:
        query = SQL(
            f"select tile_id from {schema}.{table_tiles} where st_intersects(st_geometryfromtext('SRID=28992;{wkt}'), boundary)"
        )
    else:
        query = SQL(f"select tile_id from {schema}.{table_tiles}")
    try:
        conn = PostgresConnection(
            port=int(os.environ.get("BAG3D_PG_PORT", 5432)),
            user=os.environ.get("BAG3D_PG_USER"),
            password=os.environ.get("BAG3D_PG_PASSWORD"),
            dbname=os.environ.get("BAG3D_PG_DATABASE"),
            host=os.environ.get("BAG3D_PG_HOST"),
        )
        tile_ids = [row[0] for row in conn.get_query(query)]
    except OperationalError:
        logger.error(
            "cannot establish database connection from the environment variables BAG3D_PG_*"
        )
        tile_ids = []
    except UndefinedTable:
        logger.error(f"tiles table {schema}.{table_tiles} does not exist")
        tile_ids = []
    return tile_ids
