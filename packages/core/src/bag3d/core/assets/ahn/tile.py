from concurrent.futures import ThreadPoolExecutor, as_completed

from dagster import asset, AssetKey, Output, Field, get_dagster_logger

from pgutils import PostgresTableIdentifier, inject_parameters
from psycopg.sql import SQL
from psycopg import Connection

from bag3d.common.utils.database import create_schema, load_sql
from bag3d.common.utils.geodata import wkt_from_bbox
from bag3d.core.assets.ahn.core import (
    generate_grid,
    ahn_dir,
    ahn_laz_dir,
)

import os

# The tile index bbox was computed from download_ahn_index(3, True)
PDOK_TILE_INDEX_BBOX = (13000, 306250, 279000, 616250)

logger = get_dagster_logger("ahn.tile")


@asset(
    description="Regular grid tile boundaries for partitioning LAZ files.",
    required_resource_keys={"db_connection"},
)
def regular_grid_200m(context):
    """Regular grid tile boundaries for partitioning LAZ files."""
    conn = context.resources.db_connection.connect
    new_schema = "ahn"
    create_schema(context, new_schema)
    new_table = PostgresTableIdentifier(new_schema, "regular_grid_200m")
    query = load_sql(query_params={"new_table": new_table})
    context.log.info(conn.print_query(query))
    conn.send_query(query)

    conn.send_query(
        SQL(
            "COMMENT ON TABLE {table} IS 'Regular grid tile boundaries for partitioning LAZ files.'"
        ),
        query_params={"table": new_table},
    )

    cellsize = 200
    bbox, nr_cells_x, nr_cells_y = generate_grid(
        bbox=PDOK_TILE_INDEX_BBOX, cellsize=cellsize
    )
    origin_x, origin_y = bbox[0:2]
    with Connection.connect(conn.dsn) as conn2:
        with conn2.cursor() as cur:
            for y_i in range(nr_cells_y):
                for x_i in range(nr_cells_x):
                    minx = origin_x + cellsize * x_i
                    miny = origin_y + cellsize * y_i
                    maxx = minx + cellsize
                    maxy = miny + cellsize
                    wkt = "SRID=28992;" + wkt_from_bbox((minx, miny, maxx, maxy))
                    query = inject_parameters(
                        SQL(
                            "INSERT INTO {table} (geom) VALUES(st_geomfromtext({wkt}))"
                        ),
                        {"table": new_table, "wkt": wkt},
                    )
                    cur.execute(query)
    conn.send_query(
        SQL("CREATE INDEX regular_grid_200m_geom_idx ON {table} USING gist (geom)"),
        query_params={"table": new_table},
    )
    return Output(
        new_table, metadata={"nr_tiles": nr_cells_x * nr_cells_y, "celllsize": cellsize}
    )


# ignore partitioninging
@asset(
    config_schema={
        "max_workers": Field(
            int,
            default_value=20,
            description="Passed on to the subprocess.ThreadPoolExecutor that calls "
            "las2las.",
        )
    },
    deps={AssetKey(["ahn", "metadata_ahn3"])},
    required_resource_keys={"file_store", "lastools", "db_connection"},
)
def laz_tiles_ahn3_200m(context, regular_grid_200m, metadata_table_ahn3):
    """AHN3 partitioned by a grid of 200m cells on the extent of the AHN PDOK tiles."""
    partition_laz_with_grid(
        context,
        metadata_table_ahn3,
        regular_grid_200m,
        ahn_version=3,
        cellsize=200,
        max_workers=context.op_execution_context.op_config["max_workers"],
    )


@asset(
    config_schema={
        "max_workers": Field(
            int,
            default_value=20,
            description="Passed on to the subprocess.ThreadPoolExecutor that calls "
            "las2las.",
        )
    },
    deps={AssetKey(["ahn", "metadata_ahn4"])},
    required_resource_keys={"file_store", "lastools", "db_connection"},
)
def laz_tiles_ahn4_200m(context, regular_grid_200m, metadata_table_ahn4):
    """AHN4 partitioned by a grid of 200m cells on the extent of the AHN PDOK tiles."""
    partition_laz_with_grid(
        context,
        metadata_table_ahn4,
        regular_grid_200m,
        ahn_version=4,
        cellsize=200,
        max_workers=context.op_execution_context.op_config["max_workers"],
    )


@asset(
    config_schema={
        "max_workers": Field(
            int,
            default_value=20,
            description="Passed on to the subprocess.ThreadPoolExecutor that calls "
            "las2las.",
        )
    },
    deps={AssetKey(["ahn", "metadata_ahn5"])},
    required_resource_keys={"file_store", "lastools", "db_connection"},
)
def laz_tiles_ahn5_200m(context, regular_grid_200m, metadata_table_ahn5):
    """AHN5 partitioned by a grid of 200m cells on the extent of the AHN PDOK tiles."""
    partition_laz_with_grid(
        context,
        metadata_table_ahn5,
        regular_grid_200m,
        ahn_version=5,
        cellsize=200,
        max_workers=context.op_execution_context.op_config["max_workers"],
    )


def partition_laz_with_grid(
    context, metadata_table_ahn, regular_grid_200m, ahn_version, cellsize, max_workers
):
    conn = context.resources.db_connection.connect
    query_params = {
        "grid_200m": regular_grid_200m,
        "metadata": metadata_table_ahn,
    }
    query = SQL("""
    SELECT g.id, ST_XMIN(g.geom) xmin, ST_YMIN(g.geom) ymin, array_agg(m.tile_id)
    FROM {grid_200m} AS g,
         {metadata} AS m
    WHERE ( ST_Contains(m.boundary, g.geom)
       OR ST_Overlaps(m.boundary, g.geom) )
    GROUP BY g.id;
    """)
    tile_ids = conn.get_query(query, query_params=query_params)
    out_dir = ahn_dir(
        context.resources.file_store.file_store.data_dir, ahn_version=ahn_version
    ).joinpath(f"tiles_{cellsize}m")
    out_dir.mkdir(exist_ok=True, parents=True)
    future_to_tile = {}
    failed = []
    ahn_path = ahn_laz_dir(
        context.resources.file_store.file_store.data_dir,
        ahn_version=ahn_version,
    )
    # Dictionary of all files in the AHN path
    files_in_ahn_path = {
        f[-9:-4].lower(): os.path.join(ahn_path, f)
        for f in os.listdir(ahn_path)
        if os.path.isfile(os.path.join(ahn_path, f))
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for tile, xmin, ymin, pdok_match in tile_ids:
            tile_size = cellsize
            out_file = out_dir / f"t_{tile}.laz"
            cmd = ["{exe}", "-v", "-i"]
            cmd.extend(str(files_in_ahn_path[t.lower()]) for t in pdok_match)
            cmd += [
                "-inside_tile",
                str(xmin),
                str(ymin),
                str(tile_size),
                "-merged",
                "-o",
                "{local_path}",
            ]
            future_to_tile[
                executor.submit(
                    context.resources.lastools.app.execute,
                    "las2las",
                    " ".join(cmd),
                    local_path=out_file,
                    silent=True,
                )
            ] = tile

        for i, future in enumerate(as_completed(future_to_tile)):
            tile = future_to_tile[future]
            try:
                _ = future.result()
            except Exception as e:
                failed.append(tile)
                logger.warning(f"Tile {tile} raised an exception: {e}")
    if len(failed) > 0:
        logger.error(f"Failed {len(failed)} tiles. Failed tiles: {failed}")
