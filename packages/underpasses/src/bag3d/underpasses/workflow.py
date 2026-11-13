"""Bounded, ordered processing of the experimental detector's geometry stages."""

from collections import deque
from collections.abc import Callable, Generator, Iterable, Iterator
from concurrent.futures import Future, ProcessPoolExecutor
from functools import partial
from itertools import islice
from typing import Literal

from bag3d.common.types import PostgresTableIdentifier
from psycopg import connect
from psycopg.sql import SQL, Composed, Identifier
from shapely import from_wkb, to_wkb
from shapely.geometry import MultiPolygon, Polygon

from bag3d.underpasses.geometry_ops import double_buffer_filter, dump_multi_to_polygons
from bag3d.underpasses.pipeline import (
    compute_bag_minus_bgt,
    compute_snapped_differences,
)

Polygonal = Polygon | MultiPolygon
Record = tuple[str, Polygonal, Polygonal | None]
Stage = Literal["bag_minus_bgt", "snapped_differences", "geometries"]


def ordered_results[T, R](
    function: Callable[[T], R], chunks: Iterable[T], concurrency: int
) -> Iterator[R]:
    """Yield in input order, keeping at most twice the worker count in flight."""
    if concurrency == 1:
        yield from map(function, chunks)
        return
    iterator = iter(chunks)
    with ProcessPoolExecutor(max_workers=concurrency) as executor:
        pending: deque[Future[R]] = deque(
            executor.submit(function, chunk)
            for chunk in islice(iterator, 2 * concurrency)
        )
        while pending:
            yield pending.popleft().result()
            for chunk in islice(iterator, 1):
                pending.append(executor.submit(function, chunk))


def read_chunks(
    dsn: str, query: Composed, chunk_size: int
) -> Generator[list[Record], None, None]:
    """Stream WKB input through a server-side cursor, ordered by BAG identifier."""
    with connect(dsn) as connection, connection.cursor(name="underpasses_input") as cur:
        cur.execute(query)
        while rows := cur.fetchmany(chunk_size):
            yield [
                (identificatie, from_wkb(first), from_wkb(second))
                for identificatie, first, second in rows
            ]


def process_chunk(
    rows: list[Record], *, stage: Stage, buffer_distance: float, snap_tolerance: float
) -> tuple[int, list[tuple[str, Polygonal]]]:
    """Apply the original geometry operations without database access in workers."""
    result: list[tuple[str, Polygonal]] = []
    for identificatie, first, second in rows:
        if stage == "geometries":
            for polygon in dump_multi_to_polygons(first):
                # Filtering tests the buffered polygon but retains the original shape.
                if not double_buffer_filter(polygon, buffer_distance).is_empty:
                    result.append((identificatie, polygon))
            continue
        if second is None:
            raise ValueError(f"Missing BGT geometry for {identificatie}")
        if stage == "bag_minus_bgt":
            geometry = compute_bag_minus_bgt(first, second, buffer_distance)
        else:
            geometry = compute_snapped_differences(first, second, snap_tolerance)
        if not geometry.is_empty:
            result.append((identificatie, geometry))
    return len(rows), result


def input_query(
    stage: Stage,
    source: PostgresTableIdentifier,
    bag_bgt_join: PostgresTableIdentifier | None = None,
) -> Composed:
    """Build queries from asset-provided table identifiers rather than fixed sources."""
    if stage == "bag_minus_bgt":
        return SQL("""
            SELECT identificatie, ST_AsBinary(bag_geometrie), ST_AsBinary(bgt_geometrie)
            FROM {} ORDER BY identificatie
        """).format(source.id)
    if stage == "snapped_differences":
        if bag_bgt_join is None:
            raise ValueError("The snapping stage requires the BAG-BGT join table")
        return SQL("""
            SELECT source.identificatie,
                   ST_AsBinary(joined.bag_geometrie), ST_AsBinary(joined.bgt_geometrie)
            FROM {} source JOIN {} joined USING (identificatie)
            ORDER BY source.identificatie
        """).format(source.id, bag_bgt_join.id)
    return SQL("""
        SELECT identificatie, ST_AsBinary(geom), NULL::bytea
        FROM {} ORDER BY identificatie
    """).format(source.id)


def run_geometry_stage(
    dsn: str,
    stage: Stage,
    source: PostgresTableIdentifier,
    target: PostgresTableIdentifier,
    *,
    concurrency: int,
    chunk_size: int,
    buffer_distance: float,
    snap_tolerance: float,
    bag_bgt_join: PostgresTableIdentifier | None = None,
) -> dict[str, int]:
    """Replace a stage table atomically; any worker/read/write failure rolls back."""
    query = input_query(stage, source, bag_bgt_join)
    function = partial(
        process_chunk,
        stage=stage,
        buffer_distance=buffer_distance,
        snap_tolerance=snap_tolerance,
    )
    final = stage == "geometries"
    geometry_type = SQL("Polygon" if final else "MultiPolygon")
    columns = SQL("underpass_id INTEGER PRIMARY KEY,") if final else SQL("")
    stats = {"processed": 0, "written": 0}
    # Workers only compute geometry. The parent owns a single write transaction,
    # including DDL, so errors preserve the last successful materialization.
    with connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(SQL("DROP TABLE IF EXISTS {}").format(target.id))
        cursor.execute(
            SQL("""
                CREATE TABLE {} (
                    {} identificatie TEXT NOT NULL,
                    geom geometry({}, 28992) NOT NULL
                )
            """).format(target.id, columns, geometry_type)
        )
        if final:
            insert = SQL("""
                INSERT INTO {} (underpass_id, identificatie, geom)
                VALUES (%s, %s, ST_GeomFromWKB(%s, 28992))
            """).format(target.id)
        else:
            insert = SQL("""
                INSERT INTO {} (identificatie, geom)
                VALUES (%s, ST_Multi(ST_GeomFromWKB(%s, 28992)))
            """).format(target.id)
        chunks = read_chunks(dsn, query, chunk_size)
        try:
            for processed, rows in ordered_results(function, chunks, concurrency):
                if final:
                    data = [
                        (stats["written"] + i, identificatie, to_wkb(geometry))
                        for i, (identificatie, geometry) in enumerate(rows, 1)
                    ]
                else:
                    data = [
                        (identificatie, to_wkb(geometry))
                        for identificatie, geometry in rows
                    ]
                if data:
                    cursor.executemany(insert, data)
                stats["processed"] += processed
                stats["written"] += len(rows)
        finally:
            chunks.close()
        cursor.execute(
            SQL("CREATE INDEX {} ON {} (identificatie)").format(
                Identifier(f"{target.table.str}_identificatie_idx"), target.id
            )
        )
        if final:
            cursor.execute(
                SQL("CREATE INDEX {} ON {} USING gist (geom)").format(
                    Identifier(f"{target.table.str}_geom_idx"), target.id
                )
            )
    return stats
