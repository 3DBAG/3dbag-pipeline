"""Database orchestration for classified boundaries and selective offsets."""

from collections import Counter
from collections.abc import Generator
from functools import partial
from typing import Any

from bag3d.common.types import PostgresTableIdentifier
from psycopg import connect
from psycopg.sql import SQL, Composed, Identifier
from shapely import from_wkb, to_wkb
from shapely.errors import GEOSException

from bag3d.underpasses.edge_classification.edge_classifier import (
    classify_edges_for_underpass,
)
from bag3d.underpasses.edge_offset.linework import merge_multiline_geometries
from bag3d.underpasses.edge_offset.offset_linework import (
    GeometryOffsetError,
    InvalidInputPolygonError,
    offset_polygon_from_classified_polygon,
)
from bag3d.underpasses.edge_offset.rings import classify_polygon_from_edge_sets
from bag3d.underpasses.workflow import ordered_results


def read_chunks(
    dsn: str, query: Composed, size: int
) -> Generator[list[Any], None, None]:
    with connect(dsn) as conn, conn.cursor(name="underpass_edges") as cur:
        cur.execute(query)
        while rows := cur.fetchmany(size):
            yield rows


def classify_chunk(rows, *, grid_size: float, snap_tolerance: float):
    result = []
    for uid, identificatie, geometry, bgt, neighbours in rows:
        if bgt is None:
            raise ValueError(f"Missing BGT footprint for underpass {uid}")
        classified = classify_edges_for_underpass(
            uid,
            identificatie,
            from_wkb(geometry),
            from_wkb(bgt),
            [from_wkb(wkb) for wkb in neighbours],
            grid_size,
            snap_tolerance,
        )
        for kind in ("interior", "exterior", "shared"):
            for edge in getattr(classified, f"{kind}_edges"):
                if not edge.is_empty:
                    result.append((uid, identificatie, kind, to_wkb(edge)))
    return len(rows), result, []


def offset_chunk(rows, *, offset_distance: float, tolerance: float):
    successes, skipped = [], []
    for uid, identificatie, kinds, geometries in rows:
        movable = merge_multiline_geometries(
            *[
                from_wkb(wkb)
                for kind, wkb in zip(kinds, geometries, strict=True)
                if kind == "exterior"
            ]
        )
        fixed = merge_multiline_geometries(
            *[
                from_wkb(wkb)
                for kind, wkb in zip(kinds, geometries, strict=True)
                if kind in {"interior", "shared"}
            ]
        )
        reason = None
        if movable.is_empty:
            reason = "no_movable_edges"
        else:
            try:
                classified = classify_polygon_from_edge_sets(
                    movable_edges=movable,
                    fixed_edges=fixed,
                    tolerance=tolerance,
                )
                polygon = offset_polygon_from_classified_polygon(
                    classified,
                    distance=offset_distance,
                    tolerance=tolerance,
                    strategy="boolean_patch",
                )
                successes.append((identificatie, uid, offset_distance, to_wkb(polygon)))
            except InvalidInputPolygonError:
                reason = "invalid_input_polygon"
            except (GeometryOffsetError, GEOSException):
                reason = "geometry_offset_failed"
            except ValueError as error:
                reason = (
                    "edge_matching_failed"
                    if "boundary" in str(error)
                    else "value_error"
                )
        if reason:
            skipped.append((identificatie, uid, reason))
    return len(rows), successes, skipped


def classification_query(geometries, joined, adjacency):
    return SQL("""
        SELECT g.underpass_id, g.identificatie, ST_AsBinary(g.geom),
               ST_AsBinary(b.bgt_geometrie),
               ARRAY(SELECT ST_AsBinary(n.bgt_geometrie)
                     FROM {adjacency} a JOIN {joined} n
                       ON n.identificatie = a.adjacent_identificatie
                     WHERE a.identificatie = g.identificatie
                       AND NOT ST_IsEmpty(n.bgt_geometrie)
                     ORDER BY a.adjacent_identificatie)
        FROM {geometries} g LEFT JOIN {joined} b USING (identificatie)
        ORDER BY g.underpass_id
    """).format(geometries=geometries.id, joined=joined.id, adjacency=adjacency.id)


def offset_query(geometries, edges):
    return SQL("""
        SELECT g.underpass_id, g.identificatie,
               ARRAY(SELECT e.edge_type FROM {edges} e
                     WHERE e.underpass_id=g.underpass_id AND e.identificatie=g.identificatie
                     ORDER BY e.edge_id),
               ARRAY(SELECT ST_AsBinary(e.geom) FROM {edges} e
                     WHERE e.underpass_id=g.underpass_id AND e.identificatie=g.identificatie
                     ORDER BY e.edge_id)
        FROM {geometries} g ORDER BY g.underpass_id
    """).format(geometries=geometries.id, edges=edges.id)


def run_edges(
    dsn: str,
    query: Composed,
    target: PostgresTableIdentifier,
    *,
    concurrency: int,
    chunk_size: int,
    settings: dict,
    skipped: PostgresTableIdentifier | None = None,
) -> dict:
    """Atomically rebuild a classified-edge table or both offset output tables."""
    stats = {"processed": 0, "written": 0, "skipped": 0}
    reasons: Counter[str] = Counter()
    if skipped is None:
        columns = """edge_id SERIAL PRIMARY KEY, underpass_id INTEGER NOT NULL,
            identificatie TEXT NOT NULL,
            edge_type TEXT NOT NULL CHECK (edge_type IN ('interior','exterior','shared')),
            geom geometry(LineString,28992) NOT NULL"""
        insert_columns = "underpass_id, identificatie, edge_type, geom"
        function = partial(classify_chunk, **settings)
    else:
        columns = """identificatie TEXT NOT NULL, underpass_id INTEGER NOT NULL,
            offset_distance DOUBLE PRECISION NOT NULL, geom geometry(Polygon,28992) NOT NULL,
            PRIMARY KEY (identificatie, underpass_id)"""
        insert_columns = "identificatie, underpass_id, offset_distance, geom"
        function = partial(offset_chunk, **settings)
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SQL("DROP TABLE IF EXISTS {}").format(target.id))
        cur.execute(SQL("CREATE TABLE {} ({})").format(target.id, SQL(columns)))
        if skipped is not None:
            cur.execute(SQL("DROP TABLE IF EXISTS {}").format(skipped.id))
            cur.execute(
                SQL("""CREATE TABLE {} (
                identificatie TEXT NOT NULL, underpass_id INTEGER NOT NULL,
                skip_reason TEXT NOT NULL, skipped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (identificatie, underpass_id))""").format(skipped.id)
            )
        chunks = read_chunks(dsn, query, chunk_size)
        try:
            for processed, rows, exclusions in ordered_results(
                function, chunks, concurrency
            ):
                if rows:
                    cur.executemany(
                        SQL("""INSERT INTO {} ({})
                        VALUES (%s,%s,%s,ST_GeomFromWKB(%s,28992))""").format(
                            target.id, SQL(insert_columns)
                        ),
                        rows,
                    )
                if exclusions and skipped is not None:
                    cur.executemany(
                        SQL("""INSERT INTO {} (identificatie,underpass_id,skip_reason)
                        VALUES (%s,%s,%s)""").format(skipped.id),
                        exclusions,
                    )
                stats["processed"] += processed
                stats["written"] += len(rows)
                stats["skipped"] += len(exclusions)
                reasons.update(row[2] for row in exclusions)
        finally:
            chunks.close()
        for table in [target, *([skipped] if skipped is not None else [])]:
            for column in ("identificatie", "underpass_id"):
                cur.execute(
                    SQL("CREATE INDEX {} ON {} ({})").format(
                        Identifier(f"{table.table.str}_{column}_idx"),
                        table.id,
                        Identifier(column),
                    )
                )
        cur.execute(
            SQL("CREATE INDEX {} ON {} USING gist (geom)").format(
                Identifier(f"{target.table.str}_geom_idx"), target.id
            )
        )
        if skipped is None:
            cur.execute(
                SQL("CREATE INDEX {} ON {} (edge_type)").format(
                    Identifier(f"{target.table.str}_edge_type_idx"), target.id
                )
            )
    return {**stats, "skip_reasons": dict(reasons)}
