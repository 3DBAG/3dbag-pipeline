from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import pairwise
from math import isclose
from pathlib import Path

from shapely.geometry import LineString, MultiLineString, Polygon
from shapely.geometry.polygon import orient

from bag3d.underpasses.edge_offset.linework import (
    build_polygon_from_edge_sets,
    read_multiline_feature,
)

type Point = tuple[float, float]
type PointKey = tuple[int, int]
type SegmentKey = tuple[PointKey, PointKey]


@dataclass(frozen=True, slots=True)
class BoundarySegment:
    ring_index: int
    segment_index: int
    start: Point
    end: Point
    is_movable: bool


@dataclass(frozen=True, slots=True)
class BoundaryRing:
    ring_index: int
    is_exterior: bool
    is_counter_clockwise: bool
    vertices: tuple[Point, ...]
    segments: tuple[BoundarySegment, ...]


@dataclass(frozen=True, slots=True)
class ClassifiedPolygon:
    polygon: Polygon
    rings: tuple[BoundaryRing, ...]


def classify_polygon_from_edge_geojson(
    *,
    movable_edges_path: Path,
    fixed_edges_path: Path,
    tolerance: float = 1e-6,
) -> ClassifiedPolygon:
    movable_edges = read_multiline_feature(movable_edges_path)
    fixed_edges = read_multiline_feature(fixed_edges_path)
    return classify_polygon_from_edge_sets(
        movable_edges=movable_edges,
        fixed_edges=fixed_edges,
        tolerance=tolerance,
    )


def classify_polygon_from_edge_sets(
    *,
    movable_edges: MultiLineString,
    fixed_edges: MultiLineString,
    tolerance: float = 1e-6,
) -> ClassifiedPolygon:
    if tolerance <= 0:
        raise ValueError("Classification tolerance must be greater than zero.")

    polygon = orient(
        build_polygon_from_edge_sets(
            movable_edges=movable_edges,
            fixed_edges=fixed_edges,
        ),
        sign=1.0,
    )

    movable_segments = explode_multiline(movable_edges)
    fixed_segments = explode_multiline(fixed_edges)
    movable_keys = Counter(
        _segment_key_from_linestring(segment, tolerance=tolerance)
        for segment in movable_segments
    )
    fixed_keys = Counter(
        _segment_key_from_linestring(segment, tolerance=tolerance)
        for segment in fixed_segments
    )

    rings: list[BoundaryRing] = []
    rings.append(
        _classify_ring(
            ring_index=0,
            coords=polygon.exterior.coords,
            is_exterior=True,
            movable_keys=movable_keys,
            fixed_keys=fixed_keys,
            tolerance=tolerance,
        )
    )
    for ring_index, interior in enumerate(polygon.interiors, start=1):
        rings.append(
            _classify_ring(
                ring_index=ring_index,
                coords=interior.coords,
                is_exterior=False,
                movable_keys=movable_keys,
                fixed_keys=fixed_keys,
                tolerance=tolerance,
            )
        )

    if movable_keys:
        raise ValueError(
            "Some movable input segments could not be matched to the polygon boundary."
        )
    if fixed_keys:
        raise ValueError(
            "Some fixed input segments could not be matched to the polygon boundary."
        )

    return ClassifiedPolygon(polygon=polygon, rings=tuple(rings))


def explode_multiline(
    multiline: MultiLineString | LineString,
) -> tuple[LineString, ...]:
    if isinstance(multiline, LineString):
        return _explode_linestring(multiline)

    segments: list[LineString] = []
    for linestring in multiline.geoms:
        segments.extend(_explode_linestring(linestring))
    return tuple(segments)


def _explode_linestring(linestring: LineString) -> tuple[LineString, ...]:
    coords = [(float(x), float(y)) for x, y in linestring.coords]
    if len(coords) < 2:
        return ()

    segments: list[LineString] = []
    for start, end in pairwise(coords):
        if _points_equal(start, end):
            continue
        segments.append(LineString([start, end]))
    return tuple(segments)


def _classify_ring(
    *,
    ring_index: int,
    coords: Iterable[tuple[float, ...]],
    is_exterior: bool,
    movable_keys: Counter[SegmentKey],
    fixed_keys: Counter[SegmentKey],
    tolerance: float,
) -> BoundaryRing:
    vertices = tuple((float(x), float(y)) for x, y in list(coords)[:-1])
    if len(vertices) < 3:
        raise ValueError("Polygon rings must contain at least three vertices.")

    segments: list[BoundarySegment] = []
    for segment_index, start in enumerate(vertices):
        end = vertices[(segment_index + 1) % len(vertices)]
        segment_key = _segment_key(start, end, tolerance=tolerance)
        movable_count = movable_keys[segment_key]
        fixed_count = fixed_keys[segment_key]

        if movable_count and fixed_count:
            raise ValueError(
                "Boundary segment matched both movable and fixed input linework."
            )
        if not movable_count and not fixed_count:
            raise ValueError(
                "Polygon boundary segment was not found in the supplied input linework."
            )

        is_movable = movable_count > 0
        if is_movable:
            _consume_segment_key(movable_keys, segment_key)
        else:
            _consume_segment_key(fixed_keys, segment_key)

        segments.append(
            BoundarySegment(
                ring_index=ring_index,
                segment_index=segment_index,
                start=start,
                end=end,
                is_movable=is_movable,
            )
        )

    return BoundaryRing(
        ring_index=ring_index,
        is_exterior=is_exterior,
        is_counter_clockwise=is_exterior,
        vertices=vertices,
        segments=tuple(segments),
    )


def _consume_segment_key(counter: Counter[SegmentKey], segment_key: SegmentKey) -> None:
    count = counter[segment_key]
    if count <= 1:
        del counter[segment_key]
        return
    counter[segment_key] = count - 1


def _segment_key_from_linestring(
    linestring: LineString, *, tolerance: float
) -> SegmentKey:
    coords = [(float(x), float(y)) for x, y in linestring.coords]
    return _segment_key(coords[0], coords[1], tolerance=tolerance)


def _segment_key(start: Point, end: Point, *, tolerance: float) -> SegmentKey:
    start_key = _point_key(start, tolerance=tolerance)
    end_key = _point_key(end, tolerance=tolerance)
    return (start_key, end_key) if start_key <= end_key else (end_key, start_key)


def _point_key(point: Point, *, tolerance: float) -> PointKey:
    return (
        round(point[0] / tolerance),
        round(point[1] / tolerance),
    )


def _points_equal(first: Point, second: Point, *, tolerance: float = 1e-12) -> bool:
    return isclose(first[0], second[0], abs_tol=tolerance) and isclose(
        first[1],
        second[1],
        abs_tol=tolerance,
    )
