from dataclasses import dataclass
from math import isclose, sqrt
from pathlib import Path

from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry import Point as GeometryPoint
from shapely.geometry.base import BaseGeometry
from shapely.geometry.polygon import orient

from bag3d.underpasses.edge_offset.geojson import Feature, write_feature_collection
from bag3d.underpasses.edge_offset.rings import (
    BoundaryRing,
    BoundarySegment,
    ClassifiedPolygon,
    Point,
    classify_polygon_from_edge_geojson,
)

_DEFAULT_MITER_LIMIT = 10.0


@dataclass(frozen=True, slots=True)
class _OffsetLine:
    point: Point
    direction: Point


@dataclass(frozen=True, slots=True)
class _MovableChain:
    ring_index: int
    segment_indices: tuple[int, ...]
    vertices: tuple[Point, ...]
    previous_segment_index: int | None
    next_segment_index: int | None
    is_full_ring: bool


@dataclass(frozen=True, slots=True)
class _ChainPatch:
    patch: Polygon
    sample_point: Point


class InvalidInputPolygonError(ValueError):
    """Raised when assembled input linework does not form a valid polygon."""


class GeometryOffsetError(ValueError):
    """Raised when a requested partial offset cannot be resolved safely."""


def offset_polygon_from_edge_geojson(
    *,
    movable_edges_path: Path,
    fixed_edges_path: Path,
    distance: float,
    output_path: Path | None = None,
    tolerance: float = 1e-6,
    strategy: str = "boolean_patch",
) -> Polygon:
    classified_polygon = classify_polygon_from_edge_geojson(
        movable_edges_path=movable_edges_path,
        fixed_edges_path=fixed_edges_path,
        tolerance=tolerance,
    )
    polygon = offset_polygon_from_classified_polygon(
        classified_polygon,
        distance=distance,
        tolerance=tolerance,
        strategy=strategy,
    )
    if output_path is not None:
        write_feature_collection(
            [
                Feature(
                    geometry=polygon,
                    properties={
                        "offset_distance": distance,
                        "strategy": strategy,
                    },
                )
            ],
            path=output_path,
        )
    return polygon


def offset_polygon_from_classified_polygon(
    classified_polygon: ClassifiedPolygon,
    *,
    distance: float,
    tolerance: float = 1e-6,
    strategy: str = "boolean_patch",
) -> Polygon:
    if tolerance <= 0:
        raise ValueError("Offset tolerance must be greater than zero.")

    original_polygon = _validate_input_polygon(
        classified_polygon.polygon,
        tolerance=tolerance,
    )
    if isclose(distance, 0.0, abs_tol=tolerance):
        return original_polygon

    if strategy == "boolean_patch":
        updated = _offset_polygon_with_boolean_patches(
            classified_polygon,
            distance=distance,
            tolerance=tolerance,
        )
        return _normalize_offset_polygon_result(
            updated,
            original_polygon=original_polygon,
            tolerance=tolerance,
        )

    if strategy == "linework":
        updated = _offset_polygon_with_linework(
            classified_polygon,
            distance=distance,
            tolerance=tolerance,
        )
        return _normalize_offset_polygon_result(
            updated,
            original_polygon=original_polygon,
            tolerance=tolerance,
        )

    raise ValueError("Unknown offset strategy. Expected 'boolean_patch' or 'linework'.")


def _offset_polygon_with_boolean_patches(
    classified_polygon: ClassifiedPolygon,
    *,
    distance: float,
    tolerance: float,
) -> Polygon:
    chains_by_ring = [_build_movable_chains(ring) for ring in classified_polygon.rings]
    if not any(chains_by_ring):
        return classified_polygon.polygon

    if any(chain.is_full_ring for chains in chains_by_ring for chain in chains):
        return _offset_polygon_with_linework(
            classified_polygon,
            distance=distance,
            tolerance=tolerance,
        )

    polygon = classified_polygon.polygon
    for ring, chains in zip(classified_polygon.rings, chains_by_ring, strict=True):
        for chain in chains:
            chain_patch = _build_chain_patch(
                ring=ring,
                chain=chain,
                distance=distance,
                tolerance=tolerance,
            )
            if chain_patch is None:
                continue
            polygon = _apply_chain_patch(
                polygon=polygon,
                original_polygon=classified_polygon.polygon,
                chain_patch=chain_patch,
                tolerance=tolerance,
            )

    return polygon


def _offset_polygon_with_linework(
    classified_polygon: ClassifiedPolygon,
    *,
    distance: float,
    tolerance: float,
) -> Polygon:
    rebuilt_rings = [
        _offset_ring_with_support_lines(
            ring,
            distance=distance,
            tolerance=tolerance,
        )
        for ring in classified_polygon.rings
    ]
    shell = rebuilt_rings[0]
    holes = rebuilt_rings[1:]
    return Polygon(shell=shell, holes=holes)


def _offset_ring_with_support_lines(
    ring: BoundaryRing,
    *,
    distance: float,
    tolerance: float,
) -> list[Point]:
    if len(ring.vertices) < 3:
        raise GeometryOffsetError("Polygon rings must contain at least three vertices.")

    lines = [
        _build_offset_line(
            start=segment.start,
            end=segment.end,
            distance=distance if segment.is_movable else 0.0,
            is_counter_clockwise=ring.is_counter_clockwise,
        )
        for segment in ring.segments
    ]

    rebuilt_vertices: list[Point] = []
    for index in range(len(lines)):
        previous_index = index - 1
        effective_distance = (
            distance
            if ring.segments[previous_index].is_movable
            or ring.segments[index].is_movable
            else 0.0
        )
        _extend_unique_points(
            rebuilt_vertices,
            _resolve_join_vertices(
                original_vertex=ring.vertices[index],
                previous_line=lines[previous_index],
                current_line=lines[index],
                fallback_points=(
                    _project_point_onto_line(
                        ring.segments[previous_index].end,
                        lines[previous_index],
                    ),
                    _project_point_onto_line(
                        ring.segments[index].start,
                        lines[index],
                    ),
                ),
                distance=effective_distance,
                tolerance=tolerance,
            ),
            tolerance=tolerance,
        )

    if len(rebuilt_vertices) < 3:
        raise GeometryOffsetError("Offset ring collapsed below three vertices.")

    return rebuilt_vertices


def _build_movable_chains(ring: BoundaryRing) -> tuple[_MovableChain, ...]:
    movable_count = sum(1 for segment in ring.segments if segment.is_movable)
    if movable_count == 0:
        return ()

    segment_count = len(ring.segments)
    if movable_count == segment_count:
        return (
            _MovableChain(
                ring_index=ring.ring_index,
                segment_indices=tuple(range(segment_count)),
                vertices=ring.vertices + (ring.vertices[0],),
                previous_segment_index=None,
                next_segment_index=None,
                is_full_ring=True,
            ),
        )

    chains: list[_MovableChain] = []
    for index, segment in enumerate(ring.segments):
        previous_segment = ring.segments[index - 1]
        if not segment.is_movable or previous_segment.is_movable:
            continue

        segment_indices: list[int] = []
        vertices = [segment.start]
        current_index = index
        while ring.segments[current_index].is_movable:
            current_segment = ring.segments[current_index]
            segment_indices.append(current_index)
            vertices.append(current_segment.end)
            current_index = (current_index + 1) % segment_count

        chains.append(
            _MovableChain(
                ring_index=ring.ring_index,
                segment_indices=tuple(segment_indices),
                vertices=tuple(vertices),
                previous_segment_index=(index - 1) % segment_count,
                next_segment_index=current_index,
                is_full_ring=False,
            )
        )

    return tuple(chains)


def _build_chain_patch(
    *,
    ring: BoundaryRing,
    chain: _MovableChain,
    distance: float,
    tolerance: float,
) -> _ChainPatch | None:
    if (
        chain.is_full_ring
        or chain.previous_segment_index is None
        or chain.next_segment_index is None
    ):
        raise GeometryOffsetError(
            "Boolean patching requires fixed edges on both sides of a movable chain."
        )

    previous_segment = ring.segments[chain.previous_segment_index]
    next_segment = ring.segments[chain.next_segment_index]
    movable_segments = [ring.segments[index] for index in chain.segment_indices]
    if not movable_segments:
        return None

    previous_line = _build_offset_line(
        start=previous_segment.start,
        end=previous_segment.end,
        distance=0.0,
        is_counter_clockwise=ring.is_counter_clockwise,
    )
    shifted_lines = [
        _build_offset_line(
            start=segment.start,
            end=segment.end,
            distance=distance,
            is_counter_clockwise=ring.is_counter_clockwise,
        )
        for segment in movable_segments
    ]
    next_line = _build_offset_line(
        start=next_segment.start,
        end=next_segment.end,
        distance=0.0,
        is_counter_clockwise=ring.is_counter_clockwise,
    )

    replacement_vertices = _build_replacement_vertices(
        chain_vertices=chain.vertices,
        movable_segments=tuple(movable_segments),
        shifted_lines=tuple(shifted_lines),
        previous_line=previous_line,
        next_line=next_line,
        distance=distance,
        tolerance=tolerance,
    )

    patch = Polygon([*chain.vertices, *reversed(replacement_vertices)])
    if patch.is_empty or patch.area <= tolerance:
        return None
    if not patch.is_valid:
        patch = _normalize_polygon_result(patch.buffer(0), tolerance=tolerance)

    sample_point = _build_patch_sample_point(
        segment=movable_segments[0],
        distance=distance,
        is_counter_clockwise=ring.is_counter_clockwise,
    )
    return _ChainPatch(patch=patch, sample_point=sample_point)


def _build_replacement_vertices(
    *,
    chain_vertices: tuple[Point, ...],
    movable_segments: tuple[BoundarySegment, ...],
    shifted_lines: tuple[_OffsetLine, ...],
    previous_line: _OffsetLine,
    next_line: _OffsetLine,
    distance: float,
    tolerance: float,
) -> tuple[Point, ...]:
    if not shifted_lines:
        raise GeometryOffsetError("Movable chains must contain at least one segment.")

    replacement_vertices: list[Point] = []
    _extend_unique_points(
        replacement_vertices,
        _resolve_join_vertices(
            original_vertex=chain_vertices[0],
            previous_line=previous_line,
            current_line=shifted_lines[0],
            fallback_points=(
                _project_point_onto_line(chain_vertices[0], shifted_lines[0]),
            ),
            distance=distance,
            tolerance=tolerance,
        ),
        tolerance=tolerance,
    )

    for index in range(1, len(shifted_lines)):
        _extend_unique_points(
            replacement_vertices,
            _resolve_join_vertices(
                original_vertex=chain_vertices[index],
                previous_line=shifted_lines[index - 1],
                current_line=shifted_lines[index],
                fallback_points=(
                    _project_point_onto_line(
                        movable_segments[index - 1].end,
                        shifted_lines[index - 1],
                    ),
                    _project_point_onto_line(
                        movable_segments[index].start,
                        shifted_lines[index],
                    ),
                ),
                distance=distance,
                tolerance=tolerance,
            ),
            tolerance=tolerance,
        )

    _extend_unique_points(
        replacement_vertices,
        _resolve_join_vertices(
            original_vertex=chain_vertices[-1],
            previous_line=shifted_lines[-1],
            current_line=next_line,
            fallback_points=(
                _project_point_onto_line(chain_vertices[-1], shifted_lines[-1]),
            ),
            distance=distance,
            tolerance=tolerance,
        ),
        tolerance=tolerance,
    )

    if len(replacement_vertices) < 2:
        raise GeometryOffsetError("Replacement chain collapsed below two vertices.")

    return tuple(replacement_vertices)


def _apply_chain_patch(
    *,
    polygon: Polygon,
    original_polygon: Polygon,
    chain_patch: _ChainPatch,
    tolerance: float,
) -> Polygon:
    sample_point = GeometryPoint(chain_patch.sample_point)
    if original_polygon.covers(sample_point):
        updated_geometry = polygon.difference(chain_patch.patch)
    else:
        updated_geometry = polygon.union(chain_patch.patch)

    return _normalize_polygon_result(updated_geometry, tolerance=tolerance)


def _build_offset_line(
    *,
    start: Point,
    end: Point,
    distance: float,
    is_counter_clockwise: bool,
) -> _OffsetLine:
    direction = (end[0] - start[0], end[1] - start[1])
    length = sqrt((direction[0] ** 2) + (direction[1] ** 2))
    if isclose(length, 0.0):
        raise GeometryOffsetError("Polygon contains a zero-length edge.")

    unit_direction = (direction[0] / length, direction[1] / length)
    outward_normal = _build_outward_normal(
        unit_direction=unit_direction,
        is_counter_clockwise=is_counter_clockwise,
    )

    return _OffsetLine(
        point=(
            start[0] + (outward_normal[0] * distance),
            start[1] + (outward_normal[1] * distance),
        ),
        direction=unit_direction,
    )


def _resolve_vertex(
    *,
    original_vertex: Point,
    previous_line: _OffsetLine,
    current_line: _OffsetLine,
    tolerance: float,
) -> tuple[Point, ...]:
    denominator = _cross(previous_line.direction, current_line.direction)
    if not isclose(denominator, 0.0, abs_tol=tolerance):
        delta = (
            current_line.point[0] - previous_line.point[0],
            current_line.point[1] - previous_line.point[1],
        )
        distance = _cross(delta, current_line.direction) / denominator
        intersection = (
            previous_line.point[0] + (distance * previous_line.direction[0]),
            previous_line.point[1] + (distance * previous_line.direction[1]),
        )
        return (intersection,)

    if not _parallel_lines_are_compatible(
        previous_line, current_line, tolerance=tolerance
    ):
        # Create perpendicular connector between incompatible parallel lines
        point_on_previous = _project_point_onto_line(original_vertex, previous_line)
        point_on_current = _project_point_onto_line(original_vertex, current_line)
        return (point_on_previous, point_on_current)

    return (_project_point_onto_line(original_vertex, previous_line),)


def _resolve_join_vertices(
    *,
    original_vertex: Point,
    previous_line: _OffsetLine,
    current_line: _OffsetLine,
    fallback_points: tuple[Point, ...],
    distance: float,
    tolerance: float,
) -> tuple[Point, ...]:
    resolved_vertices = _resolve_vertex(
        original_vertex=original_vertex,
        previous_line=previous_line,
        current_line=current_line,
        tolerance=tolerance,
    )
    # If multiple vertices returned (perpendicular connector case), use them directly
    if len(resolved_vertices) > 1:
        return resolved_vertices

    # Single vertex - check miter limit
    if not resolved_vertices:
        raise GeometryOffsetError("Offset join has no resolved vertices.")
    resolved_vertex = next(iter(resolved_vertices))
    if _is_within_miter_limit(
        original_vertex=original_vertex,
        resolved_vertex=resolved_vertex,
        distance=distance,
        tolerance=tolerance,
    ):
        return (resolved_vertex,)
    return fallback_points


def _validate_input_polygon(
    geometry: BaseGeometry,
    *,
    tolerance: float,
) -> Polygon:
    if geometry.is_empty:
        raise InvalidInputPolygonError("Input polygon is empty.")
    if not isinstance(geometry, Polygon):
        raise InvalidInputPolygonError("Input geometry is not a polygon.")

    polygon = orient(geometry, sign=1.0)
    if not polygon.is_valid:
        raise InvalidInputPolygonError("Input polygon is invalid.")
    if polygon.area <= tolerance:
        raise InvalidInputPolygonError(
            "Input polygon collapsed below the minimum area."
        )
    return polygon


def _normalize_offset_polygon_result(
    geometry: BaseGeometry,
    *,
    original_polygon: Polygon,
    tolerance: float,
) -> Polygon:
    polygon = _normalize_polygon_result(geometry, tolerance=tolerance)
    if polygon.equals(original_polygon):
        raise GeometryOffsetError("Nonzero offset produced the original polygon.")
    return polygon


def _normalize_polygon_result(
    geometry: BaseGeometry,
    *,
    tolerance: float = 1e-6,
) -> Polygon:
    if geometry.is_empty:
        raise GeometryOffsetError("Offset produced an empty geometry.")

    if isinstance(geometry, Polygon):
        polygon = geometry
    elif isinstance(geometry, MultiPolygon) and len(geometry.geoms) == 1:
        candidate = geometry.geoms[0]
        if not isinstance(candidate, Polygon):
            raise GeometryOffsetError("Offset produced a non-polygon geometry.")
        polygon = candidate
    else:
        raise GeometryOffsetError("Offset produced multiple polygon parts.")

    normalized_polygon = orient(polygon, sign=1.0)
    if not normalized_polygon.is_valid:
        raise GeometryOffsetError("Offset produced an invalid polygon.")
    if normalized_polygon.area <= tolerance:
        raise GeometryOffsetError("Offset polygon collapsed below the minimum area.")
    return normalized_polygon


def _build_patch_sample_point(
    *,
    segment: BoundarySegment,
    distance: float,
    is_counter_clockwise: bool,
) -> Point:
    start = segment.start
    end = segment.end
    midpoint = (
        (start[0] + end[0]) / 2.0,
        (start[1] + end[1]) / 2.0,
    )
    direction = (end[0] - start[0], end[1] - start[1])
    length = sqrt((direction[0] ** 2) + (direction[1] ** 2))
    if isclose(length, 0.0):
        raise GeometryOffsetError("Polygon contains a zero-length edge.")

    unit_direction = (direction[0] / length, direction[1] / length)
    outward_normal = _build_outward_normal(
        unit_direction=unit_direction,
        is_counter_clockwise=is_counter_clockwise,
    )
    return (
        midpoint[0] + (outward_normal[0] * (distance / 2.0)),
        midpoint[1] + (outward_normal[1] * (distance / 2.0)),
    )


def _build_outward_normal(
    *,
    unit_direction: Point,
    is_counter_clockwise: bool,
) -> Point:
    if is_counter_clockwise:
        return (unit_direction[1], -unit_direction[0])
    return (-unit_direction[1], unit_direction[0])


def _parallel_lines_are_compatible(
    first: _OffsetLine,
    second: _OffsetLine,
    *,
    tolerance: float,
) -> bool:
    delta = (
        second.point[0] - first.point[0],
        second.point[1] - first.point[1],
    )
    return isclose(_cross(delta, first.direction), 0.0, abs_tol=tolerance)


def _is_within_miter_limit(
    *,
    original_vertex: Point,
    resolved_vertex: Point,
    distance: float,
    tolerance: float,
) -> bool:
    offset_distance = abs(distance)
    if isclose(offset_distance, 0.0, abs_tol=tolerance):
        return True

    return _distance_between_points(original_vertex, resolved_vertex) <= max(
        offset_distance * _DEFAULT_MITER_LIMIT, tolerance
    )


def _extend_unique_points(
    points: list[Point],
    new_points: tuple[Point, ...],
    *,
    tolerance: float,
) -> None:
    for point in new_points:
        if points and _points_are_close(points[-1], point, tolerance=tolerance):
            continue
        points.append(point)


def _project_point_onto_line(point: Point, line: _OffsetLine) -> Point:
    delta = (
        point[0] - line.point[0],
        point[1] - line.point[1],
    )
    scale = _dot(delta, line.direction)
    return (
        line.point[0] + (scale * line.direction[0]),
        line.point[1] + (scale * line.direction[1]),
    )


def _cross(first: Point, second: Point) -> float:
    return (first[0] * second[1]) - (first[1] * second[0])


def _distance_between_points(first: Point, second: Point) -> float:
    return sqrt(((first[0] - second[0]) ** 2) + ((first[1] - second[1]) ** 2))


def _dot(first: Point, second: Point) -> float:
    return (first[0] * second[0]) + (first[1] * second[1])


def _points_are_close(first: Point, second: Point, *, tolerance: float) -> bool:
    return _distance_between_points(first, second) <= tolerance
