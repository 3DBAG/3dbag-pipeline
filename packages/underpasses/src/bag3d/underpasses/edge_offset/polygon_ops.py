from dataclasses import dataclass
from math import isclose, sqrt
from typing import NamedTuple

from shapely.geometry import Polygon
from shapely.geometry.polygon import orient


@dataclass(frozen=True, slots=True)
class EdgeOffset:
    edge_index: int
    distance: float


class _OffsetLine(NamedTuple):
    point: tuple[float, float]
    direction: tuple[float, float]


def offset_polygon_edges(polygon: Polygon, offsets: list[EdgeOffset]) -> Polygon:
    if polygon.is_empty:
        return polygon

    exterior_coords = list(polygon.exterior.coords)
    if len(exterior_coords) < 4:
        raise ValueError("Polygon exterior must contain at least three edges.")

    distance_by_edge = _validate_offsets(
        offsets=offsets,
        edge_count=len(exterior_coords) - 1,
    )
    if not distance_by_edge:
        return polygon

    shell = [(float(x), float(y)) for x, y in exterior_coords[:-1]]
    lines = [
        _build_offset_line(
            start=shell[index],
            end=shell[(index + 1) % len(shell)],
            distance=distance_by_edge.get(index, 0.0),
            is_counter_clockwise=polygon.exterior.is_ccw,
        )
        for index in range(len(shell))
    ]

    rebuilt_shell = [
        _line_intersection(
            first=lines[index - 1],
            second=lines[index],
        )
        for index in range(len(lines))
    ]

    rebuilt = Polygon(
        shell=rebuilt_shell,
        holes=[list(interior.coords) for interior in polygon.interiors],
    )
    if not rebuilt.is_valid:
        raise ValueError("Offset edges produced an invalid polygon.")

    orientation_sign = 1.0 if polygon.exterior.is_ccw else -1.0
    return orient(rebuilt, sign=orientation_sign)


def _validate_offsets(
    *, offsets: list[EdgeOffset], edge_count: int
) -> dict[int, float]:
    distance_by_edge: dict[int, float] = {}
    for offset in offsets:
        if offset.edge_index < 0 or offset.edge_index >= edge_count:
            raise ValueError(
                f"Edge index {offset.edge_index} is outside the polygon shell."
            )
        if offset.edge_index in distance_by_edge:
            raise ValueError(f"Duplicate offset for edge index {offset.edge_index}.")
        distance_by_edge[offset.edge_index] = offset.distance
    return distance_by_edge


def _build_offset_line(
    *,
    start: tuple[float, float],
    end: tuple[float, float],
    distance: float,
    is_counter_clockwise: bool,
) -> _OffsetLine:
    direction = (end[0] - start[0], end[1] - start[1])
    length = sqrt((direction[0] ** 2) + (direction[1] ** 2))
    if isclose(length, 0.0):
        raise ValueError("Polygon contains a zero-length edge.")

    unit_direction = (direction[0] / length, direction[1] / length)
    if is_counter_clockwise:
        outward_normal = (unit_direction[1], -unit_direction[0])
    else:
        outward_normal = (-unit_direction[1], unit_direction[0])

    offset_point = (
        start[0] + (outward_normal[0] * distance),
        start[1] + (outward_normal[1] * distance),
    )
    return _OffsetLine(point=offset_point, direction=unit_direction)


def _line_intersection(
    *, first: _OffsetLine, second: _OffsetLine
) -> tuple[float, float]:
    delta = (
        second.point[0] - first.point[0],
        second.point[1] - first.point[1],
    )
    denominator = _cross(first.direction, second.direction)
    if isclose(denominator, 0.0, abs_tol=1e-12):
        raise ValueError("Adjacent polygon edges are parallel or collinear.")

    distance = _cross(delta, second.direction) / denominator
    return (
        first.point[0] + (distance * first.direction[0]),
        first.point[1] + (distance * first.direction[1]),
    )


def _cross(first: tuple[float, float], second: tuple[float, float]) -> float:
    return (first[0] * second[1]) - (first[1] * second[0])
