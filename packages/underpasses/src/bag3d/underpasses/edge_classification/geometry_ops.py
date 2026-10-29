"""Geometry operations for edge classification."""

from collections.abc import Sequence
from typing import cast

from shapely import set_precision
from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPolygon,
    Polygon,
)
from shapely.geometry.base import BaseGeometry
from shapely.ops import linemerge, unary_union


def snap_to_grid(
    geom: Polygon | LineString | MultiLineString | MultiPolygon,
    grid_size: float = 0.001,
) -> Polygon | LineString | MultiLineString | MultiPolygon:
    """
    Snap geometry coordinates to a grid with the given grid size.

    Uses Shapely's set_precision() for efficient coordinate snapping.

    Args:
        geom: Input geometry
        grid_size: Grid size for snapping (default: 0.001)

    Returns:
        Snapped geometry
    """
    if geom is None or geom.is_empty:
        return geom

    return set_precision(geom, grid_size=grid_size)


def extract_exterior_rings(
    geom: Polygon | MultiPolygon | None, union_rings: bool = True
) -> LineString | MultiLineString:
    """
    Extract exterior rings from a polygon or multipolygon.

    Matches SQL logic:
    - For Polygon: returns the exterior ring
    - For MultiPolygon: extracts all exterior rings and unions them (ST_Union)

    Args:
        geom: Input polygon or multipolygon
        union_rings: Whether to union rings for MultiPolygon (default: True, matches SQL ST_Union)

    Returns:
        LineString or MultiLineString of exterior rings
    """
    if geom is None or geom.is_empty:
        return MultiLineString([])

    if isinstance(geom, Polygon):
        return geom.exterior
    elif isinstance(geom, MultiPolygon):
        rings = [poly.exterior for poly in geom.geoms]
        if union_rings and len(rings) > 1:
            # Union the rings (matches SQL ST_Union behavior)
            return cast(LineString | MultiLineString, unary_union(rings))
        else:
            return MultiLineString(rings)
    else:
        return MultiLineString([])


def safe_difference(
    geom1: LineString | MultiLineString | None,
    geom2: BaseGeometry | None,
    grid_size: float = 0.001,
) -> MultiLineString:
    """
    Compute the geometric difference: parts of geom1 that do NOT overlap with geom2.

    Process:
    1. Snap both geometries to grid using set_precision()
    2. Compute geom1.difference(geom2) - removes overlapping parts
    3. Merge connected line segments with linemerge()
    4. Return as MultiLineString

    Args:
        geom1: Base geometry (line or multiline)
        geom2: Geometry to subtract (line or multiline)
        grid_size: Grid size for coordinate snapping (default: 0.001m)

    Returns:
        Parts of geom1 NOT in geom2, as MultiLineString. Empty if geom1 is empty.
    """
    if geom1 is None or geom1.is_empty:
        return MultiLineString([])
    if geom2 is None or geom2.is_empty:
        return MultiLineString(
            [geom1] if isinstance(geom1, LineString) else list(geom1.geoms)
        )

    try:
        # Use grid_size parameter for difference operation
        diff = set_precision(geom1, grid_size=grid_size).difference(
            set_precision(geom2, grid_size=grid_size)
        )

        # Check for None result
        if diff is None or diff.is_empty:
            return MultiLineString([])
        # Handle result based on type
        elif isinstance(diff, LineString):
            # Already a single line, just wrap it
            return MultiLineString([diff])
        elif isinstance(diff, MultiLineString):
            # Merge connected segments, then return
            merged = linemerge(diff)
            if isinstance(merged, LineString):
                return MultiLineString([merged])
            else:
                return merged
        else:
            # Handle empty or other geometry types
            return MultiLineString([])
    except Exception as e:  # noqa: BLE001 - Preserve experimental fallback.
        print(f"  ✗ Geometry difference failed: {e}, returning empty MultiLineString.")
        return MultiLineString([])


def safe_intersection(
    geom1: LineString | MultiLineString | None,
    geom2: BaseGeometry | None,
    grid_size: float = 0.001,
) -> MultiLineString:
    """
    Compute the geometric intersection: parts where geom1 and geom2 overlap.

    Process:
    1. Snap geom2 TO geom1 (aligns vertices within snap_tolerance)
    2. If geom2 is a polygon/multipolygon, extract and union exterior rings
    3. Snap both geometries to grid using set_precision()
    4. Compute geom1.intersection(geom2) - find overlapping parts
    5. Merge connected line segments with linemerge()
    6. Return as MultiLineString

    Args:
        geom1: First geometry (line or multiline) - the reference geometry
        geom2: Second geometry (line, or multiline) - snapped to geom1
        grid_size: Grid size for coordinate snapping (default: 0.001m)

    Returns:
        Parts where geom1 and geom2 overlap, as MultiLineString. Empty if no intersection.
    """
    if geom1 is None or geom1.is_empty or geom2 is None or geom2.is_empty:
        return MultiLineString([])

    try:
        # Compute intersection with precision
        g1_precision = set_precision(geom1, grid_size=grid_size)
        g2_precision = set_precision(geom2, grid_size=grid_size)

        intersection = g1_precision.intersection(g2_precision)

        # Check for None result
        if intersection is None or intersection.is_empty:
            return MultiLineString([])
        # Handle result based on type
        elif isinstance(intersection, LineString):
            # Already a single line, just wrap it
            return MultiLineString([intersection])
        elif isinstance(intersection, MultiLineString):
            # Merge connected segments, then return
            merged = linemerge(intersection)
            if isinstance(merged, LineString):
                return MultiLineString([merged])
            else:
                return merged
        else:
            # Handle empty or other geometry types
            return MultiLineString([])
    except Exception as e:  # noqa: BLE001 - Preserve experimental fallback.
        print(
            f"  ✗ Geometry intersection failed: {e}, returning empty MultiLineString."
        )
        return MultiLineString([])


def union_geometries(
    geoms: Sequence[LineString | MultiLineString],
) -> MultiLineString | None:
    """
    Union multiple line geometries.

    Args:
        geoms: List of line geometries

    Returns:
        Unioned geometry as MultiLineString or None if empty
    """
    valid_geoms = [g for g in geoms if g is not None and not g.is_empty]

    if not valid_geoms:
        return None

    try:
        unioned = unary_union(valid_geoms)

        # Handle result based on type
        if isinstance(unioned, LineString):
            # Already a single line, just wrap it
            return MultiLineString([unioned])
        elif isinstance(unioned, MultiLineString):
            # Merge connected segments, then return
            merged = linemerge(unioned)
            if isinstance(merged, LineString):
                return MultiLineString([merged])
            else:
                return merged
        else:
            # Handle empty or other geometry types
            return None
    except Exception:  # noqa: BLE001 - Preserve experimental fallback.
        return None


def dump_multilinestring(geom: MultiLineString) -> list[LineString]:
    """
    Convert a MultiLineString to a list of LineStrings.

    Args:
        geom: Input MultiLineString

    Returns:
        List of LineStrings
    """
    if geom is None or geom.is_empty:
        return []

    if isinstance(geom, LineString):
        return [geom]
    elif isinstance(geom, MultiLineString):
        return list(geom.geoms)
    else:
        return []
