"""Core pipeline logic for underpass detection.

Implements the 4-step SQL pipeline as Python/Shapely operations:
  1. bag_bgt_join: union BGT geometries per BAG identificatie (SQL)
  2. bag_minus_bgt: difference with double buffer filtering
  3. snapped_differences: double snapping + intersection of differences
  4. final_filter: dump to single polygons + double buffer filter + ID assignment
"""

from shapely.geometry import MultiPolygon, Polygon

from bag3d.underpasses.geometry_ops import (
    double_buffer_filter,
    extract_polygons,
    safe_difference,
    snap_pair,
)


def compute_bag_minus_bgt(
    bag_geom: Polygon | MultiPolygon | None,
    bgt_geom: Polygon | MultiPolygon | None,
    buffer_distance: float = 0.2,
) -> Polygon | MultiPolygon:
    if bag_geom is None or bag_geom.is_empty:
        return Polygon()
    if bgt_geom is None or bgt_geom.is_empty:
        return extract_polygons(bag_geom)

    raw_diff = safe_difference(bag_geom, bgt_geom)
    if raw_diff.is_empty:
        return Polygon()

    diff_poly = extract_polygons(raw_diff)
    if diff_poly.is_empty:
        return Polygon()

    if double_buffer_filter(diff_poly, buffer_distance).is_empty:
        return Polygon()

    if isinstance(diff_poly, Polygon):
        return MultiPolygon([diff_poly])
    return diff_poly


def compute_snapped_differences(
    bag_geom: Polygon | MultiPolygon | None,
    bgt_geom: Polygon | MultiPolygon | None,
    snap_tolerance: float = 0.01,
) -> Polygon | MultiPolygon:
    if bag_geom is None or bag_geom.is_empty:
        return Polygon()
    if bgt_geom is None or bgt_geom.is_empty:
        return Polygon()

    bag_snap, bgt_snap = snap_pair(bag_geom, bgt_geom, snap_tolerance)

    diff1 = safe_difference(bag_geom, bgt_snap)
    diff2 = safe_difference(bag_snap, bgt_geom)

    if diff1.is_empty or diff2.is_empty:
        return Polygon()

    try:
        intersection = diff1.intersection(diff2)
        if intersection is None or intersection.is_empty:
            return Polygon()

        result = extract_polygons(intersection)
        if result.is_empty:
            return Polygon()

        if isinstance(result, Polygon):
            return MultiPolygon([result])
        return result
    except Exception:  # noqa: BLE001 - Preserve the experimental geometry fallback.
        return Polygon()
