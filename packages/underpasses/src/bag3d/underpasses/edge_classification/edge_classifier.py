"""Core edge classification logic for underpass geometries."""

from dataclasses import dataclass

from shapely.geometry import LineString, MultiLineString, Polygon
from shapely.ops import snap

from bag3d.underpasses.edge_classification.geometry_ops import (
    dump_multilinestring,
    extract_exterior_rings,
    safe_difference,
    safe_intersection,
    union_geometries,
)


@dataclass
class ClassifiedEdges:
    """Result of edge classification for a single underpass."""

    underpass_id: int
    identificatie: str
    interior_edges: list[LineString]
    exterior_edges: list[LineString]
    shared_edges: list[LineString]


def classify_edges_for_underpass(
    underpass_id: int,
    identificatie: str,
    underpass_geom: Polygon,
    bgt_geom: Polygon,
    adjacent_geoms: list[Polygon],
    grid_size: float = 0.001,
    snap_tolerance: float = 0.03,
) -> ClassifiedEdges:
    """
    Classify edges of an underpass polygon into interior, exterior, and shared edges.

    This implements the SQL logic from edges.sql:
    1. Snap underpass geom to bgt geom to ensure they are aligned for edge classification
    2. Compute exterior edges (parts of ring NOT intersecting with BGT)
    3. Derive interior edges as everything that's NOT exterior (full ring - exterior edges)
    4. Add interior rings as interior edges
    5. Find intersections with adjacent buildings to identify shared edges
    6. Separate shared edges from exterior edges

    Args:
        underpass_id: Unique identifier for the underpass
        identificatie: BAG building identifier
        underpass_geom: The underpass polygon geometry
        bgt_geom: The BGT geometry for this building
        adjacent_geoms: List of adjacent building geometries
        grid_size: Grid size for snapping (default: 0.001)
        snap_tolerance: Tolerance for snapping adjacent geometries (default: 0.03)

    Returns:
        ClassifiedEdges object containing the classified edge lists
    """
    # Step 1: Snap BGT to underpass
    snapped_bgt = snap(bgt_geom, underpass_geom, snap_tolerance)

    # Step 2: Get the full exterior ring from original underpass
    full_ring = LineString(underpass_geom.exterior.coords)

    # Step 3: Get BGT exterior rings from SNAPPED BGT and union them

    bgt_exterior = extract_exterior_rings(snapped_bgt, union_rings=True)

    # Step 4: Compute exterior edges (parts NOT intersecting with BGT)
    exterior_edges_geom = safe_difference(full_ring, bgt_exterior, grid_size)

    # Step 5: Compute interior edges from exterior ring (full ring - exterior edges)
    if exterior_edges_geom and not exterior_edges_geom.is_empty:
        interior_edges_from_exterior = safe_difference(
            full_ring, exterior_edges_geom, grid_size
        )
    else:
        # If no exterior edges, entire ring is interior
        interior_edges_from_exterior = MultiLineString([full_ring])

    # Step 6: Add interior rings as interior edges
    interior_edges_from_rings = []
    for interior_ring in underpass_geom.interiors:
        interior_edges_from_rings.append(LineString(interior_ring.coords))

    # Combine all interior edges
    all_interior_geoms = [interior_edges_from_exterior]
    if interior_edges_from_rings:
        all_interior_geoms.append(MultiLineString(interior_edges_from_rings))

    combined_interior = union_geometries(all_interior_geoms)

    # Step 7: Find shared edges (intersection with adjacent building exteriors)
    # snap() will align adjacent geometry vertices to exterior edges within snap_tolerance
    shared_edge_parts = []

    if not exterior_edges_geom.is_empty and adjacent_geoms:
        for adjacent_geom in adjacent_geoms:
            if adjacent_geom is None or adjacent_geom.is_empty:
                continue

            # Compute intersection (safe_intersection will snap adjacent_geom to exterior_edges_geom)

            adjacent_snapped = snap(adjacent_geom, exterior_edges_geom, snap_tolerance)
            intersection = safe_intersection(
                exterior_edges_geom, adjacent_snapped, grid_size=grid_size
            )

            if intersection and not intersection.is_empty:
                shared_edge_parts.append(intersection)

    # Step 8: Union all shared edge parts
    shared_edges_geom = (
        union_geometries(shared_edge_parts) if shared_edge_parts else None
    )

    # Step 9: Remove shared edges from exterior edges to get final exterior edges
    if shared_edges_geom and not shared_edges_geom.is_empty:
        final_exterior_edges = safe_difference(
            exterior_edges_geom, shared_edges_geom, grid_size
        )
    else:
        final_exterior_edges = exterior_edges_geom

    # Step 10: Convert to lists of LineStrings
    interior_list = dump_multilinestring(combined_interior) if combined_interior else []
    exterior_list = (
        dump_multilinestring(final_exterior_edges) if final_exterior_edges else []
    )
    shared_list = dump_multilinestring(shared_edges_geom) if shared_edges_geom else []

    return ClassifiedEdges(
        underpass_id=underpass_id,
        identificatie=identificatie,
        interior_edges=interior_list,
        exterior_edges=exterior_list,
        shared_edges=shared_list,
    )
