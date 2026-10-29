from pathlib import Path

from shapely import build_area
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPolygon,
    Polygon,
)
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from bag3d.underpasses.edge_offset.geojson import (
    Feature,
    read_feature_collection,
    write_feature_collection,
)


def load_polygon_from_edge_geojson(
    *,
    movable_edges_path: Path,
    fixed_edges_path: Path,
) -> Polygon:
    movable_edges = read_multiline_feature(movable_edges_path)
    fixed_edges = read_multiline_feature(fixed_edges_path)
    return build_polygon_from_edge_sets(
        movable_edges=movable_edges,
        fixed_edges=fixed_edges,
    )


def write_polygon_from_edge_geojson(
    *,
    movable_edges_path: Path,
    fixed_edges_path: Path,
    output_path: Path,
) -> Polygon:
    polygon = load_polygon_from_edge_geojson(
        movable_edges_path=movable_edges_path,
        fixed_edges_path=fixed_edges_path,
    )
    write_feature_collection(
        [
            Feature(
                geometry=polygon,
                properties={},
            )
        ],
        path=output_path,
    )
    return polygon


def read_multiline_feature(path: Path) -> MultiLineString:
    features = read_feature_collection(path)
    if len(features) != 1:
        raise ValueError("GeoJSON edge input must contain exactly one feature.")

    return coerce_multiline_geometry(features[0].geometry)


def coerce_multiline_geometry(geometry: BaseGeometry | None) -> MultiLineString:
    if geometry is None or geometry.is_empty:
        return MultiLineString()
    if isinstance(geometry, MultiLineString):
        return geometry
    if isinstance(geometry, LineString):
        return MultiLineString([geometry.coords])

    if isinstance(geometry, GeometryCollection):
        line_parts: list[LineString] = []
        for part in geometry.geoms:
            multiline = coerce_multiline_geometry(part)
            line_parts.extend(multiline.geoms)
        return MultiLineString([list(line.coords) for line in line_parts])

    raise ValueError(
        "Edge input must contain only LineString or MultiLineString geometry."
    )


def merge_multiline_geometries(*geometries: BaseGeometry | None) -> MultiLineString:
    line_parts: list[LineString] = []
    for geometry in geometries:
        multiline = coerce_multiline_geometry(geometry)
        line_parts.extend(multiline.geoms)
    return MultiLineString([list(line.coords) for line in line_parts])


def build_polygon_from_edge_sets(
    *,
    movable_edges: MultiLineString,
    fixed_edges: MultiLineString,
) -> Polygon:
    merged_linework = unary_union([movable_edges, fixed_edges])
    area_geometry = build_area(merged_linework)

    if isinstance(area_geometry, Polygon):
        return area_geometry

    if isinstance(area_geometry, MultiPolygon) and len(area_geometry.geoms) == 1:
        polygon = area_geometry.geoms[0]
        if isinstance(polygon, Polygon):
            return polygon

    raise ValueError("Edge linework did not resolve to a single polygon.")
