from pathlib import Path

from shapely.geometry import MultiLineString

from bag3d.underpasses.edge_offset.linework import read_multiline_feature
from bag3d.underpasses.edge_offset.rings import (
    classify_polygon_from_edge_geojson,
    classify_polygon_from_edge_sets,
    explode_multiline,
)


def test_classify_polygon_from_edge_sets_marks_expected_segments() -> None:
    movable_edges = MultiLineString(
        [
            [(4.0, 0.0), (4.0, 4.0)],
            [(4.0, 4.0), (0.0, 4.0)],
        ]
    )
    fixed_edges = MultiLineString(
        [
            [(0.0, 4.0), (0.0, 0.0)],
            [(0.0, 0.0), (4.0, 0.0)],
        ]
    )

    classified = classify_polygon_from_edge_sets(
        movable_edges=movable_edges,
        fixed_edges=fixed_edges,
    )

    assert len(classified.rings) == 1
    ring = classified.rings[0]

    movable_segments = {
        frozenset((segment.start, segment.end))
        for segment in ring.segments
        if segment.is_movable
    }
    fixed_segments = {
        frozenset((segment.start, segment.end))
        for segment in ring.segments
        if not segment.is_movable
    }

    assert ring.is_exterior
    assert ring.is_counter_clockwise
    assert movable_segments == {
        frozenset(((4.0, 0.0), (4.0, 4.0))),
        frozenset(((4.0, 4.0), (0.0, 4.0))),
    }
    assert fixed_segments == {
        frozenset(((0.0, 0.0), (4.0, 0.0))),
        frozenset(((0.0, 4.0), (0.0, 0.0))),
    }


def test_classify_polygon_from_fixture_edge_geojson_uses_all_segments() -> None:
    data_dir = Path(__file__).resolve().parent / "data"
    classified = classify_polygon_from_edge_geojson(
        movable_edges_path=data_dir / "exterior_one.geojson",
        fixed_edges_path=data_dir / "interior_one.geojson",
    )

    movable_segment_count = sum(
        1
        for ring in classified.rings
        for segment in ring.segments
        if segment.is_movable
    )
    fixed_segment_count = sum(
        1
        for ring in classified.rings
        for segment in ring.segments
        if not segment.is_movable
    )

    assert classified.polygon.is_valid
    assert len(classified.rings) == 2
    assert movable_segment_count == len(
        explode_multiline(
            MultiLineString(read_coords(data_dir / "exterior_one.geojson"))
        )
    )
    assert fixed_segment_count == len(
        explode_multiline(
            MultiLineString(read_coords(data_dir / "interior_one.geojson"))
        )
    )


def read_coords(path: Path) -> list[list[tuple[float, float]]]:
    multiline = read_multiline_feature(path)
    return [
        [(float(x), float(y)) for x, y in linestring.coords]
        for linestring in multiline.geoms
    ]
