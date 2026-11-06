"""Tests for geometry operations used in underpass detection."""

from shapely.geometry import MultiPolygon, Polygon

from bag3d.underpasses.geometry_ops import (
    double_buffer_filter,
    dump_multi_to_polygons,
    extract_polygons,
    safe_difference,
    snap_pair,
    union_geometries,
)


class TestUnionGeometries:
    def test_empty_list_returns_empty(self):
        result = union_geometries([])
        assert result.is_empty

    def test_single_polygon(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = union_geometries([p])
        assert result.equals(p)

    def test_overlapping_polygons(self):
        p1 = Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])
        p2 = Polygon([(1, 0), (3, 0), (3, 1), (1, 1)])
        result = union_geometries([p1, p2])
        assert result.area > 0
        assert result.geom_type in ("Polygon", "MultiPolygon")

    def test_none_and_empty_filtered_out(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = union_geometries([None, Polygon(), p])
        assert result.equals(p)


class TestSafeDifference:
    def test_basic_difference(self):
        p1 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        p2 = Polygon([(1, 0), (2, 0), (2, 1), (1, 1)])
        result = safe_difference(p1, p2)
        assert not result.is_empty
        assert result.area < p1.area

    def test_no_overlap(self):
        p1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        p2 = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
        result = safe_difference(p1, p2)
        assert result.equals(p1)

    def test_complete_overlap(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = safe_difference(p, p)
        assert result.is_empty

    def test_empty_inputs(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        assert safe_difference(Polygon(), p).is_empty
        assert safe_difference(p, Polygon()).equals(p)


class TestExtractPolygons:
    def test_single_polygon_passthrough(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = extract_polygons(p)
        assert result.equals(p)

    def test_multipolygon_returns_polygons(self):
        p1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        p2 = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
        mp = MultiPolygon([p1, p2])
        result = extract_polygons(mp)
        assert result.geom_type == "MultiPolygon"

    def test_empty_geom(self):
        assert extract_polygons(Polygon()).is_empty
        assert extract_polygons(None).is_empty


class TestDoubleBufferFilter:
    def test_filter_removes_small_slivers(self):
        small_sliver = Polygon([(0, 0), (0.1, 0), (0.1, 0.1), (0, 0.1)])
        result = double_buffer_filter(small_sliver, distance=0.2)
        assert result.is_empty

    def test_large_polygon_survives(self):
        p = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        result = double_buffer_filter(p, distance=0.2)
        assert not result.is_empty

    def test_empty_input(self):
        assert double_buffer_filter(Polygon()).is_empty
        assert double_buffer_filter(None).is_empty


class TestSnapPair:
    def test_snap_nearby_polygons(self):
        p1 = Polygon([(0, 0), (1, 0), (1.001, 1), (0, 1)])
        p2 = Polygon([(0.999, 0), (2, 0), (2, 1), (0.999, 1)])
        s1, s2 = snap_pair(p1, p2, tolerance=0.01)
        assert not s1.is_empty
        assert not s2.is_empty

    def test_empty_inputs(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        s1, s2 = snap_pair(Polygon(), p, 0.03)
        assert s1.is_empty
        s1, s2 = snap_pair(p, Polygon(), 0.03)
        assert s2.is_empty


class TestDumpMultiToPolygons:
    def test_single_polygon(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = dump_multi_to_polygons(p)
        assert len(result) == 1
        assert result[0].equals(p)

    def test_multipolygon(self):
        p1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        p2 = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
        mp = MultiPolygon([p1, p2])
        result = dump_multi_to_polygons(mp)
        assert len(result) == 2
        assert result[0].equals(p1)
        assert result[1].equals(p2)

    def test_empty(self):
        assert dump_multi_to_polygons(Polygon()) == []
        assert dump_multi_to_polygons(None) == []
