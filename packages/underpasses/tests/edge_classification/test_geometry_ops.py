"""Tests for geometry operations."""

import pytest
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Polygon

from bag3d.underpasses.edge_classification.geometry_ops import (
    extract_exterior_rings,
    safe_difference,
    safe_intersection,
)


class TestSafeDifference:
    """Tests for safe_difference function."""

    def test_basic_difference(self):
        """Test basic line difference operation."""
        # Line from (0,0) to (10,0)
        line1 = LineString([(0, 0), (10, 0)])
        # Line from (5,0) to (15,0) - overlaps with second half of line1
        line2 = LineString([(5, 0), (15, 0)])

        result = safe_difference(line1, line2)

        # Should get the first half: (0,0) to (5,0)
        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        # The difference should be approximately from 0 to 5
        coords = list(result.geoms[0].coords) if len(result.geoms) > 0 else []
        assert len(coords) >= 2
        assert coords[0] == pytest.approx((0, 0), abs=0.001)
        assert coords[-1] == pytest.approx((5, 0), abs=0.001)

    def test_no_overlap_returns_original(self):
        """Test that non-overlapping lines return the original geometry."""
        line1 = LineString([(0, 0), (5, 0)])
        line2 = LineString([(10, 0), (15, 0)])

        result = safe_difference(line1, line2)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        # Should return the original line
        assert result.length == pytest.approx(line1.length, abs=0.001)
        assert list(result.geoms[0].coords) == list(line1.coords)

    def test_complete_overlap_returns_empty(self):
        """Test that complete overlap returns empty geometry."""
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString([(0, 0), (10, 0)])

        result = safe_difference(line1, line2)

        assert isinstance(result, MultiLineString)
        assert result.is_empty or result.length == pytest.approx(0, abs=0.001)

    def test_multilinestring_input(self):
        """Test with MultiLineString input."""
        mls = MultiLineString([[(0, 0), (10, 0)], [(0, 5), (10, 5)]])
        line = LineString([(5, 0), (15, 0)])

        result = safe_difference(mls, line)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        assert result.geoms[0].coords[0] == pytest.approx((0, 0), abs=0.001)
        assert result.geoms[0].coords[-1] == pytest.approx((5, 0), abs=0.001)

    def test_empty_geom1_returns_empty(self):
        """Test that empty first geometry returns empty."""
        line1 = LineString()
        line2 = LineString([(0, 0), (10, 0)])

        result = safe_difference(line1, line2)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_empty_geom2_returns_geom1(self):
        """Test that empty second geometry returns first geometry."""
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString()

        result = safe_difference(line1, line2)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        assert result.length == pytest.approx(line1.length, abs=0.001)
        assert list(result.geoms[0].coords) == list(line1.coords)

    def test_none_geom1_returns_empty(self):
        """Test that None first geometry returns empty."""
        line2 = LineString([(0, 0), (10, 0)])

        result = safe_difference(None, line2)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_none_geom2_returns_geom1(self):
        """Test that None second geometry returns first geometry."""
        line1 = LineString([(0, 0), (10, 0)])

        result = safe_difference(line1, None)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty

    def test_difference_grid_snapping(self):
        """Test that geometries are snapped to grid."""
        # Line with coordinates that need snapping
        line1 = LineString([(0.0001, 0.0001), (10.0002, 0.0003)])
        line2 = LineString([(5.0001, 0.0002), (15.0003, 0.0001)])

        result = safe_difference(line1, line2, grid_size=0.001)

        assert isinstance(result, MultiLineString)
        assert result.geoms[0].coords[0] == pytest.approx((0, 0), abs=0.001)
        assert result.geoms[0].coords[-1] == pytest.approx((5, 0), abs=0.001)

    def test_real_geometry_difference(self):
        """Test with real-world underpass geometry that shares edges."""
        # Real geometries from
        geom_a = LineString(
            [
                (252882.147, 593755.995),
                (252882.184, 593756.485),
                (252882.216, 593756.91),
                (252882.255, 593757.419),
                (252882.293, 593757.929),
                (252882.337, 593758.511),
                (252882.374, 593758.991),
                (252882.414, 593759.521),
                (252882.445, 593759.932),
                (252882.481, 593760.415),
                (252882.51, 593760.79),
                (252882.529, 593761.034),
                (252882.55, 593761.32),
                (252884.68, 593761.18),
                (252884.41, 593755.54),
                (252882.117, 593755.602),
                (252882.147, 593755.995),
            ]
        )

        geom_b = LineString(
            [
                (252893.163, 593760.773),
                (252893.16, 593761.4400000001),
                (252884.68, 593761.18),
                (252884.41, 593755.54),
                (252884.402, 593755.363),
                (252893.19, 593755.3),
                (252893.19, 593755.384),
                (252893.182, 593756.889),
                (252893.163, 593760.773),
            ]
        )

        result = safe_difference(geom_a, geom_b, grid_size=0.001)

        # Result should be a MultiLineString
        assert isinstance(result, MultiLineString)
        # Should not be empty - there are parts of geom_a not in geom_b
        assert not result.is_empty
        # The result should have positive length
        assert result.length > 0
        assert result.geoms[0].coords[0] == pytest.approx(
            (252884.41, 593755.54), abs=0.001
        )


class TestSafeIntersection:
    """Tests for safe_intersection function."""

    def test_basic_intersection_lines(self):
        """Test basic line intersection."""
        # Two overlapping horizontal lines
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString([(5, 0), (15, 0)])

        result = safe_intersection(line1, line2)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        # Intersection should be from (5,0) to (10,0)
        assert result.length == pytest.approx(5.0, abs=0.1)

        assert result.geoms[0].coords[0] == pytest.approx((5, 0), abs=0.001)
        assert result.geoms[0].coords[-1] == pytest.approx((10, 0), abs=0.001)

    def test_no_intersection_returns_empty(self):
        """Test that non-intersecting geometries return empty."""
        line1 = LineString([(0, 0), (5, 0)])
        line2 = LineString([(10, 0), (15, 0)])

        result = safe_intersection(line1, line2)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_complete_overlap(self):
        """Test complete overlap returns the overlapping segment."""
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString([(0, 0), (10, 0)])

        result = safe_intersection(line1, line2)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        assert result.length == pytest.approx(10.0, abs=0.001)
        assert result.geoms[0].coords[0] == pytest.approx((0, 0), abs=0.001)
        assert result.geoms[0].coords[-1] == pytest.approx((10, 0), abs=0.001)

    def test_multiline_line_intersection(self):
        """Test intersection with MultiLineString."""
        line = LineString([(0, 5), (0, 30)])
        # Two separate line strings
        line1 = LineString([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
        line2 = LineString([(0, 20), (10, 20), (10, 30), (0, 30), (0, 20)])
        multiline = MultiLineString([line1, line2])

        result = safe_intersection(line, multiline)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        assert len(result.geoms) == 2
        assert result.geoms[0].coords[0] == pytest.approx((0, 5), abs=0.001)
        assert result.geoms[0].coords[-1] == pytest.approx((0, 10), abs=0.001)
        assert result.geoms[1].coords[0] == pytest.approx((0, 20), abs=0.001)
        assert result.geoms[1].coords[-1] == pytest.approx((0, 30), abs=0.001)

    def test_intersection_with_grid_snapping(self):
        """Test that grid snapping brings nearly-touching geometries together."""
        # Line slightly offset from another
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString([(5, 0.0001), (15, 0.0001)])

        # With snap tolerance of 0.1, these should snap together
        result = safe_intersection(line1, line2, grid_size=0.001)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty

    def test_empty_geom1_returns_empty(self):
        """Test that empty first geometry returns empty."""
        line1 = LineString()
        line2 = LineString([(0, 0), (10, 0)])

        result = safe_intersection(line1, line2)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_empty_geom2_returns_empty(self):
        """Test that empty second geometry returns empty."""
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString()

        result = safe_intersection(line1, line2)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_none_geom1_returns_empty(self):
        """Test that None first geometry returns empty."""
        line2 = LineString([(0, 0), (10, 0)])

        result = safe_intersection(None, line2)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_none_geom2_returns_empty(self):
        """Test that None second geometry returns empty."""
        line1 = LineString([(0, 0), (10, 0)])

        result = safe_intersection(line1, None)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_real_geometry_intersection(self):
        """Test with real-world underpass geometry that shares edges."""
        # Real geometry from underpass edge classification
        geom_a = LineString(
            [
                (252882.147, 593755.995),
                (252882.184, 593756.485),
                (252882.216, 593756.91),
                (252882.255, 593757.419),
                (252882.293, 593757.929),
                (252882.337, 593758.511),
                (252882.374, 593758.991),
                (252882.414, 593759.521),
                (252882.445, 593759.932),
                (252882.481, 593760.415),
                (252882.51, 593760.79),
                (252882.529, 593761.034),
                (252882.55, 593761.32),
                (252884.68, 593761.18),
                (252884.41, 593755.54),
                (252882.117, 593755.602),
                (252882.147, 593755.995),
            ]
        )

        geom_b = LineString(
            [
                (252893.163, 593760.773),
                (252893.16, 593761.4400000001),
                (252884.68, 593761.18),
                (252884.41, 593755.54),
                (252884.402, 593755.363),
                (252893.19, 593755.3),
                (252893.19, 593755.384),
                (252893.182, 593756.889),
                (252893.163, 593760.773),
            ]
        )

        result = safe_intersection(geom_a, geom_b, grid_size=0.001)

        # Result should be a MultiLineString
        assert isinstance(result, MultiLineString)
        # Should not be empty - there are parts of geom_a not in geom_b
        assert not result.is_empty
        # The result should have positive length
        assert result.length > 0
        assert result.geoms[0].coords[0] == pytest.approx(
            (252884.68, 593761.18), abs=0.001
        )


class TestExtractExteriorRings:
    """Tests for extract_exterior_rings function."""

    def test_simple_polygon(self):
        """Test extracting exterior ring from a simple polygon."""
        # Square polygon
        poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])

        result = extract_exterior_rings(poly)

        assert isinstance(result, LineString)
        assert not result.is_empty
        # Should have same length as perimeter (4 * 10 = 40)
        assert result.length == pytest.approx(40.0, abs=0.1)
        # First and last coords should match (closed ring)
        coords = list(result.coords)
        assert coords[0] == coords[-1]

    def test_polygon_with_hole(self):
        """Test that only exterior ring is extracted, not holes."""
        # Polygon with a hole in the middle
        exterior = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        hole = [(2, 2), (8, 2), (8, 8), (2, 8), (2, 2)]
        poly = Polygon(exterior, [hole])

        result = extract_exterior_rings(poly)

        assert isinstance(result, LineString)
        assert not result.is_empty
        # Should only have exterior ring length (40), not including hole
        assert result.length == pytest.approx(40.0, abs=0.1)

    def test_multipolygon_with_union(self):
        """Test extracting exterior rings from MultiPolygon with union."""
        # Two separate squares
        poly1 = Polygon([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)])
        poly2 = Polygon([(10, 0), (15, 0), (15, 5), (10, 5), (10, 0)])
        multipoly = MultiPolygon([poly1, poly2])

        result = extract_exterior_rings(multipoly, union_rings=True)

        # Result can be LineString or MultiLineString depending on union result
        assert isinstance(result, (LineString, MultiLineString))
        assert not result.is_empty
        # Total length should be sum of both perimeters (20 + 20 = 40)
        assert result.length == pytest.approx(40.0, abs=0.1)

    def test_multipolygon_without_union(self):
        """Test extracting exterior rings from MultiPolygon without union."""
        # Two separate squares
        poly1 = Polygon([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)])
        poly2 = Polygon([(10, 0), (15, 0), (15, 5), (10, 5), (10, 0)])
        multipoly = MultiPolygon([poly1, poly2])

        result = extract_exterior_rings(multipoly, union_rings=False)

        assert isinstance(result, MultiLineString)
        assert not result.is_empty
        assert len(result.geoms) == 2
        # Each ring should have perimeter of 20
        assert result.geoms[0].length == pytest.approx(20.0, abs=0.1)
        assert result.geoms[1].length == pytest.approx(20.0, abs=0.1)

    def test_multipolygon_touching_polygons(self):
        """Test MultiPolygon with touching (adjacent) polygons."""
        # Two squares that share an edge
        poly1 = Polygon([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)])
        poly2 = Polygon([(5, 0), (10, 0), (10, 5), (5, 5), (5, 0)])
        multipoly = MultiPolygon([poly1, poly2])

        result = extract_exterior_rings(multipoly, union_rings=True)

        # Union may merge the touching edges
        assert isinstance(result, (LineString, MultiLineString))
        assert not result.is_empty
        assert result.length > 0

    def test_empty_polygon_returns_empty(self):
        """Test that empty polygon returns empty MultiLineString."""
        poly = Polygon()

        result = extract_exterior_rings(poly)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_none_polygon_returns_empty(self):
        """Test that None returns empty MultiLineString."""
        result = extract_exterior_rings(None)

        assert isinstance(result, MultiLineString)
        assert result.is_empty

    def test_complex_polygon(self):
        """Test with complex real-world polygon shape."""
        # Irregular polygon (e.g., building footprint)
        coords = [
            (100, 100),
            (150, 100),
            (150, 120),
            (180, 120),
            (180, 200),
            (150, 200),
            (150, 180),
            (100, 180),
            (100, 100),
        ]
        poly = Polygon(coords)

        result = extract_exterior_rings(poly)

        assert isinstance(result, LineString)
        assert not result.is_empty
        # Verify it's closed
        result_coords = list(result.coords)
        assert result_coords[0] == result_coords[-1]
        # Should have same number of vertices
        assert len(result_coords) == len(coords)

    def test_multipolygon_single_polygon(self):
        """Test MultiPolygon with single polygon."""
        poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
        multipoly = MultiPolygon([poly])

        result = extract_exterior_rings(multipoly, union_rings=True)

        # Single polygon in MultiPolygon may not need union
        assert isinstance(result, (LineString, MultiLineString))
        assert not result.is_empty
        assert result.length == pytest.approx(40.0, abs=0.1)

    def test_multipolygon_with_different_sizes(self):
        """Test MultiPolygon with polygons of different sizes."""
        # Small and large square
        small = Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
        large = Polygon([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)])
        multipoly = MultiPolygon([small, large])

        result = extract_exterior_rings(multipoly, union_rings=False)

        assert isinstance(result, MultiLineString)
        assert len(result.geoms) == 2
        # Small perimeter = 8, large perimeter = 40
        lengths = sorted([geom.length for geom in result.geoms])
        assert lengths[0] == pytest.approx(8.0, abs=0.1)
        assert lengths[1] == pytest.approx(40.0, abs=0.1)


class TestSafeDifferenceAndIntersectionComplementarity:
    """Test that safe_difference and safe_intersection are complementary."""

    def test_difference_and_intersection_cover_original(self):
        """Test that difference + intersection approximately equals original."""
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString([(5, 0), (15, 0)])

        diff = safe_difference(line1, line2)
        inter = safe_intersection(line1, line2)

        # The sum of lengths should approximately equal original
        total_length = diff.length + inter.length
        assert total_length == pytest.approx(line1.length, abs=0.1)

    def test_non_overlapping_difference_full_intersection_empty(self):
        """Test non-overlapping: difference=original, intersection=empty."""
        line1 = LineString([(0, 0), (5, 0)])
        line2 = LineString([(10, 0), (15, 0)])

        diff = safe_difference(line1, line2)
        inter = safe_intersection(line1, line2)

        # Difference should be the full original
        assert diff.length == pytest.approx(line1.length, abs=0.1)
        # Intersection should be empty
        assert inter.is_empty

    def test_complete_overlap_difference_empty_intersection_full(self):
        """Test complete overlap: difference=empty, intersection=full."""
        line1 = LineString([(0, 0), (10, 0)])
        line2 = LineString([(0, 0), (10, 0)])

        diff = safe_difference(line1, line2)
        inter = safe_intersection(line1, line2)

        # Difference should be empty or nearly empty
        assert diff.is_empty or diff.length < 0.1
        # Intersection should be the full original
        assert inter.length == pytest.approx(line1.length, abs=0.1)
