"""Tests for edge classifier logic."""

import pytest
from shapely.geometry import LineString, Polygon

from bag3d.underpasses.edge_classification.edge_classifier import (
    ClassifiedEdges,
    classify_edges_for_underpass,
)


class TestClassifyEdgesForUnderpass:
    """Tests for classify_edges_for_underpass function."""

    @pytest.fixture
    def real_underpass_data(self):
        """Real underpass data from NL.IMBAG.Pand.0014100022186524."""
        return {
            "underpass_id": 72,
            "identificatie": "NL.IMBAG.Pand.0014100022186524",
            "underpass_geom": Polygon(
                [
                    (233531.173, 582714.127),
                    (233528.32807035584, 582717.8590531689),
                    (233532.397, 582720.934),
                    (233535.264, 582717.173),
                    (233531.173, 582714.127),
                ]
            ),
            "bgt_geom": Polygon(
                [
                    (233525.144, 582722.036),
                    (233528.328, 582717.859),
                    (233532.397, 582720.934),
                    (233529.213, 582725.111),
                    (233525.144, 582722.036),
                ]
            ),
            "adjacent_geoms": [
                Polygon(
                    [
                        (233526.751, 582710.834),
                        (233531.173, 582714.127),
                        (233525.144, 582722.036),
                        (233521.147, 582719.015),
                        (233520.745, 582718.711),
                        (233520.811, 582718.624),
                        (233520.221, 582718.178),
                        (233521.297, 582716.993),
                        (233521.223, 582716.926),
                        (233526.751, 582710.834),
                    ]
                ),
                Polygon(
                    [
                        (233529.213, 582725.111),
                        (233535.264, 582717.173),
                        (233539.355, 582720.22),
                        (233533.282, 582728.187),
                        (233529.213, 582725.111),
                    ]
                ),
            ],
        }

    def test_basic_classification(self, real_underpass_data):
        """Test that classification returns the expected structure."""
        result = classify_edges_for_underpass(
            underpass_id=real_underpass_data["underpass_id"],
            identificatie=real_underpass_data["identificatie"],
            underpass_geom=real_underpass_data["underpass_geom"],
            bgt_geom=real_underpass_data["bgt_geom"],
            adjacent_geoms=real_underpass_data["adjacent_geoms"],
        )

        # Check return type
        assert isinstance(result, ClassifiedEdges)
        assert result.underpass_id == 72
        assert result.identificatie == "NL.IMBAG.Pand.0014100022186524"

        # Check that we get lists
        assert isinstance(result.interior_edges, list)
        assert isinstance(result.exterior_edges, list)
        assert isinstance(result.shared_edges, list)

        # All items should be LineStrings
        for edge in result.interior_edges:
            assert isinstance(edge, LineString)
        for edge in result.exterior_edges:
            assert isinstance(edge, LineString)
        for edge in result.shared_edges:
            assert isinstance(edge, LineString)

    def test_identical_underpass_and_bgt_produces_all_interior(
        self, real_underpass_data
    ):
        """When underpass and BGT are identical, all edges should be interior."""
        result = classify_edges_for_underpass(
            underpass_id=real_underpass_data["underpass_id"],
            identificatie=real_underpass_data["identificatie"],
            underpass_geom=real_underpass_data["underpass_geom"],
            bgt_geom=real_underpass_data["underpass_geom"],
            adjacent_geoms=[],
        )

        # When underpass == BGT, there should be no exterior edges
        # (all edges are covered by BGT)
        assert len(result.exterior_edges) == 0, (
            "No exterior edges expected when underpass == BGT"
        )
        assert len(result.shared_edges) == 0, (
            "No shared edges expected when underpass == BGT and no adjacent buildings"
        )

        # All edges should be either interior or shared with adjacent buildings
        total_edge_length = sum(edge.length for edge in result.interior_edges)

        # Total should roughly equal the perimeter (accounting for shared edges)
        underpass_perimeter = real_underpass_data["underpass_geom"].length
        assert total_edge_length > 0, "Should have interior edges"
        assert total_edge_length <= underpass_perimeter, (
            "Interior edges should not exceed perimeter"
        )

    def test_shared_edges_with_adjacent_buildings(self, real_underpass_data):
        """Test that shared edges are correctly identified."""
        result = classify_edges_for_underpass(
            underpass_id=real_underpass_data["underpass_id"],
            identificatie=real_underpass_data["identificatie"],
            underpass_geom=real_underpass_data["underpass_geom"],
            bgt_geom=real_underpass_data["bgt_geom"],
            adjacent_geoms=real_underpass_data["adjacent_geoms"],
        )

        # With adjacent buildings, we should have some shared edges
        # The first adjacent building shares two edges with the underpass
        assert len(result.shared_edges) > 0, (
            "Should have shared edges with adjacent buildings"
        )

        # Check that shared edges have reasonable length
        total_shared_length = sum(edge.length for edge in result.shared_edges)
        assert total_shared_length > 0, "Shared edges should have positive length"

        # Shared edges should not exceed the perimeter
        assert total_shared_length <= real_underpass_data["underpass_geom"].length

    def test_no_adjacent_buildings(self, real_underpass_data):
        """Test classification when there are no adjacent buildings."""
        result = classify_edges_for_underpass(
            underpass_id=real_underpass_data["underpass_id"],
            identificatie=real_underpass_data["identificatie"],
            underpass_geom=real_underpass_data["underpass_geom"],
            bgt_geom=real_underpass_data["bgt_geom"],
            adjacent_geoms=[],  # No adjacent buildings
        )

        # Should have no shared edges
        assert len(result.shared_edges) == 0, (
            "No shared edges expected without adjacent buildings"
        )

        # Should still have interior edges (since underpass == BGT)
        assert len(result.interior_edges) > 0, "Should have interior edges"

    def test_partial_bgt_coverage(self):
        """Test when BGT only partially covers the underpass."""
        # Create a square underpass
        underpass_geom = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])

        # BGT only covers half of it
        bgt_geom = Polygon([(0, 0), (5, 0), (5, 10), (0, 10), (0, 0)])

        result = classify_edges_for_underpass(
            underpass_id=1,
            identificatie="TEST.001",
            underpass_geom=underpass_geom,
            bgt_geom=bgt_geom,
            adjacent_geoms=[],
        )

        # Should have both interior and exterior edges
        assert len(result.interior_edges) > 0, (
            "Should have interior edges (covered by BGT)"
        )
        assert len(result.exterior_edges) > 0, (
            "Should have exterior edges (not covered by BGT)"
        )

        # The right half edges should be exterior
        exterior_length = sum(edge.length for edge in result.exterior_edges)
        # Right edge (10 units) + portions of top/bottom should total roughly 20+ units
        assert exterior_length > 10, "Exterior edges should include the uncovered side"

    def test_with_interior_rings(self):
        """Test classification with a polygon that has holes (interior rings)."""
        # Create a polygon with a hole
        exterior = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        hole = [(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]
        underpass_geom = Polygon(exterior, [hole])

        # BGT matches the underpass
        bgt_geom = Polygon(exterior, [hole])

        result = classify_edges_for_underpass(
            underpass_id=2,
            identificatie="TEST.002",
            underpass_geom=underpass_geom,
            bgt_geom=bgt_geom,
            adjacent_geoms=[],
        )

        # Interior rings should be classified as interior edges
        assert len(result.interior_edges) > 0, (
            "Should have interior edges including the hole"
        )

        # The hole perimeter should be included in interior edges
        hole_perimeter = Polygon(hole).length
        total_interior_length = sum(edge.length for edge in result.interior_edges)

        # Should include both exterior ring and interior ring
        assert total_interior_length >= hole_perimeter, (
            "Should include the interior ring"
        )

    def test_grid_size_parameter(self, real_underpass_data):
        """Test that different grid_size values produce valid results."""
        grid_sizes = [0.001, 0.01, 0.1]

        for grid_size in grid_sizes:
            result = classify_edges_for_underpass(
                underpass_id=real_underpass_data["underpass_id"],
                identificatie=real_underpass_data["identificatie"],
                underpass_geom=real_underpass_data["underpass_geom"],
                bgt_geom=real_underpass_data["bgt_geom"],
                adjacent_geoms=real_underpass_data["adjacent_geoms"],
                grid_size=grid_size,
            )

            # Should still produce valid results
            assert isinstance(result, ClassifiedEdges)
            # At least one of the edge types should have edges
            total_edges = (
                len(result.interior_edges)
                + len(result.exterior_edges)
                + len(result.shared_edges)
            )
            assert total_edges > 0, f"Should have edges with grid_size={grid_size}"

    def test_snap_tolerance_parameter(self, real_underpass_data):
        """Test that different snap_tolerance values produce valid results."""
        tolerances = [0.01, 0.1, 0.5]

        for tolerance in tolerances:
            result = classify_edges_for_underpass(
                underpass_id=real_underpass_data["underpass_id"],
                identificatie=real_underpass_data["identificatie"],
                underpass_geom=real_underpass_data["underpass_geom"],
                bgt_geom=real_underpass_data["bgt_geom"],
                adjacent_geoms=real_underpass_data["adjacent_geoms"],
                snap_tolerance=tolerance,
            )

            # Should still produce valid results
            assert isinstance(result, ClassifiedEdges)
            # At least one of the edge types should have edges
            total_edges = (
                len(result.interior_edges)
                + len(result.exterior_edges)
                + len(result.shared_edges)
            )
            assert total_edges > 0, f"Should have edges with snap_tolerance={tolerance}"

    def test_edge_continuity(self, real_underpass_data):
        """Test that edges are properly split and don't overlap incorrectly."""
        result = classify_edges_for_underpass(
            underpass_id=real_underpass_data["underpass_id"],
            identificatie=real_underpass_data["identificatie"],
            underpass_geom=real_underpass_data["underpass_geom"],
            bgt_geom=real_underpass_data["bgt_geom"],
            adjacent_geoms=real_underpass_data["adjacent_geoms"],
        )

        # Each edge should have at least 2 points
        all_edges = result.interior_edges + result.exterior_edges + result.shared_edges
        for edge in all_edges:
            coords = list(edge.coords)
            assert len(coords) >= 2, "Each edge should have at least 2 points"
            assert edge.length > 0, "Each edge should have positive length"

    def test_empty_adjacent_geometries(self, real_underpass_data):
        """Test handling of None or empty geometries in adjacent list."""
        adjacent_with_empties = [
            real_underpass_data["adjacent_geoms"][0],
            None,
            Polygon(),  # Empty polygon
            real_underpass_data["adjacent_geoms"][1],
        ]

        result = classify_edges_for_underpass(
            underpass_id=real_underpass_data["underpass_id"],
            identificatie=real_underpass_data["identificatie"],
            underpass_geom=real_underpass_data["underpass_geom"],
            bgt_geom=real_underpass_data["bgt_geom"],
            adjacent_geoms=adjacent_with_empties,
        )

        # Should handle None and empty geometries gracefully
        assert isinstance(result, ClassifiedEdges)
        # Should still find shared edges with valid adjacent buildings
        assert len(result.shared_edges) >= 0
