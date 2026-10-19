"""Tests for the underpass detection pipeline."""

from shapely.geometry import Polygon

from bag3d.underpasses.pipeline import (
    compute_bag_minus_bgt,
    compute_snapped_differences,
)


class TestComputeBagMinusBgt:
    def test_full_bgt_coverage_gives_empty(self):
        bag = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        result = compute_bag_minus_bgt(bag, bag, buffer_distance=0.2)
        assert result.is_empty

    def test_partial_bgt_coverage(self):
        bag = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        bgt = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = compute_bag_minus_bgt(bag, bgt, buffer_distance=0.2)
        assert not result.is_empty
        assert result.area < bag.area

    def test_empty_inputs(self):
        bag = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        assert compute_bag_minus_bgt(Polygon(), bag, 0.2).is_empty
        assert compute_bag_minus_bgt(bag, Polygon(), 0.2).geom_type in (
            "MultiPolygon",
            "Polygon",
        )


class TestComputeSnappedDifferences:
    def test_identical_geometries_give_empty(self):
        p1 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        p2 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        result = compute_snapped_differences(p1, p2)
        assert result.is_empty

    def test_empty_inputs(self):
        p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        assert compute_snapped_differences(Polygon(), p).is_empty
        assert compute_snapped_differences(p, Polygon()).is_empty
