import pytest
from shapely.geometry import Polygon

from bag3d.underpasses.edge_offset.polygon_ops import EdgeOffset, offset_polygon_edges


def test_offset_empty_polygon() -> None:
    polygon = Polygon()

    updated = offset_polygon_edges(
        polygon,
        [EdgeOffset(edge_index=1, distance=2.0)],
    )

    assert updated.equals(Polygon())


def test_offset_polygon_edges_moves_one_edge_outward() -> None:
    polygon = Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])

    updated = offset_polygon_edges(
        polygon,
        [EdgeOffset(edge_index=1, distance=2.0)],
    )

    assert updated.equals(Polygon([(0, 0), (6, 0), (6, 4), (0, 4)]))


def test_offset_polygon_edges_combines_multiple_edges() -> None:
    polygon = Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])

    updated = offset_polygon_edges(
        polygon,
        [
            EdgeOffset(edge_index=1, distance=1.0),
            EdgeOffset(edge_index=2, distance=1.0),
        ],
    )

    assert updated.equals(Polygon([(0, 0), (5, 0), (5, 5), (0, 5)]))


def test_offset_polygon_edges_rejects_unknown_edge_index() -> None:
    polygon = Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])

    with pytest.raises(ValueError, match="outside the polygon shell"):
        offset_polygon_edges(
            polygon,
            [EdgeOffset(edge_index=9, distance=1.0)],
        )
