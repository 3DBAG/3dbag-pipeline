from pathlib import Path

from shapely.geometry import Polygon

from bag3d.underpasses.edge_offset.geojson import (
    Feature,
    read_feature_collection,
    write_feature_collection,
)
from bag3d.underpasses.edge_offset.linework import write_polygon_from_edge_geojson


def test_read_and_write_feature_collection_round_trip(tmp_path: Path) -> None:
    source_path = tmp_path / "example.geojson"
    feature = Feature(
        geometry=Polygon([(0, 0), (4, 0), (4, 2), (0, 2)]),
        properties={"name": "example"},
        feature_id="square-1",
    )

    write_feature_collection([feature], path=source_path)
    loaded = read_feature_collection(source_path)

    assert len(loaded) == 1
    assert loaded[0].feature_id == "square-1"
    assert loaded[0].properties == {"name": "example"}
    assert loaded[0].geometry.equals(feature.geometry)


def test_load_polygon_from_supplied_edge_sets(tmp_path) -> None:
    data_dir = Path(__file__).resolve().parent / "data"
    output_path = tmp_path / "polygon_from_edges.geojson"

    polygon = write_polygon_from_edge_geojson(
        movable_edges_path=data_dir / "exterior_one.geojson",
        fixed_edges_path=data_dir / "interior_one.geojson",
        output_path=output_path,
    )
    reference_features = read_feature_collection(data_dir / "polygon.geojson")
    written_features = read_feature_collection(output_path)

    assert polygon.is_valid
    assert len(polygon.interiors) > 0
    assert polygon.area > 0
    assert len(reference_features) == 1
    assert polygon.equals(reference_features[0].geometry)
    assert len(written_features) == 1
    assert written_features[0].geometry.equals(reference_features[0].geometry)
