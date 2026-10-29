from collections import defaultdict
from dataclasses import dataclass
from math import hypot, isclose
from pathlib import Path

import pytest
from shapely.geometry import LineString, Polygon, mapping
from shapely.geometry import Point as GeometryPoint
from shapely.geometry.base import BaseGeometry

from bag3d.underpasses.edge_offset.geojson import Feature, read_feature_collection
from bag3d.underpasses.edge_offset.linework import merge_multiline_geometries
from bag3d.underpasses.edge_offset.offset_linework import (
    GeometryOffsetError,
    offset_polygon_from_classified_polygon,
)
from bag3d.underpasses.edge_offset.rings import (
    BoundarySegment,
    ClassifiedPolygon,
    classify_polygon_from_edge_sets,
)

DISTANCE = 0.25
TOLERANCE = 1e-6
FIXED_EDGE_TYPES = frozenset({"interior", "shared"})
REGRESSION_DIR = Path(__file__).resolve().parent / "data" / "regression"
type GroupKey = tuple[str, int]


@dataclass(frozen=True, slots=True)
class RegressionGroup:
    fixture_path: Path
    identificatie: str
    underpass_id: int


def _collect_regression_groups() -> list[RegressionGroup]:
    groups: list[RegressionGroup] = []
    for fixture_path in sorted(REGRESSION_DIR.glob("*.geojson")):
        seen_groups: set[GroupKey] = set()
        for feature in read_feature_collection(fixture_path):
            key = _feature_group_key(feature, fixture_path=fixture_path)
            if key in seen_groups:
                continue

            seen_groups.add(key)
            identificatie, underpass_id = key
            groups.append(
                RegressionGroup(
                    fixture_path=fixture_path,
                    identificatie=identificatie,
                    underpass_id=underpass_id,
                )
            )
    return groups


def _collect_identificaties() -> list[str]:
    return sorted({group.identificatie for group in REGRESSION_GROUPS})


def _read_group_features(regression_group: RegressionGroup) -> list[Feature]:
    return [
        feature
        for feature in read_feature_collection(regression_group.fixture_path)
        if _feature_group_key(feature, fixture_path=regression_group.fixture_path)
        == (regression_group.identificatie, regression_group.underpass_id)
    ]


def _split_edge_geometries(
    features: list[Feature],
    *,
    regression_group: RegressionGroup,
) -> tuple[list[BaseGeometry], list[BaseGeometry]]:
    edge_geometries: defaultdict[str, list[BaseGeometry]] = defaultdict(list)
    for feature in features:
        edge_type = feature.properties.get("edge_type")
        if not isinstance(edge_type, str):
            pytest.fail(f"{_group_label(regression_group)} has an invalid edge_type.")
        if edge_type != "exterior" and edge_type not in FIXED_EDGE_TYPES:
            pytest.fail(
                f"{_group_label(regression_group)} has an unknown edge_type: "
                f"{edge_type!r}."
            )
        edge_geometries[edge_type].append(feature.geometry)

    movable_geometries = edge_geometries["exterior"]
    fixed_geometries: list[BaseGeometry] = []
    for edge_type in FIXED_EDGE_TYPES:
        fixed_geometries.extend(edge_geometries[edge_type])
    return movable_geometries, fixed_geometries


def _feature_group_key(feature: Feature, *, fixture_path: Path) -> GroupKey:
    identificatie = feature.properties.get("identificatie")
    underpass_id = feature.properties.get("underpass_id")
    if not isinstance(identificatie, str) or not isinstance(underpass_id, int):
        raise AssertionError(  # noqa: TRY004 - A fixture assertion.
            f"{fixture_path.name} contains a feature without a valid group key."
        )
    return identificatie, underpass_id


def _offset_regression_group(
    regression_group: RegressionGroup,
) -> tuple[ClassifiedPolygon, Polygon]:
    features = _read_group_features(regression_group)
    movable_geometries, fixed_geometries = _split_edge_geometries(
        features,
        regression_group=regression_group,
    )
    if not movable_geometries:
        pytest.fail(f"{_group_label(regression_group)} has no exterior edge geometry.")

    movable_edges = merge_multiline_geometries(*movable_geometries)
    fixed_edges = merge_multiline_geometries(*fixed_geometries)
    classified = classify_polygon_from_edge_sets(
        movable_edges=movable_edges,
        fixed_edges=fixed_edges,
        tolerance=TOLERANCE,
    )

    try:
        updated = offset_polygon_from_classified_polygon(
            classified,
            distance=DISTANCE,
            tolerance=TOLERANCE,
        )
    except GeometryOffsetError as e:
        pytest.fail(f"{_group_label(regression_group)} failed to offset: {e}")
    return classified, updated


def _offset_feature_collection_payload(
    *,
    identificatie: str,
    features: list[Feature],
) -> dict[str, object]:
    return {
        "type": "FeatureCollection",
        "name": f"{identificatie}-offset",
        "crs": {
            "type": "name",
            "properties": {"name": "EPSG:28992"},
        },
        "features": [
            {
                "type": "Feature",
                "properties": feature.properties,
                "geometry": mapping(feature.geometry),
            }
            for feature in features
        ],
    }


def _outward_shifted_segment(
    segment: BoundarySegment,
    *,
    distance: float,
    is_counter_clockwise: bool,
) -> LineString:
    dx = segment.end[0] - segment.start[0]
    dy = segment.end[1] - segment.start[1]
    length = hypot(dx, dy)
    if isclose(length, 0.0, abs_tol=1e-12):
        raise AssertionError("Cannot offset a zero-length boundary segment.")

    unit_direction = (dx / length, dy / length)
    if is_counter_clockwise:
        outward_normal = (unit_direction[1], -unit_direction[0])
    else:
        outward_normal = (-unit_direction[1], unit_direction[0])

    offset_x = outward_normal[0] * distance
    offset_y = outward_normal[1] * distance
    return LineString(
        [
            (segment.start[0] + offset_x, segment.start[1] + offset_y),
            (segment.end[0] + offset_x, segment.end[1] + offset_y),
        ]
    )


def _midpoint(line: LineString) -> GeometryPoint:
    return line.interpolate(0.5, normalized=True)


def _group_label(regression_group: RegressionGroup) -> str:
    return (
        f"{regression_group.fixture_path.name} "
        f"{regression_group.identificatie}/{regression_group.underpass_id}"
    )


REGRESSION_GROUPS = _collect_regression_groups()
REGRESSION_IDENTIFICATIES = _collect_identificaties()


@pytest.mark.parametrize("identificatie", REGRESSION_IDENTIFICATIES)
def test_regression_offset_linework_builds_offset_geojson_payload(
    identificatie: str,
) -> None:
    output_features = []
    for regression_group in REGRESSION_GROUPS:
        if regression_group.identificatie != identificatie:
            continue

        _, updated = _offset_regression_group(regression_group)
        output_features.append(
            Feature(
                geometry=updated,
                properties={
                    "identificatie": regression_group.identificatie,
                    "underpass_id": regression_group.underpass_id,
                    "offset_distance": DISTANCE,
                    "crs": "EPSG:28992",
                },
            )
        )

    payload = _offset_feature_collection_payload(
        identificatie=identificatie,
        features=output_features,
    )

    assert output_features
    assert payload["type"] == "FeatureCollection"
    assert payload["name"] == f"{identificatie}-offset"
    assert payload["crs"] == {
        "type": "name",
        "properties": {"name": "EPSG:28992"},
    }

    payload_features = payload["features"]
    assert isinstance(payload_features, list)
    assert len(payload_features) == len(output_features)
    for payload_feature, output_feature in zip(
        payload_features,
        output_features,
        strict=True,
    ):
        assert payload_feature["type"] == "Feature"
        assert payload_feature["properties"] == output_feature.properties
        assert payload_feature["geometry"] == mapping(output_feature.geometry)


@pytest.mark.parametrize(
    "regression_group",
    REGRESSION_GROUPS,
    ids=lambda group: f"{group.fixture_path.stem}-{group.underpass_id}",
)
def test_regression_offset_linework_offsets_material_exterior_edge_midpoints(
    regression_group: RegressionGroup,
) -> None:
    classified, updated = _offset_regression_group(regression_group)

    assert isinstance(updated, Polygon)
    assert not updated.is_empty
    assert updated.is_valid
    assert not updated.equals(classified.polygon)

    updated_boundary = updated.boundary
    checked_segments = 0
    for ring in classified.rings:
        for segment in ring.segments:
            if not segment.is_movable:
                continue

            expected_segment = _outward_shifted_segment(
                segment,
                distance=DISTANCE,
                is_counter_clockwise=ring.is_counter_clockwise,
            )
            if expected_segment.length <= 3 * DISTANCE:
                continue

            checked_segments += 1
            expected_midpoint = _midpoint(expected_segment)
            assert updated_boundary.distance(expected_midpoint) <= TOLERANCE, (
                f"{_group_label(regression_group)} has a movable boundary segment "
                f"whose midpoint is not offset by {DISTANCE}: "
                f"{list(expected_segment.coords)}"
            )

    assert checked_segments > 0, (
        f"{_group_label(regression_group)} has no movable boundary segments long "
        "enough for a stable midpoint assertion."
    )
