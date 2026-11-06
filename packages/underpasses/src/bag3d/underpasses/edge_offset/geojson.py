import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry

type JsonObject = dict[str, Any]


@dataclass(frozen=True, slots=True)
class Feature:
    geometry: BaseGeometry
    properties: JsonObject
    feature_id: str | int | None = None


def read_feature_collection(path: Path) -> list[Feature]:
    if not path.exists():
        raise FileNotFoundError(path)

    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("type") != "FeatureCollection":
        raise ValueError("GeoJSON payload must be a FeatureCollection.")

    if "features" not in payload or not isinstance(payload["features"], list):
        raise ValueError("GeoJSON FeatureCollection must contain a features list.")

    features: list[Feature] = []
    for feature_payload in payload["features"]:
        if not isinstance(feature_payload, dict):
            raise ValueError("GeoJSON feature entries must be objects.")  # noqa: TRY004 - Preserve API.
        if feature_payload.get("type") != "Feature":
            raise ValueError("GeoJSON features must have type 'Feature'.")
        if "geometry" not in feature_payload:
            raise ValueError("GeoJSON features must contain geometry.")

        geometry = shape(feature_payload["geometry"])
        properties = feature_payload.get("properties", {})
        if not isinstance(properties, dict):
            raise ValueError("GeoJSON feature properties must be an object.")  # noqa: TRY004 - Preserve API.

        feature_id = feature_payload.get("id")
        if feature_id is not None and not isinstance(feature_id, str | int):
            raise ValueError("GeoJSON feature id must be a string or integer.")

        features.append(
            Feature(
                geometry=geometry,
                properties=properties,
                feature_id=feature_id,
            )
        )

    return features


def write_feature_collection(features: list[Feature], *, path: Path) -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [_serialize_feature(feature) for feature in features],
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"{json.dumps(payload, indent=2)}\n",
        encoding="utf-8",
    )


def _serialize_feature(feature: Feature) -> JsonObject:
    feature_payload: JsonObject = {
        "type": "Feature",
        "properties": feature.properties,
        "geometry": mapping(feature.geometry),
    }
    if feature.feature_id is not None:
        feature_payload["id"] = feature.feature_id
    return feature_payload
