from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable

import cityjson_lib


@dataclass(frozen=True, slots=True)
class FeatureRecord:
    feature: dict[str, Any]
    source_path: str | Path


def write_feature_records_as_cityjsonseq(
    output_path: Path,
    records: Iterable[FeatureRecord],
) -> None:
    items = sorted(list(records), key=lambda record: _feature_id(record.feature))
    if not items:
        raise ValueError("cannot write CityJSONSeq without any features")

    base_root = cityjson_lib.CityModel.parse_document_bytes(
        _read_cityjsonseq_root_bytes(Path(items[0].source_path))
    )
    feature_models = [
        cityjson_lib.CityModel.parse_feature_bytes(_feature_json_bytes(record.feature))
        for record in items
    ]
    try:
        payload = cityjson_lib.write_cityjsonseq_auto_transform_bytes(
            base_root, feature_models
        )
    finally:
        base_root.close()
        for model in feature_models:
            model.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload)


def _read_cityjsonseq_root_bytes(path: Path) -> bytes:
    with path.open("rb") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if payload.get("type") != "CityJSON":
                raise ValueError(
                    f"expected CityJSON root object as first non-empty item in {path}"
                )
            return stripped
    raise ValueError(f"empty CityJSONSeq source file: {path}")


def _feature_json_bytes(feature: dict[str, Any]) -> bytes:
    return json.dumps(feature, separators=(",", ":")).encode("utf-8")


def _feature_id(feature: dict[str, Any]) -> str:
    feature_id = feature.get("id")
    if isinstance(feature_id, str) and feature_id:
        return feature_id

    cityobjects = feature.get("CityObjects", {})
    if isinstance(cityobjects, dict) and cityobjects:
        first_key = next(iter(cityobjects))
        if isinstance(first_key, str):
            return first_key
    return ""
