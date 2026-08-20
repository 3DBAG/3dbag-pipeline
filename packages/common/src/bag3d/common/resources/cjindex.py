"""Dagster resource wrapping the published ``cityjson-index`` package.

The pipeline keeps this module path for compatibility while exposing the
package-oriented CityJSON index API used by downstream assets.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import cityjson_index
from dagster import ConfigurableResource


def package_ref_page_after_record_id(
    index: cityjson_index.OpenedIndex,
    after_record_id: int | None,
    page_size: int,
) -> list[cityjson_index.PackageRef]:
    """Return one keyset page of package refs, ordered by record ID."""
    return index.package_ref_page_after_record_id(after_record_id, page_size)


def iter_package_refs(
    index: cityjson_index.OpenedIndex, page_size: int
) -> Iterator[list[cityjson_index.PackageRef]]:
    """Yield package-ref pages and advance using the page's final record ID."""
    after_record_id = None
    while True:
        refs = package_ref_page_after_record_id(index, after_record_id, page_size)
        if not refs:
            return
        yield refs
        after_record_id = refs[-1].record_id


def city_model_to_feature_json(model: object) -> dict[str, Any]:
    """Serialize a native CityJSONFeature model and release it."""
    if isinstance(model, dict):
        return model
    try:
        return json.loads(model.serialize_feature_bytes())  # type: ignore[attr-defined]
    finally:
        model.close()  # type: ignore[attr-defined]


def read_package_feature_json(
    index: cityjson_index.OpenedIndex, ref: cityjson_index.PackageRef
) -> dict[str, Any]:
    """Read a package and convert its native model to a CityJSONFeature dict."""
    return city_model_to_feature_json(index.read_package(ref))


def open_ready_index(resource: CityIndexResource) -> cityjson_index.OpenedIndex:
    """Open the index and rebuild it when its source dataset is stale."""
    index = resource.open()
    if index.status().needs_reindex:
        index.reindex()
    return index


class CityIndexResource(ConfigurableResource):
    """Dagster resource for a ``cityjson_index.OpenedIndex`` over a stage."""

    dataset_dir: str
    index_path_override: str | None = None

    def open(self) -> cityjson_index.OpenedIndex:
        """Return an opened native index for this resource's dataset."""
        return cityjson_index.OpenedIndex.open(
            self.dataset_dir, self.index_path_override
        )

    @classmethod
    def from_filestore(
        cls,
        filestore_root: str | os.PathLike[str],
        stage: str,
        index_path_override: str | None = None,
    ) -> Self:
        """Build a resource from a filestore root and stage name."""
        dataset_dir = Path(filestore_root) / "stages" / stage
        return cls(
            dataset_dir=str(dataset_dir),
            index_path_override=index_path_override,
        )
