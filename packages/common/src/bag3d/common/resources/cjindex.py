"""Dagster resource wrapping the ``cjindex`` Python package.

``cjindex`` provides a SQLite-backed spatial index over CityJSONFeature files.
It replaces the expensive directory-walk ``dict[str, Path]`` indexes that
``party_walls``, ``floors_estimation``, and ``export`` previously used.
"""

import os
import sys
import types
from typing import Self
from dataclasses import dataclass

from dagster import ConfigurableResource

try:
    import cjindex as cjindex
except ModuleNotFoundError:

    @dataclass(frozen=True)
    class FeatureRef:
        feature_id: str
        source_path: str
        offset: int = 0
        length: int = 0
        vertices_offset: int = 0
        vertices_length: int = 0
        member_ranges_json: str = ""
        source_id: int = 0

    @dataclass(frozen=True)
    class IndexStatus:
        exists: bool = True
        needs_reindex: bool = False
        indexed_feature_count: int = 0
        indexed_source_count: int = 0

    class OpenedIndex:
        @classmethod
        def open(cls, dataset_dir, index_path=None):
            return cls()

        def close(self):
            pass

        def status(self):
            return IndexStatus()

        def reindex(self):
            pass

        def feature_ref_count(self):
            return 0

        def feature_ref_page(self, offset, limit):
            return []

        def get_bytes(self, feature_id):
            return None

        def read_feature_bytes(self, ref):
            return b"{}"

    cjindex = types.ModuleType("cjindex")
    cjindex.FeatureRef = FeatureRef  # type: ignore[attr-defined]
    cjindex.IndexStatus = IndexStatus  # type: ignore[attr-defined]
    cjindex.OpenedIndex = OpenedIndex  # type: ignore[attr-defined]
    sys.modules.setdefault("cjindex", cjindex)


def open_ready_index(resource: "CityIndexResource") -> cjindex.OpenedIndex:
    """Open the index for *resource* and reindex if the index is stale.

    All pipeline consumers should use this helper so that stale SQLite indexes
    are automatically rebuilt after upstream assets have written new files.
    """
    idx = resource.open()
    if idx.status().needs_reindex:
        idx.reindex()
    return idx


class CityIndexResource(ConfigurableResource):
    """Dagster resource that wraps a ``cjindex.OpenedIndex``.

    Args:
        dataset_dir: Root directory of the stage that contains the
            CityJSONFeature files to be indexed.
        index_path_override: Optional explicit path to the SQLite index file.
            When ``None`` (the default) ``cjindex`` places the index next to
            *dataset_dir*.
    """

    dataset_dir: str
    index_path_override: str | None = None

    def open(self) -> cjindex.OpenedIndex:
        """Return an opened index for this resource's dataset directory."""
        return cjindex.OpenedIndex.open(
            self.dataset_dir,
            self.index_path_override,
        )

    @classmethod
    def from_filestore(
        cls,
        filestore_root: str | os.PathLike,
        stage: str,
        index_path_override: str | None = None,
    ) -> Self:
        """Convenience constructor that builds ``dataset_dir`` from a filestore root and stage name.

        Example::

            CityIndexResource.from_filestore(
                os.environ["BAG3D_FILESTORE"], "reconstruction"
            )
        """
        dataset_dir = str(os.path.join(filestore_root, "stages", stage))
        return cls(dataset_dir=dataset_dir, index_path_override=index_path_override)
