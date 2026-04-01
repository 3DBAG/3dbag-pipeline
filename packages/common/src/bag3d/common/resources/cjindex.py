"""Dagster resource wrapping the ``cjindex`` Python package.

``cjindex`` provides a SQLite-backed spatial index over CityJSONFeature files.
It replaces the expensive directory-walk ``dict[str, Path]`` indexes that
``party_walls``, ``floors_estimation``, and ``export`` previously used.
"""

import os
from typing import Any, Self

from dagster import ConfigurableResource

_CJINDEX_IMPORT_ERROR: ModuleNotFoundError | None = None

try:
    import cjindex as cjindex
except ModuleNotFoundError as exc:
    _CJINDEX_IMPORT_ERROR = exc
    cjindex = None


def _ensure_cjindex_runtime_available() -> None:
    if _CJINDEX_IMPORT_ERROR is not None:
        raise RuntimeError(
            "cjindex is not importable. Install the vendored cjindex wheel in the runtime "
            "environment before opening a CityIndexResource."
        ) from _CJINDEX_IMPORT_ERROR
    if cjindex is None:
        raise RuntimeError(
            "cjindex is not importable. Install the vendored cjindex wheel in the runtime "
            "environment before opening a CityIndexResource."
        )

    native_module = getattr(cjindex, "_native", None)
    native_lib = getattr(native_module, "LIB", None)
    if native_module is not None and native_lib is None:
        raise RuntimeError(
            "cjindex imported successfully but its native library is unavailable. "
            "The vendored wheel only provides the Python wrapper; ensure the runtime also "
            "ships libcjindex and that cjindex can locate it."
        )


def open_ready_index(resource: "CityIndexResource") -> Any:
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

    def open(self) -> Any:
        """Return an opened index for this resource's dataset directory."""
        _ensure_cjindex_runtime_available()
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
