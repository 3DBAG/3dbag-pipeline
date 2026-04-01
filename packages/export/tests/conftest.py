import sys
import types
from dataclasses import dataclass

pytest_plugins = ["bag3d.common.testing.conftest_plugin"]


# Stub cjindex if not installed so tests can be collected without the native extension.
if "cjindex" not in sys.modules:

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

    cjindex_module = types.ModuleType("cjindex")
    cjindex_module.FeatureRef = FeatureRef  # type: ignore[attr-defined]
    cjindex_module.IndexStatus = IndexStatus  # type: ignore[attr-defined]
    cjindex_module.OpenedIndex = OpenedIndex  # type: ignore[attr-defined]
    sys.modules["cjindex"] = cjindex_module
