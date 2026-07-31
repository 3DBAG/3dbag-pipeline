import sys
import types
from dataclasses import dataclass
import json


walls_module = types.ModuleType("building_surfaces.walls")
setattr(walls_module, "shared_walls", lambda *args, **kwargs: {})
setattr(walls_module, "write_cityjsonfeature", lambda *args, **kwargs: None)

sys.modules.setdefault("building_surfaces", types.ModuleType("building_surfaces"))
sys.modules["building_surfaces.walls"] = walls_module


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

        def get_json(self, feature_id):
            return None

        def read_feature_bytes(self, ref):
            return b"{}"

        def read_feature_json(self, ref):
            return {}

    cjindex_module = types.ModuleType("cjindex")
    cjindex_module.FeatureRef = FeatureRef  # type: ignore[attr-defined]
    cjindex_module.IndexStatus = IndexStatus  # type: ignore[attr-defined]
    cjindex_module.OpenedIndex = OpenedIndex  # type: ignore[attr-defined]
    sys.modules["cjindex"] = cjindex_module


if "cjlib" not in sys.modules:

    class CityModel:
        def __init__(self, payload):
            self.payload = payload

        @classmethod
        def parse_document_bytes(cls, data):
            return cls(json.loads(data))

        @classmethod
        def parse_feature_bytes(cls, data):
            return cls(json.loads(data))

        def close(self):
            pass

    def write_cityjsonseq_auto_transform_bytes(base_root, features):
        lines = [base_root.payload, *[feature.payload for feature in features]]
        return (
            "\n".join(json.dumps(item, separators=(",", ":")) for item in lines) + "\n"
        ).encode()

    cjlib_module = types.ModuleType("cjlib")
    setattr(cjlib_module, "CityModel", CityModel)
    setattr(
        cjlib_module,
        "write_cityjsonseq_auto_transform_bytes",
        write_cityjsonseq_auto_transform_bytes,
    )
    sys.modules["cjlib"] = cjlib_module
