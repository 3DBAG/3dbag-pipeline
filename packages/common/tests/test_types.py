from pathlib import Path
from bag3d.common.types import ExportResult


def test_export_result_to_dict():
    """Can we cast an ExportResult to a dict?"""
    export_result = ExportResult(
        tile_id="z/x/y", cityjson_path=Path("/data"),
        gpkg_path=Path("/data"),
        obj_paths=[Path("/data/file.obj"), ],
        wkt="POLYGON((0 0, 1 1))")
    d = dict(export_result)
    assert d["tile_id"] == "z/x/y"
