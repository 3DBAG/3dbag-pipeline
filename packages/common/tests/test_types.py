from pathlib import Path

from bag3d.common.types import ExportResult


def test_export_result(tmp_path):
    """Test ExportResult class"""
    export_result = ExportResult(
        tile_id="z/x/y",
        cityjson_path=tmp_path,
        gpkg_path=tmp_path,
        obj_paths=[
            Path(tmp_path),
        ],
        wkt="POLYGON((0 0, 1 1))",
    )
    d = dict(export_result)
    assert d["tile_id"] == "z/x/y"
    assert export_result.has_cityjson is True
    assert export_result.has_gpkg is True
    # has_obj option needs 3 paths to validate True
    assert export_result.has_obj is False

