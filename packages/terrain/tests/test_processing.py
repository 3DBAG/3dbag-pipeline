import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from bag3d.terrain import processing
from bag3d.terrain._mapterhorn import heights
from bag3d.terrain.state import file_record, read_manifest, save_manifest


def test_fill_uses_neighbor_and_preserves_valid_measurements(tmp_path, write_raster):
    values = np.full((32, 32), 7.25, dtype="float32")
    values[:, 24:] = -9999
    source = write_raster(tmp_path / "left.tif", values)
    neighbor = write_raster(
        tmp_path / "right.tif",
        np.full((32, 32), 7.25),
        transform=from_origin(85160, 448000, 5, 5),
    )
    files = [processing.raster_record(file_record(p)) for p in [source, neighbor]]
    manifest = save_manifest(
        tmp_path / "input.json", files, bounds=[4.3, 52.0, 4.4, 52.1]
    )
    output = processing.fill_ahn(tmp_path, manifest, 60, 1)
    result = read_manifest(output)
    with rasterio.open(result["files"][0]["path"]) as raster:
        filled = raster.read(1)
        assert np.max(np.abs(filled - 7.25)) < 0.0011
        assert raster.transform == from_origin(85000, 448000, 5, 5)
    assert result["remaining_nodata_pixels"] == 0
    assert output == processing.fill_ahn(tmp_path, manifest, 60, 2)
    assert output != processing.fill_ahn(tmp_path, manifest, 80, 1)


def test_normalization_never_modifies_download(tmp_path, write_raster):
    source = write_raster(tmp_path / "raw.tif", np.full((16, 16), -1.25))
    before = source.read_bytes()
    manifest = save_manifest(
        tmp_path / "input.json", [file_record(source)], bounds=[4.3, 52.0, 4.4, 52.1]
    )
    output = processing.normalize_ahn(tmp_path, manifest, 1)
    assert source.read_bytes() == before
    record = read_manifest(output)["files"][0]
    with rasterio.open(record["path"]) as raster:
        assert raster.tags()["HEIGHT_REFERENCE"] == "NAP"
        assert raster.tags(ns="IMAGE_STRUCTURE")["LAYOUT"] == "COG"
        np.testing.assert_allclose(raster.read(1), -1.25, atol=0.0011)


def test_height_conversion_preserves_nodata_and_rejects_double_conversion(
    tmp_path, write_raster, monkeypatch
):
    class Transformer:
        def transform(self, x, y, z, **kwargs):
            return x, y, z + 43

    monkeypatch.setattr(heights, "TRANSFORMER", Transformer())
    values = np.full((8, 8), -1.5)
    values[0, 0] = -9999
    source = write_raster(
        tmp_path / "source.tif", values, tags={"HEIGHT_REFERENCE": "NAP"}
    )
    target = tmp_path / "ellipsoid.tif"
    heights.convert_file((source, target, {}, True))
    with rasterio.open(target) as raster:
        result = raster.read(1)
        assert result[0, 0] == -9999
        np.testing.assert_allclose(result[1:], 41.5, atol=0.0011)
    with pytest.raises(ValueError, match="another height reference"):
        heights.convert_file((target, tmp_path / "double.tif", {}, True))


def test_missing_grid_fails_before_creating_height_outputs(tmp_path, write_raster):
    source = write_raster(tmp_path / "source.tif", np.ones((8, 8)))
    inputs = save_manifest(
        tmp_path / "inputs.json", [file_record(source)], bounds=[4.3, 52, 4.4, 52.1]
    )
    grids = save_manifest(tmp_path / "grids.json", [])
    with pytest.raises(ValueError, match="Missing mandatory grid"):
        processing.convert_ahn(tmp_path, inputs, grids, 1)
    assert not list((tmp_path / "builds").rglob("*.tif"))


def test_processing_requires_a_matching_download_selection(tmp_path):
    bounds = [4.3, 52, 4.4, 52.1]
    manifest = save_manifest(
        tmp_path / "inputs.json", [], requested_bounds=bounds, selection_radius_m=500
    )
    processing.validate_selection(manifest, bounds, 500)
    with pytest.raises(ValueError, match="Rematerialize terrain_download"):
        processing.validate_selection(manifest, None, 500)
    with pytest.raises(ValueError, match="Rematerialize terrain_download"):
        processing.validate_selection(manifest, bounds, 1000)
