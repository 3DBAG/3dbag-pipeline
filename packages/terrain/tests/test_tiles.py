from pathlib import Path

import imagecodecs
import mercantile
import numpy as np
import pytest
from pmtiles.reader import MmapSource, Reader
from pmtiles.tile import zxy_to_tileid

from bag3d.terrain import tiles
from bag3d.terrain._mapterhorn.codec import save_terrarium_tile
from bag3d.terrain.assets import export_xyz
from bag3d.terrain.settings import TerrainSettings
from bag3d.terrain.state import file_record, read_manifest, save_manifest


def test_terrarium_matches_mapterhorn_encoding(tmp_path):
    values = np.linspace(-2, 258, 512 * 512).reshape((512, 512)).astype("float32")
    target = tmp_path / "14-8390-5460.webp"
    save_terrarium_tile(values.copy(), str(target))
    np.testing.assert_array_equal(
        imagecodecs.webp_decode(target.read_bytes()),
        imagecodecs.webp_decode(tiles.encode(values, 14)),
    )
    np.testing.assert_allclose(
        tiles.decode(tiles.encode(values, 14)), values, atol=0.063
    )


def test_overviews_average_heights_and_preserve_native_levels(tmp_path):
    children = [
        mercantile.Tile(2, 2, 2),
        mercantile.Tile(3, 2, 2),
        mercantile.Tile(2, 3, 2),
        mercantile.Tile(3, 3, 2),
    ]
    records = []
    for child, height in zip(children, [255, 257, 259, 261]):
        path = tmp_path / f"child-{child.x}-{child.y}.pmtiles"
        tiles.write_archive(
            path,
            [
                (
                    zxy_to_tileid(2, child.x, child.y),
                    tiles.encode(np.full((512, 512), height), 2),
                )
            ],
        )
        records.append(file_record(path))
    source = save_manifest(tmp_path / "source.json", records, bounds=[0.1, -1, 1, -0.1])
    result = read_manifest(tiles.overviews(tmp_path, source, 1))
    index = tiles.tile_index(result["files"])
    with Path(index[1, 1, 1]).open("rb") as stream:
        data = Reader(MmapSource(stream)).get(1, 1, 1)
        assert data is not None
        values = tiles.decode(data)
    np.testing.assert_allclose(values[:256, :256], 255)
    np.testing.assert_allclose(values[:256, 256:], 257)
    assert all((2, child.x, child.y) in index for child in children)
    assert (0, 0, 0) in index


def test_archive_export_clips_completes_pyramid_and_resumes(tmp_path):
    source_dir = tmp_path / "source"
    bounds = [4.3, 52.0, 4.31, 52.01]
    target_tile = next(mercantile.tiles(*bounds, zooms=3))
    root_bytes = tiles.encode(np.full((512, 512), 43.25), 0)
    detailed_bytes = tiles.encode(np.full((512, 512), 43.25), 3)
    archive = source_dir / "planet.pmtiles"
    tiles.write_archive(
        archive,
        sorted(
            [
                (0, root_bytes),
                (zxy_to_tileid(3, target_tile.x, target_tile.y), detailed_bytes),
            ]
        ),
        bounds,
    )
    manifest = save_manifest(
        source_dir / "manifest.json",
        [file_record(archive)],
        bounds=bounds,
        minzoom=0,
        maxzoom=3,
    )
    output = export_xyz(tmp_path, manifest, TerrainSettings(workers=1))
    result = read_manifest(output)
    web_root = Path(result["files"][-1]["path"]).parent
    tilejson = read_manifest(web_root / "tilejson.json")
    assert tilejson["encoding"] == "terrarium"
    assert tilejson["tileSize"] == 512
    assert tilejson["bounds"] == bounds
    assert tilejson["scheme"] == "xyz"
    for zoom in range(4):
        for tile in mercantile.tiles(*bounds, zooms=zoom):
            assert (web_root / str(zoom) / str(tile.x) / f"{tile.y}.webp").exists()
    native = web_root / "3" / str(target_tile.x) / f"{target_tile.y}.webp"
    assert native.read_bytes() == detailed_bytes
    timestamp = native.stat().st_mtime_ns
    export_xyz(
        tmp_path,
        manifest,
        TerrainSettings(workers=1, base_url="https://example.com/terrain"),
    )
    assert native.stat().st_mtime_ns == timestamp
    assert read_manifest(web_root / "tilejson.json")["tiles"] == [
        "https://example.com/terrain/{z}/{x}/{y}.webp"
    ]
    # A corrupt synthesized tile is repaired rather than trusted merely because it exists.
    synthesized = next(web_root.glob("1/*/*.webp"))
    synthesized.write_bytes(b"corrupt")
    export_xyz(tmp_path, manifest, TerrainSettings(workers=1))
    assert synthesized.read_bytes() != b"corrupt"


def test_coarse_completion_interpolates_heights_not_rgb(tmp_path):
    from bag3d.terrain._mapterhorn.export import overzoom_parent

    values = np.full((512, 512), 255.0)
    values[:, 128:] = 257
    source = tmp_path / "parent.webp"
    source.write_bytes(tiles.encode(values, 14))
    target = tmp_path / "child.webp"
    overzoom_parent((source, [(15, 0, 0, target)], "webp", "terrarium"))
    decoded = tiles.decode(target.read_bytes())
    assert decoded.min() >= 255
    assert decoded.max() <= 257


def test_encoding_rejects_nonfinite_and_out_of_range_values():
    for value in [float("nan"), float("inf"), -40000, 40000]:
        with pytest.raises(ValueError):
            tiles.encode(np.full((512, 512), value), 14)
