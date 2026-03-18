"""Integration tests for the export stage data flow.

Tests that export assets correctly read from reconstruction/export stages
and produce the expected output files.
"""

import csv
import json
from pathlib import Path
from unittest.mock import MagicMock

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.export.assets.export.metadata import feature_evaluation, export_index
from bag3d.export.assets.export.archive import compressed_tiles, CompressionConfig


VERSION = "test_version"


def _make_reconstruction_feature(root_dir: Path, tile_id: str, pand_id: str) -> Path:
    """Create a CityJSONFeature in the reconstruction stage layout."""
    feature_path = (
        root_dir
        / "stages"
        / "reconstruction"
        / tile_id
        / "objects"
        / pand_id
        / "reconstruct"
        / f"{pand_id}.city.jsonl"
    )
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    content = {
        "type": "CityJSONFeature",
        "id": pand_id,
        "CityObjects": {
            pand_id: {
                "type": "Building",
                "attributes": {
                    "b3_pw_selectie_reden": "A",
                    "b3_pw_bron": "ahn4",
                    "b3_puntdichtheid_ahn3": 5.0,
                    "b3_puntdichtheid_ahn4": 10.0,
                    "b3_puntdichtheid_ahn5": None,
                    "b3_mutatie_ahn3_ahn4": 0.1,
                    "b3_mutatie_ahn4_ahn5": None,
                    "b3_nodata_fractie_ahn3": 0.0,
                    "b3_nodata_fractie_ahn4": 0.0,
                    "b3_nodata_fractie_ahn5": None,
                    "b3_nodata_radius_ahn3": 0.0,
                    "b3_nodata_radius_ahn4": 0.0,
                    "b3_nodata_radius_ahn5": None,
                },
                "geometry": [
                    {"type": "Solid", "lod": "0", "boundaries": []},
                    {"type": "Solid", "lod": "1.2", "boundaries": []},
                    {"type": "Solid", "lod": "1.3", "boundaries": []},
                    {"type": "Solid", "lod": "2.2", "boundaries": []},
                ],
            }
        },
        "vertices": [],
    }
    feature_path.write_text(json.dumps(content))
    return feature_path


def test_feature_evaluation_reads_reconstruction(tmp_path):
    """feature_evaluation scans reconstruction stage and produces reconstructed_features.csv."""
    tile_id = "10/434/716"
    pand_ids = [
        "NL.IMBAG.Pand.0307100000308298",
        "NL.IMBAG.Pand.0307100000368987",
    ]
    extra_input_id = "NL.IMBAG.Pand.9999999999999999"

    for pand_id in pand_ids:
        _make_reconstruction_feature(tmp_path, tile_id, pand_id)

    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)

    # Create output directory so feature_evaluation can write
    output_dir = tmp_path / "stages" / "export" / VERSION
    output_dir.mkdir(parents=True, exist_ok=True)

    mock_db = MagicMock()
    # Return reconstructed + one extra that was not reconstructed
    mock_db.connection.get_query.return_value = [
        (pand_ids[0],),
        (pand_ids[1],),
        (extra_input_id,),
    ]

    result_csv = feature_evaluation(file_store, mock_db, version)

    assert result_csv.exists()
    assert result_csv.name == "reconstructed_features.csv"

    with result_csv.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    row_ids = {row["id"] for row in rows}
    assert pand_ids[0] in row_ids
    assert pand_ids[1] in row_ids
    assert extra_input_id in row_ids

    # Check expected columns
    expected_cols = {
        "id",
        "identificatie",
        "lod_0",
        "lod_12",
        "lod_13",
        "lod_22",
        "has_geometry",
    }
    assert expected_cols.issubset(set(reader.fieldnames))

    # Reconstructed buildings should have geometry
    for row in rows:
        if row["id"] in pand_ids:
            assert row["has_geometry"] == "True"
            assert row["lod_0"] == "1"
            assert row["lod_22"] == "1"
        elif row["id"] == extra_input_id:
            assert row["has_geometry"] == "False"

    # not_reconstructed_buildings.txt should list the extra ID
    not_recon_file = output_dir / "not_reconstructed_buildings.txt"
    assert not_recon_file.exists()
    not_recon_ids = not_recon_file.read_text().strip().splitlines()
    assert extra_input_id in not_recon_ids
    assert pand_ids[0] not in not_recon_ids


def _make_quadtree_tsv(path: Path, tile_ids: list[str]) -> None:
    """Create a tyler-format quadtree.tsv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["id", "level", "nr_items", "leaf", "wkt"])
        for tid in tile_ids:
            writer.writerow([tid, "3", "10", "true", "POLYGON((0 0,1 0,1 1,0 1,0 0))"])


def _make_tile_files(tiles_dir: Path, tile_id: str) -> None:
    """Create placeholder tile files (.city.json, .gpkg, .obj)."""
    tile_dir = tiles_dir / tile_id
    tile_dir.mkdir(parents=True, exist_ok=True)
    lid = tile_id.replace("/", "-")
    (tile_dir / f"{lid}.city.json").write_text("{}")
    (tile_dir / f"{lid}.gpkg").write_bytes(b"")
    for suffix in ["-lod12.obj", "-lod13.obj", "-lod22.obj"]:
        (tile_dir / f"{lid}{suffix}").write_text("")


def test_export_index_reads_quadtree(tmp_path):
    """export_index parses quadtree.tsv and checks tile file existence."""
    tile_ids = ["10/434/716", "10/435/717"]
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)

    export_dir = tmp_path / "stages" / "export" / VERSION
    tiles_dir = export_dir / "tiles"

    _make_quadtree_tsv(export_dir / "quadtree.tsv", tile_ids)
    for tid in tile_ids:
        _make_tile_files(tiles_dir, tid)

    result_path = export_index(file_store, version)

    assert result_path.exists()
    assert result_path.name == "export_index.csv"

    with result_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == len(tile_ids)
    expected_cols = {"tile_id", "has_cityjson", "has_gpkg", "has_obj", "wkt"}
    assert expected_cols == set(reader.fieldnames)

    for row in rows:
        assert row["tile_id"] in tile_ids
        assert row["has_cityjson"] == "True"
        assert row["has_gpkg"] == "True"
        assert row["has_obj"] == "True"
        assert row["wkt"] == "POLYGON((0 0,1 0,1 1,0 1,0 0))"


def test_compressed_tiles(tmp_path):
    """compressed_tiles gzips CityJSON/GPKG and zips OBJ files, removing originals."""
    tile_id = "10/434/716"
    lid = tile_id.replace("/", "-")
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)

    export_dir = tmp_path / "stages" / "export" / VERSION
    tiles_dir = export_dir / "tiles"
    tile_dir = tiles_dir / tile_id
    tile_dir.mkdir(parents=True, exist_ok=True)

    # Create tile files
    cj_file = tile_dir / f"{lid}.city.json"
    gpkg_file = tile_dir / f"{lid}.gpkg"
    obj_file = tile_dir / f"{lid}-lod22.obj"
    mtl_file = tile_dir / f"{lid}-lod22.mtl"

    cj_file.write_text('{"type":"CityJSON"}')
    gpkg_file.write_bytes(b"fake-gpkg-content")
    obj_file.write_text("v 0 0 0")
    mtl_file.write_text("newmtl material")

    # Create export_index.csv as input
    export_index_path = export_dir / "export_index.csv"
    with export_index_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["tile_id", "has_cityjson", "has_gpkg", "has_obj", "wkt"])
        writer.writerow(
            [tile_id, "True", "True", "True", "POLYGON((0 0,1 0,1 1,0 1,0 0))"]
        )

    compressed_tiles(
        CompressionConfig(concurrency=1),
        file_store,
        version,
        export_index_path,
    )

    # Compressed files exist
    assert (tile_dir / f"{lid}.city.json.gz").exists()
    assert (tile_dir / f"{lid}.gpkg.gz").exists()
    assert (tile_dir / f"{lid}-obj.zip").exists()

    # Original uncompressed files are deleted
    assert not cj_file.exists()
    assert not gpkg_file.exists()
    assert not obj_file.exists()
    assert not mtl_file.exists()
