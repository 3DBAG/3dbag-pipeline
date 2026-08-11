"""Integration tests for the export stage data flow.

Tests that export assets correctly read from reconstruction/export stages
and produce the expected output files.
"""

import csv
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

from bag3d.common.resources.cjindex import CityIndexResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.export.assets.export import metadata as metadata_module
from bag3d.export.assets.export.tile import (
    TylerConfig,
    merged_quadtree,
    reconstruction_output_gpkg,
)
from bag3d.export.assets.export.metadata import export_index, feature_evaluation
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
    """feature_evaluation reads reconstruction stage via cjindex and produces reconstructed_features.csv."""
    tile_id = "10/434/716"
    pand_ids = [
        "NL.IMBAG.Pand.0307100000308298",
        "NL.IMBAG.Pand.0307100000368987",
    ]
    extra_input_id = "NL.IMBAG.Pand.9999999999999999"

    paths = [
        _make_reconstruction_feature(tmp_path, tile_id, pand_id) for pand_id in pand_ids
    ]

    # Build mock PackageRef objects backed by the real on-disk files
    refs_with_bytes = []
    for pand_id, path in zip(pand_ids, paths):
        ref = MagicMock()
        ref.model_id = pand_id
        ref.source_path = str(path)
        refs_with_bytes.append((ref, path.read_bytes()))

    refs = [r for r, _ in refs_with_bytes]
    bytes_map = {r.model_id: b for r, b in refs_with_bytes}

    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_bounds_summary.return_value.package_count = len(refs)
    mock_idx.package_ref_page_after_record_id.side_effect = lambda after, limit: (
        refs if after is None else []
    )
    mock_idx.read_package.side_effect = lambda ref: json.loads(bytes_map[ref.model_id])

    recon_resource = CityIndexResource(
        dataset_dir=str(tmp_path / "stages" / "reconstruction")
    )

    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)
    output_dir = tmp_path / "stages" / "export" / VERSION

    mock_db = MagicMock()
    # Return reconstructed + one extra that was not reconstructed
    mock_db.connection.get_query.return_value = [
        (pand_ids[0],),
        (pand_ids[1],),
        (extra_input_id,),
    ]

    with patch(
        "bag3d.export.assets.export.metadata.open_ready_index",
        return_value=mock_idx,
    ):
        result_csv = cast(
            Path, feature_evaluation(file_store, mock_db, version, recon_resource)
        )

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
    assert reader.fieldnames is not None
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


def test_metadata_creates_versioned_export_directory(tmp_path, monkeypatch):
    """metadata writes metadata.json even when the versioned export dir is absent."""
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)

    mock_instance = MagicMock()
    mock_instance.fetch_materializations.return_value.records = []
    context = cast(object, SimpleNamespace(instance=mock_instance))

    monkeypatch.setattr(metadata_module, "_build_software_list", lambda: [])

    decorated_fn = cast(Any, metadata_module.metadata.op.compute_fn).decorated_fn
    result = decorated_fn(
        context,
        file_store,
        version,
    )

    output_path = tmp_path / "stages" / "export" / VERSION / "metadata.json"
    assert output_path.exists()
    assert cast(Path, result.value) == output_path

    metadata_json = json.loads(output_path.read_text())
    assert metadata_json["dataQualityInfo"]["lineage"]["software"] == []


def _make_quadtree_tsv(path: Path, tile_ids: list[str]) -> None:
    """Create a tyler-format quadtree.tsv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["id", "level", "nr_items", "leaf", "wkt"])
        for tid in tile_ids:
            writer.writerow([tid, "3", "10", "true", "POLYGON((0 0,1 0,1 1,0 1,0 0))"])


def _make_tile_files(tiles_dir: Path, tile_id: str) -> None:
    """Create placeholder Tyler tile files."""
    basename = tiles_dir / tile_id
    basename.parent.mkdir(parents=True, exist_ok=True)
    (basename.with_suffix(".city.json")).write_text("{}")
    (basename.with_suffix(".gpkg")).write_bytes(b"")
    (basename.with_suffix(".obj")).write_text("")


def test_export_index_reads_quadtree(tmp_path):
    """export_index parses quadtree.tsv and checks tile file existence."""
    tile_ids = ["10/434/716", "10/435/717"]
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)

    export_dir = tmp_path / "stages" / "export" / VERSION
    tiles_dir = export_dir / "t"

    _make_quadtree_tsv(export_dir / "quadtree.tsv", tile_ids)
    for tid in tile_ids:
        _make_tile_files(tiles_dir, tid)

    result_path = cast(
        Path, export_index(file_store, version, export_dir / "quadtree.tsv")
    )

    assert result_path.exists()
    assert result_path.name == "export_index.csv"

    with result_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == len(tile_ids)
    expected_cols = {"tile_id", "has_cityjson", "has_gpkg", "has_obj", "wkt"}
    assert reader.fieldnames is not None
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
    tiles_dir = export_dir / "t"
    tile_dir = tiles_dir / tile_id
    tile_dir.parent.mkdir(parents=True, exist_ok=True)

    # Create tile files
    cj_file = tile_dir.with_suffix(".city.json")
    gpkg_file = tile_dir.with_suffix(".gpkg")
    obj_file = tile_dir.with_suffix(".obj")
    mtl_file = tile_dir.with_suffix(".mtl")

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
    assert tile_dir.with_suffix(".city.json.gz").exists()
    assert tile_dir.with_suffix(".gpkg.gz").exists()
    assert tile_dir.with_name(f"{lid}-obj.zip").exists()

    # Original uncompressed files are deleted
    assert not cj_file.exists()
    assert not gpkg_file.exists()
    assert not obj_file.exists()
    assert not mtl_file.exists()


def test_reconstruction_output_gpkg_exports_quadtree(tmp_path):
    """The mocked Tyler runner produces both the export directory and quadtree asset."""
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version=VERSION)
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        json.dumps({"identificationInfo": {"citation": {"edition": VERSION}}})
    )

    runner = MagicMock()

    def run(command, *, exe_name, cwd, logger):
        assert exe_name == "tyler"
        assert "--debug-dump-grid" in command
        debug_dir = Path(cwd) / "debug"
        debug_dir.mkdir()
        (debug_dir / "quadtree_level-3.tsv").write_text(
            "node_id\tnode_level\tnr_items\twkt\n3/434/716\t3\t10\tPOLYGON((0 0,1 0,1 1,0 1,0 0))\n"
        )

    runner.run.side_effect = run
    tyler = SimpleNamespace(runner=runner)

    gpkg_output = reconstruction_output_gpkg(
        TylerConfig(concurrency=1),
        metadata_path,
        tyler,
        file_store,
        version,
        MagicMock(),
    )

    quadtree_output = merged_quadtree(gpkg_output)
    assert gpkg_output == tmp_path / "stages" / "export" / VERSION
    assert quadtree_output == gpkg_output / "debug" / "quadtree.tsv"
    assert quadtree_output.is_file()
