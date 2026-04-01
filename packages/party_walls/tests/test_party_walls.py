import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import cjindex
from bag3d.common.testing import build_asset_context_for
from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    building_surfaces,
)
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.cjindex import CityIndexResource


def _make_feature_bytes(pand_id: str) -> bytes:
    """Create a minimal CityJSONFeature payload for testing."""
    content = {
        "type": "CityJSONFeature",
        "id": pand_id,
        "CityObjects": {
            pand_id: {
                "type": "Building",
                "attributes": {},
                "geometry": [],
            }
        },
        "vertices": [],
    }
    return json.dumps(content).encode()


def _make_refs_and_index(tmp_path: Path, pand_ids: list[str], tile_id: str):
    """Create FeatureRef mocks backed by a tile-level reconstruction source."""
    source_path = (
        tmp_path / "stages" / "reconstruction" / tile_id / "reconstruct.ndjson"
    )
    refs_with_bytes = []
    for pand_id in pand_ids:
        ref = cjindex.FeatureRef(feature_id=pand_id, source_path=str(source_path))
        refs_with_bytes.append((ref, _make_feature_bytes(pand_id)))
    return refs_with_bytes


def _stub_open_index(refs_with_bytes: list) -> MagicMock:
    """Build a mock OpenedIndex that serves the given (ref, bytes) pairs."""
    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_ref_count.return_value = len(refs_with_bytes)

    refs = [r for r, _ in refs_with_bytes]
    feature_map = {r.feature_id: json.loads(b) for r, b in refs_with_bytes}

    def feature_ref_page(offset, limit):
        return refs[offset : offset + limit]

    mock_idx.feature_ref_page.side_effect = feature_ref_page
    mock_idx.read_feature_json.side_effect = lambda ref: feature_map[ref.feature_id]
    mock_idx.get_json.side_effect = lambda fid: feature_map.get(fid)
    return mock_idx


def test_building_surfaces_empty_index(tmp_path):
    """building_surfaces returns [] when the reconstruction index has no features."""
    file_store = FileStoreResource(root_dir=str(tmp_path))
    mock_db = MagicMock()
    resource = CityIndexResource(
        dataset_dir=str(tmp_path / "stages" / "reconstruction")
    )

    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_ref_count.return_value = 0
    mock_idx.feature_ref_page.return_value = []

    with patch(
        "bag3d.party_walls.assets.party_walls.open_ready_index", return_value=mock_idx
    ):
        with build_asset_context_for(building_surfaces) as context:
            result = building_surfaces(
                context,
                PartyWallsConfig(),
                resource,
                mock_db,
                file_store,
            )

    assert result == []
    mock_db.connection.get_dict.assert_not_called()


def test_building_surfaces_writes_computed_features(tmp_path, monkeypatch):
    """building_surfaces computes shared walls for tile features and writes outputs."""
    tile_id = "10/434/716"
    file_store = FileStoreResource(root_dir=str(tmp_path))
    target_id = "NL.IMBAG.Pand.0307100000308298"
    adjacent_id = "NL.IMBAG.Pand.0307100000368987"

    resource = CityIndexResource(
        dataset_dir=str(tmp_path / "stages" / "reconstruction")
    )
    refs_with_bytes = _make_refs_and_index(tmp_path, [target_id, adjacent_id], tile_id)
    mock_idx = _stub_open_index(refs_with_bytes)

    def fake_shared_walls(target, adjacent):
        return {"b3_opp_scheidingsmuur": 12.5, "b3_opp_buitenmuur": 8.0}

    def fake_write_cityjsonfeature(raw_feature, result, output_path):
        building_id = raw_feature["id"]
        raw_feature["CityObjects"][building_id]["attributes"].update(result)
        output_path.write_text(json.dumps(raw_feature))

    monkeypatch.setattr(
        "bag3d.party_walls.assets.party_walls.shared_walls",
        fake_shared_walls,
    )
    monkeypatch.setattr(
        "bag3d.party_walls.assets.party_walls.write_cityjsonfeature",
        fake_write_cityjsonfeature,
    )

    mock_db = MagicMock()
    mock_db.connection.get_dict.return_value = [
        {"identificatie": target_id, "adjacent_identificatie": adjacent_id},
        {"identificatie": adjacent_id, "adjacent_identificatie": target_id},
    ]

    # Workers open their own index via cjindex.OpenedIndex.open(dataset_dir)
    import cjindex as _cjindex

    monkeypatch.setattr(_cjindex.OpenedIndex, "open", lambda *a, **kw: mock_idx)

    with patch(
        "bag3d.party_walls.assets.party_walls.open_ready_index", return_value=mock_idx
    ):
        with build_asset_context_for(building_surfaces) as context:
            result = building_surfaces(
                context,
                PartyWallsConfig(concurrency=1),
                resource,
                mock_db,
                file_store,
            )

    output_paths = cast(list[Path], result)
    assert len(output_paths) == 2

    for output_path in output_paths:
        assert output_path.exists()
        data = json.loads(output_path.read_text())
        pand_id = output_path.name.removesuffix(".city.jsonl")
        assert (
            data["CityObjects"][pand_id]["attributes"]["b3_opp_scheidingsmuur"] == 12.5
        )
        assert data["CityObjects"][pand_id]["attributes"]["b3_opp_buitenmuur"] == 8.0

    mock_db.connection.get_dict.assert_called_once()


def test_building_surfaces_writes_profile_summary(tmp_path, monkeypatch):
    """building_surfaces can emit a minimal profiling summary artifact."""
    tile_id = "10/434/716"
    file_store = FileStoreResource(root_dir=str(tmp_path))
    target_id = "NL.IMBAG.Pand.0307100000308298"
    adjacent_id = "NL.IMBAG.Pand.0307100000368987"

    resource = CityIndexResource(
        dataset_dir=str(tmp_path / "stages" / "reconstruction")
    )
    refs_with_bytes = _make_refs_and_index(tmp_path, [target_id, adjacent_id], tile_id)
    mock_idx = _stub_open_index(refs_with_bytes)

    def fake_shared_walls(target, adjacent):
        return {"b3_opp_scheidingsmuur": 12.5, "b3_opp_buitenmuur": 8.0}

    def fake_write_cityjsonfeature(raw_feature, result, output_path):
        building_id = raw_feature["id"]
        raw_feature["CityObjects"][building_id]["attributes"].update(result)
        output_path.write_text(json.dumps(raw_feature))

    monkeypatch.setattr(
        "bag3d.party_walls.assets.party_walls.shared_walls",
        fake_shared_walls,
    )
    monkeypatch.setattr(
        "bag3d.party_walls.assets.party_walls.write_cityjsonfeature",
        fake_write_cityjsonfeature,
    )

    mock_db = MagicMock()
    mock_db.connection.get_dict.return_value = [
        {"identificatie": target_id, "adjacent_identificatie": adjacent_id},
        {"identificatie": adjacent_id, "adjacent_identificatie": target_id},
    ]

    import cjindex as _cjindex

    monkeypatch.setattr(_cjindex.OpenedIndex, "open", lambda *a, **kw: mock_idx)

    with patch(
        "bag3d.party_walls.assets.party_walls.open_ready_index", return_value=mock_idx
    ):
        with build_asset_context_for(building_surfaces) as context:
            _ = building_surfaces(
                context,
                PartyWallsConfig(concurrency=1, profile=True),
                resource,
                mock_db,
                file_store,
            )

    profile_path = (
        tmp_path
        / "stages"
        / "party_walls"
        / "_profiling"
        / "building_surfaces_profile.json"
    )
    assert profile_path.exists()

    summary = json.loads(profile_path.read_text())
    assert summary["buildings_profiled"] == 2
    assert summary["files_written"] == 2
    assert summary["adjacency_rows"] == 2
    assert summary["max_workers"] == 1
    assert len(summary["top_slowest_buildings"]) == 2
