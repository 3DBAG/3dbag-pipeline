import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

from bag3d.common.testing import build_asset_context_for
from bag3d.common.resources import nl_transform
from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    features_file_index,
    building_surfaces,
)
from bag3d.common.resources.files import FileStoreResource


def _make_feature_file(path, pand_id: str) -> None:
    """Create a minimal CityJSONFeature file for testing."""
    path.parent.mkdir(parents=True, exist_ok=True)
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
    path.write_text(json.dumps(content))


def _make_reconstruction_feature(root_dir: Path, tile_id: str, pand_id: str) -> Path:
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
    _make_feature_file(feature_path, pand_id)
    return feature_path


def test_features_file_index(tmp_path):
    """features_file_index maps pand_id -> path for all .city.jsonl files."""
    recon_dir = tmp_path / "stages" / "reconstruction"
    # Create z/x/y/objects/<pand_id>/reconstruct/<pand_id>.city.jsonl structure
    pand_ids = [
        "NL.IMBAG.Pand.0307100000308298",
        "NL.IMBAG.Pand.0307100000368987",
    ]
    for pand_id in pand_ids:
        feature_path = (
            recon_dir
            / "0"
            / "0"
            / "0"
            / "objects"
            / pand_id
            / "reconstruct"
            / f"{pand_id}.city.jsonl"
        )
        _make_feature_file(feature_path, pand_id)

    file_store = FileStoreResource(root_dir=str(tmp_path))
    result = features_file_index(PartyWallsConfig(), file_store)

    assert isinstance(result, dict)
    assert len(result) == len(pand_ids)
    for pand_id in pand_ids:
        assert pand_id in result
        assert result[pand_id].exists()


def test_building_surfaces_empty_index(tmp_path):
    """building_surfaces returns [] when features_file_index is empty."""
    file_store = FileStoreResource(root_dir=str(tmp_path))
    mock_db = MagicMock()

    with build_asset_context_for(building_surfaces) as context:
        result = building_surfaces(
            context,
            PartyWallsConfig(),
            {},  # Empty index
            mock_db,
            file_store,
            nl_transform,
        )

    assert result == []
    # DB should not be queried when index is empty
    mock_db.connection.get_dict.assert_not_called()


def test_building_surfaces_writes_computed_features(tmp_path, monkeypatch):
    """building_surfaces computes shared walls for tile features and writes outputs."""
    tile_id = "10/434/716"
    file_store = FileStoreResource(root_dir=str(tmp_path))
    target_id = "NL.IMBAG.Pand.0307100000308298"
    adjacent_id = "NL.IMBAG.Pand.0307100000368987"

    target_path = _make_reconstruction_feature(tmp_path, tile_id, target_id)
    adjacent_path = _make_reconstruction_feature(tmp_path, tile_id, adjacent_id)

    shared_walls_calls = []

    def fake_shared_walls(target, adjacent):
        shared_walls_calls.append((target, adjacent))
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
        {
            "identificatie": target_id,
            "adjacent_identificatie": adjacent_id,
        },
        {
            "identificatie": adjacent_id,
            "adjacent_identificatie": target_id,
        },
    ]

    with build_asset_context_for(building_surfaces) as context:
        result = building_surfaces(
            context,
            PartyWallsConfig(concurrency=1),
            {
                target_id: target_path,
                adjacent_id: adjacent_path,
            },
            mock_db,
            file_store,
            nl_transform,
        )

    output_paths = cast(list[Path], result)

    assert output_paths == [
        tmp_path / "stages" / "party_walls" / tile_id / f"{target_id}.city.jsonl",
        tmp_path / "stages" / "party_walls" / tile_id / f"{adjacent_id}.city.jsonl",
    ]
    # shared_walls call count cannot be asserted via a local list with ProcessPoolExecutor
    # (worker mutations are not visible in the parent process); correctness is verified
    # via output file content below.

    target_output = json.loads(output_paths[0].read_text())
    assert (
        target_output["CityObjects"][target_id]["attributes"]["b3_opp_scheidingsmuur"]
        == 12.5
    )
    assert (
        target_output["CityObjects"][target_id]["attributes"]["b3_opp_buitenmuur"]
        == 8.0
    )
    mock_db.connection.get_dict.assert_called_once()


def test_building_surfaces_writes_profile_summary(tmp_path, monkeypatch):
    """building_surfaces can emit a minimal profiling summary artifact."""
    tile_id = "10/434/716"
    file_store = FileStoreResource(root_dir=str(tmp_path))
    target_id = "NL.IMBAG.Pand.0307100000308298"
    adjacent_id = "NL.IMBAG.Pand.0307100000368987"

    target_path = _make_reconstruction_feature(tmp_path, tile_id, target_id)
    adjacent_path = _make_reconstruction_feature(tmp_path, tile_id, adjacent_id)

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
        {
            "identificatie": target_id,
            "adjacent_identificatie": adjacent_id,
        },
        {
            "identificatie": adjacent_id,
            "adjacent_identificatie": target_id,
        },
    ]

    with build_asset_context_for(building_surfaces) as context:
        _ = building_surfaces(
            context,
            PartyWallsConfig(concurrency=1, profile=True),
            {
                target_id: target_path,
                adjacent_id: adjacent_path,
            },
            mock_db,
            file_store,
            nl_transform,
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
