import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

from dagster import build_asset_context
from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    features_file_index,
    adjacency_wall_surfaces,
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


def test_party_walls_nl_empty_tile(tmp_path):
    """adjacency_wall_surfaces returns [] when no features are found for the tile."""
    from bag3d.common.resources.version import ReleaseVersionResource

    tile_id = "10/434/716"
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version="test_version")

    # features_file_index has features only for a different tile
    recon_dir = tmp_path / "stages" / "reconstruction"
    pand_id = "NL.IMBAG.Pand.0307100000308298"
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

    mock_db = MagicMock()

    with build_asset_context(partition_key=tile_id) as context:
        result = adjacency_wall_surfaces(
            context,
            PartyWallsConfig(),
            {pand_id: feature_path},
            mock_db,
            file_store,
            version,
        )

    assert result == []
    # DB should not be queried when tile is empty
    mock_db.connection.get_dict.assert_not_called()


def test_party_walls_nl_writes_computed_features(tmp_path, version, monkeypatch):
    """adjacency_wall_surfaces computes shared walls for tile features and writes outputs."""
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
        }
    ]

    with build_asset_context(partition_key=tile_id) as context:
        result = adjacency_wall_surfaces(
            context,
            PartyWallsConfig(concurrency=1),
            {
                target_id: target_path,
                adjacent_id: adjacent_path,
            },
            mock_db,
            file_store,
            version,
        )

    output_paths = cast(list[Path], result)

    assert output_paths == [
        tmp_path / "stages" / "party_walls" / tile_id / f"{target_id}.city.jsonl",
        tmp_path / "stages" / "party_walls" / tile_id / f"{adjacent_id}.city.jsonl",
    ]
    assert len(shared_walls_calls) == 2
    assert len(shared_walls_calls[0][1]) == 1

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
