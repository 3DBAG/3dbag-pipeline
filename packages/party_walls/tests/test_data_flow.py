"""Integration test: reconstruction stage -> party_walls stage.

Chains features_file_index -> party_walls_nl to verify stage-to-stage handoff.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

from dagster import build_asset_context

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    features_file_index,
    party_walls_nl,
)


def _make_feature_file(path: Path, pand_id: str) -> None:
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


def test_reconstruction_to_party_walls(tmp_path, monkeypatch):
    """Chain features_file_index -> party_walls_nl: reconstruction stage feeds party_walls stage."""
    tile_id = "10/434/716"
    target_id = "NL.IMBAG.Pand.0307100000308298"
    adjacent_id = "NL.IMBAG.Pand.0307100000368987"

    # Seed reconstruction stage files
    for pand_id in (target_id, adjacent_id):
        feature_path = (
            tmp_path
            / "stages"
            / "reconstruction"
            / tile_id
            / "objects"
            / pand_id
            / "reconstruct"
            / f"{pand_id}.city.jsonl"
        )
        _make_feature_file(feature_path, pand_id)

    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version="test_version")

    # Step 1: features_file_index reads from reconstruction stage
    index = features_file_index(PartyWallsConfig(concurrency=1), file_store)

    assert isinstance(index, dict)
    assert len(index) == 2
    for pand_id in (target_id, adjacent_id):
        assert pand_id in index
        assert "stages/reconstruction" in str(index[pand_id])

    # Step 2: party_walls_nl consumes the index and writes to party_walls stage
    shared_walls_calls = []

    def fake_shared_walls(target, adjacent):
        shared_walls_calls.append((target, adjacent))
        return {"b3_opp_scheidingsmuur": 12.5, "b3_opp_buitenmuur": 8.0}

    def fake_write_cityjsonfeature(raw_feature, result, output_path):
        building_id = raw_feature["id"]
        raw_feature["CityObjects"][building_id]["attributes"].update(result)
        output_path.parent.mkdir(parents=True, exist_ok=True)
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

    with build_asset_context(partition_key=tile_id) as context:
        output_paths = party_walls_nl(
            context,
            PartyWallsConfig(concurrency=1),
            index,
            mock_db,
            file_store,
            version,
        )

    # Verify output files exist at stages/party_walls/{tile_id}/
    party_walls_dir = tmp_path / "stages" / "party_walls" / tile_id
    assert party_walls_dir.is_dir()
    assert len(output_paths) == 2

    for pand_id in (target_id, adjacent_id):
        output_file = party_walls_dir / f"{pand_id}.city.jsonl"
        assert output_file.exists(), f"Missing output for {pand_id}"

        feature = json.loads(output_file.read_text())
        attrs = feature["CityObjects"][pand_id]["attributes"]
        assert attrs["b3_opp_scheidingsmuur"] == 12.5
        assert attrs["b3_opp_buitenmuur"] == 8.0

    # shared_walls was called for both buildings
    assert len(shared_walls_calls) == 2
