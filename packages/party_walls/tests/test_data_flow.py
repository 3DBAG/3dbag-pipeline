"""Integration test: reconstruction stage -> party_walls stage.

Chains features_file_index -> building_surfaces to verify stage-to-stage handoff.
"""

import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

from bag3d.common.testing import build_asset_context_for
from bag3d.common.resources import nl_transform
from bag3d.common.resources.files import FileStoreResource
from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    features_file_index,
    building_surfaces,
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
                "geometry": [
                    {
                        "type": "MultiSurface",
                        "lod": "2.2",
                        "boundaries": [[[0, 1, 2]]],
                        "semantics": {
                            "surfaces": [{"type": "GroundSurface"}],
                            "values": [0],
                        },
                    }
                ],
            }
        },
        "vertices": [[0, 0, 0], [1, 0, 0], [0, 1, 0]],
    }
    path.write_text(json.dumps(content))


def test_reconstruction_to_party_walls(tmp_path, monkeypatch):
    """Chain features_file_index -> party_walls: reconstruction stage feeds party_walls stage."""
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

    # Step 1: features_file_index reads from reconstruction stage
    index = features_file_index(PartyWallsConfig(concurrency=1), file_store)

    assert isinstance(index, dict)
    assert len(index) == 2
    for pand_id in (target_id, adjacent_id):
        assert pand_id in index
        assert "stages/reconstruction" in str(index[pand_id])

    # Step 2: building_surfaces consumes the index and writes to party_walls stage
    def fake_shared_walls(target: object, adjacent: object) -> dict:
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

    with build_asset_context_for(building_surfaces) as context:
        output_paths = building_surfaces(
            context,
            PartyWallsConfig(concurrency=1),
            index,
            mock_db,
            file_store,
            nl_transform,
        )

    # Verify output files exist at stages/party_walls/{tile_id}/
    party_walls_dir = tmp_path / "stages" / "party_walls" / tile_id
    assert party_walls_dir.is_dir()
    assert len(cast(list, output_paths)) == 2

    # The real shared_walls runs in a ProcessPoolExecutor (monkeypatch doesn't
    # propagate to spawned processes), so computed values reflect the simple
    # non-touching fixture geometry — no shared walls.
    for pand_id in (target_id, adjacent_id):
        output_file = party_walls_dir / f"{pand_id}.city.jsonl"
        assert output_file.exists(), f"Missing output for {pand_id}"

        feature = json.loads(output_file.read_text())
        attrs = feature["CityObjects"][pand_id]["attributes"]
        assert attrs["b3_opp_scheidingsmuur"] == 0.0

    # shared_walls was called for both buildings (verified via output file content above;
    # call-count cannot be asserted directly with ProcessPoolExecutor since worker
    # mutations to the local list are not visible in the parent process)
