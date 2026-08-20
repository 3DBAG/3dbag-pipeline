"""Integration test: reconstruction stage -> party_walls stage.

Runs building_surfaces directly to verify stage-to-stage handoff.
"""

import json
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, patch

import cityjson_index
from bag3d.common.resources.cjindex import CityIndexResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.testing import build_asset_context_for

from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    building_surfaces,
)


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


def _make_root_bytes() -> bytes:
    return json.dumps(
        {
            "type": "CityJSON",
            "version": "2.0",
            "transform": {
                "scale": [0.001, 0.001, 0.001],
                "translate": [100.0, 200.0, 300.0],
            },
            "metadata": {"title": "reconstruction-root"},
            "CityObjects": {},
            "vertices": [],
        }
    ).encode()


def test_reconstruction_to_party_walls(tmp_path, monkeypatch):
    """building_surfaces reads reconstruction stage data and writes party_walls output."""
    tile_id = "10/434/716"
    target_id = "NL.IMBAG.Pand.0307100000308298"
    adjacent_id = "NL.IMBAG.Pand.0307100000368987"

    # Seed reconstruction stage refs
    source_path = (
        tmp_path / "stages" / "reconstruction" / tile_id / "reconstruct.ndjson"
    )
    source_path.parent.mkdir(parents=True, exist_ok=True)
    file_store = FileStoreResource(root_dir=str(tmp_path))
    resource = CityIndexResource(
        dataset_dir=str(tmp_path / "stages" / "reconstruction")
    )

    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_bounds_summary.return_value.package_count = 2

    refs = []
    feature_map = {}
    for pand_id in (target_id, adjacent_id):
        ref = cityjson_index.PackageRef(record_id=0, model_id=pand_id)
        refs.append(ref)
        feature_map[pand_id] = json.loads(_make_feature_bytes(pand_id))
    source_path.write_bytes(
        b"\n".join(
            [
                _make_root_bytes(),
                *[_make_feature_bytes(pand_id) for pand_id in feature_map],
            ]
        )
        + b"\n"
    )

    mock_idx.package_ref_page_after_record_id.side_effect = lambda after, limit: (
        refs if after is None else []
    )
    mock_idx.package_source_paths.side_effect = lambda page: [
        str(source_path) for _ in page
    ]
    mock_idx.read_package.side_effect = lambda ref: feature_map[ref.model_id]
    mock_idx.get_json.side_effect = lambda fid: feature_map.get(fid)

    import cityjson_index as _cityjson_index

    monkeypatch.setattr(_cityjson_index.OpenedIndex, "open", lambda *a, **kw: mock_idx)

    def fake_shared_walls(target: object, adjacent: object) -> SimpleNamespace:
        return SimpleNamespace(
            area_shared_wall=12.5,
            area_exterior_wall=8.0,
            area_ground=0.0,
            area_roof_flat=0.0,
            area_roof_sloped=0.0,
        )

    monkeypatch.setattr(
        "bag3d.party_walls.assets.party_walls.shared_walls",
        fake_shared_walls,
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

    with (
        patch(
            "bag3d.party_walls.assets.party_walls.open_ready_index",
            return_value=mock_idx,
        ),
        build_asset_context_for(building_surfaces) as context,
    ):
        output_paths = building_surfaces(
            context,
            PartyWallsConfig(concurrency=1),
            resource,
            mock_db,
            file_store,
        )

    # Verify output files exist at stages/party_walls/{tile_id}/
    party_walls_dir = tmp_path / "stages" / "party_walls" / tile_id
    assert party_walls_dir.is_dir()
    assert len(cast(list, output_paths)) == 1

    output_file = party_walls_dir / f"{tile_id.split('/')[-1]}.city.jsonl"
    assert output_file.exists(), "Missing output for tile"

    items = [
        json.loads(line)
        for line in output_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(items) == 3
    assert items[0]["type"] == "CityJSON"
    for feature in items[1:]:
        pand_id = feature["id"]
        attrs = feature["CityObjects"][pand_id]["attributes"]
        assert attrs["b3_opp_scheidingsmuur"] == 12.5
        assert attrs["b3_opp_buitenmuur"] == 8.0

    # shared_walls was called for both buildings (verified via output file content above;
    # call-count cannot be asserted directly with ProcessPoolExecutor since worker
    # mutations to the local list are not visible in the parent process)
