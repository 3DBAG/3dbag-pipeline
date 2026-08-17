import json
from pathlib import Path
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
    content = {
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
    return json.dumps(content).encode()


def _stream_items(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


_SOURCE_PATHS: dict[str, str] = {}


def _make_refs_and_index(tmp_path: Path, pand_ids: list[str], tile_id: str):
    """Create PackageRef mocks backed by a tile-level reconstruction source."""
    source_path = (
        tmp_path / "stages" / "reconstruction" / tile_id / "reconstruct.ndjson"
    )
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(
        b"\n".join(
            [
                _make_root_bytes(),
                *[_make_feature_bytes(pand_id) for pand_id in pand_ids],
            ]
        )
        + b"\n"
    )
    refs_with_bytes = []
    for pand_id in pand_ids:
        ref = cityjson_index.PackageRef(record_id=0, model_id=pand_id)
        refs_with_bytes.append((ref, _make_feature_bytes(pand_id)))
    _SOURCE_PATHS.clear()
    _SOURCE_PATHS.update({pand_id: str(source_path) for pand_id in pand_ids})
    return refs_with_bytes


def _stub_open_index(refs_with_bytes: list) -> MagicMock:
    """Build a mock OpenedIndex that serves the given (ref, bytes) pairs."""
    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_bounds_summary.return_value.package_count = len(refs_with_bytes)

    refs = [r for r, _ in refs_with_bytes]
    feature_map = {r.model_id: json.loads(b) for r, b in refs_with_bytes}

    mock_idx.package_ref_page_after_record_id.side_effect = lambda after, limit: (
        refs if after is None else []
    )
    mock_idx.package_source_paths.side_effect = lambda page: [
        _SOURCE_PATHS[ref.model_id] for ref in page
    ]
    mock_idx.read_package.side_effect = lambda ref: feature_map[ref.model_id]
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
    mock_idx.feature_bounds_summary.return_value.package_count = 0
    mock_idx.package_ref_page_after_record_id.return_value = []

    with (
        patch(
            "bag3d.party_walls.assets.party_walls.open_ready_index",
            return_value=mock_idx,
        ),
        build_asset_context_for(building_surfaces) as context,
    ):
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
        {"identificatie": target_id, "adjacent_identificatie": adjacent_id},
        {"identificatie": adjacent_id, "adjacent_identificatie": target_id},
    ]

    # Workers open their own index via cityjson_index.OpenedIndex.open(dataset_dir)
    import cityjson_index as _cityjson_index

    monkeypatch.setattr(_cityjson_index.OpenedIndex, "open", lambda *a, **kw: mock_idx)

    with (
        patch(
            "bag3d.party_walls.assets.party_walls.open_ready_index",
            return_value=mock_idx,
        ),
        build_asset_context_for(building_surfaces) as context,
    ):
        result = building_surfaces(
            context,
            PartyWallsConfig(concurrency=1),
            resource,
            mock_db,
            file_store,
        )

    output_paths = cast(list[Path], result)
    assert len(output_paths) == 1

    for output_path in output_paths:
        assert output_path.exists()
        items = _stream_items(output_path)
        assert len(items) == 3
        assert items[0]["type"] == "CityJSON"
        assert items[0]["metadata"]["title"] == "reconstruction-root"
        for data in items[1:]:
            pand_id = data["id"]
            assert (
                data["CityObjects"][pand_id]["attributes"]["b3_opp_scheidingsmuur"]
                == 12.5
            )
            assert (
                data["CityObjects"][pand_id]["attributes"]["b3_opp_buitenmuur"] == 8.0
            )

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
        {"identificatie": target_id, "adjacent_identificatie": adjacent_id},
        {"identificatie": adjacent_id, "adjacent_identificatie": target_id},
    ]

    import cityjson_index as _cityjson_index

    monkeypatch.setattr(_cityjson_index.OpenedIndex, "open", lambda *a, **kw: mock_idx)

    with (
        patch(
            "bag3d.party_walls.assets.party_walls.open_ready_index",
            return_value=mock_idx,
        ),
        build_asset_context_for(building_surfaces) as context,
    ):
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
    assert summary["features_written"] == 2
    assert summary["tiles_written"] == 1
    assert summary["adjacency_rows"] == 2
    assert summary["max_workers"] == 1
    assert len(summary["top_slowest_buildings"]) == 2
