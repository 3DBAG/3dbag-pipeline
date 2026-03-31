import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.cjindex import CityIndexResource
from bag3d.floors_estimation.assets.floors_estimation import (
    FloorsEstimationConfig,
    FloorsEstimationIOConfig,
    features_file_index,
    save_cjfiles,
)


def _make_party_walls_feature(path: Path, pand_id: str) -> None:
    """Create a minimal CityJSONFeature file in stages/party_walls/."""
    path.parent.mkdir(parents=True, exist_ok=True)
    content = {
        "type": "CityJSONFeature",
        "id": pand_id,
        "CityObjects": {
            pand_id: {
                "type": "Building",
                "attributes": {
                    "b3_opp_scheidingsmuur": 10.0,
                    "b3_opp_buitenmuur": 20.0,
                },
                "geometry": [],
            }
        },
        "vertices": [],
    }
    path.write_text(json.dumps(content))


def _make_refs(tmp_path: Path, pand_ids: list[str]) -> list:
    """Create on-disk party_walls features and return matching mock FeatureRef list."""
    refs_with_bytes = []
    for pand_id in pand_ids:
        feature_path = (
            tmp_path / "stages" / "party_walls" / "0" / "0" / "0"
            / f"{pand_id}.city.jsonl"
        )
        _make_party_walls_feature(feature_path, pand_id)
        ref = MagicMock()
        ref.feature_id = pand_id
        ref.source_path = str(feature_path)
        refs_with_bytes.append((ref, feature_path.read_bytes()))
    return refs_with_bytes


def _stub_index(refs_with_bytes: list) -> MagicMock:
    """Build a mock OpenedIndex from a list of (ref, bytes) pairs."""
    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_ref_count.return_value = len(refs_with_bytes)
    refs = [r for r, _ in refs_with_bytes]
    bytes_map = {r.feature_id: b for r, b in refs_with_bytes}

    mock_idx.feature_ref_page.side_effect = lambda offset, limit: refs[offset: offset + limit]
    mock_idx.read_feature_bytes.side_effect = lambda ref: bytes_map[ref.feature_id]
    return mock_idx


def test_features_file_index(tmp_path):
    """features_file_index returns indexed_feature_count from the party_walls index."""
    pand_ids = [
        "NL.IMBAG.Pand.0307100000377456",
        "NL.IMBAG.Pand.0307100000364333",
    ]
    refs_with_bytes = _make_refs(tmp_path, pand_ids)
    mock_idx = _stub_index(refs_with_bytes)
    resource = CityIndexResource(
        dataset_dir=str(tmp_path / "stages" / "party_walls")
    )

    with patch(
        "bag3d.floors_estimation.assets.floors_estimation.open_ready_index",
        return_value=mock_idx,
    ):
        result = features_file_index(resource)

    assert isinstance(result, dict)
    assert result["indexed_feature_count"] == len(pand_ids)


def test_save_cjfiles(tmp_path):
    """save_cjfiles writes the expected b3_bouwlagen values for each output feature."""
    pand_ids = [
        "NL.IMBAG.Pand.0307100000340455",
        "NL.IMBAG.Pand.0307100000351286",
        "NL.IMBAG.Pand.0307100000364333",
    ]
    refs_with_bytes = _make_refs(tmp_path, pand_ids)
    mock_idx = _stub_index(refs_with_bytes)
    resource = CityIndexResource(
        dataset_dir=str(tmp_path / "stages" / "party_walls")
    )

    inferenced_floors = pd.DataFrame(
        {
            "identificatie": [
                "NL.IMBAG.Pand.0307100000340455",
                "NL.IMBAG.Pand.0307100000351286",
            ],
            "floors_int": [3.0, 8.0],
        }
    ).set_index("identificatie")

    file_store = FileStoreResource(root_dir=str(tmp_path))

    with patch(
        "bag3d.floors_estimation.assets.floors_estimation.open_ready_index",
        return_value=mock_idx,
    ):
        save_cjfiles(
            FloorsEstimationIOConfig(),
            inferenced_floors,
            resource,
            file_store,
        )

    # Output is under stages/floors_estimation/{last_tile_component}/{pand_id}.city.jsonl
    within_limit = json.loads(
        (
            tmp_path
            / "stages/floors_estimation/0/NL.IMBAG.Pand.0307100000340455.city.jsonl"
        ).read_text()
    )
    over_limit = json.loads(
        (
            tmp_path
            / "stages/floors_estimation/0/NL.IMBAG.Pand.0307100000351286.city.jsonl"
        ).read_text()
    )
    missing_prediction = json.loads(
        (
            tmp_path
            / "stages/floors_estimation/0/NL.IMBAG.Pand.0307100000364333.city.jsonl"
        ).read_text()
    )

    assert (
        within_limit["CityObjects"]["NL.IMBAG.Pand.0307100000340455"]["attributes"][
            "b3_bouwlagen"
        ]
        == 3
    )
    assert (
        over_limit["CityObjects"]["NL.IMBAG.Pand.0307100000351286"]["attributes"][
            "b3_bouwlagen"
        ]
        is None
    )
    assert (
        missing_prediction["CityObjects"]["NL.IMBAG.Pand.0307100000364333"][
            "attributes"
        ]["b3_bouwlagen"]
        is None
    )
