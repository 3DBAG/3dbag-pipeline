import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import cityjson_index
import pandas as pd
from bag3d.common.resources.cjindex import CityIndexResource
from bag3d.common.resources.files import FileStoreResource

from bag3d.floors_estimation.assets.floors_estimation import (
    FloorsEstimationIOConfig,
    save_cjfiles,
)


def _make_root() -> dict:
    return {
        "type": "CityJSON",
        "version": "2.0",
        "transform": {
            "scale": [0.001, 0.001, 0.001],
            "translate": [100.0, 200.0, 300.0],
        },
        "metadata": {"title": "party-walls-root"},
        "CityObjects": {},
        "vertices": [],
    }


_SOURCE_PATHS: dict[str, str] = {}


def _make_refs(tmp_path: Path, pand_ids: list[str], tile_id: str) -> list:
    """Create on-disk party_walls features and return matching mock PackageRef list."""
    tile_leaf = tile_id.split("/")[-1]
    feature_path = (
        tmp_path / "stages" / "party_walls" / tile_id / f"{tile_leaf}.city.jsonl"
    )
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    refs_with_bytes = []
    lines: list[str] = [json.dumps(_make_root())]
    for pand_id in pand_ids:
        feature = {
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
        lines.append(json.dumps(feature))
        ref = cityjson_index.PackageRef(record_id=0, model_id=pand_id)
        refs_with_bytes.append((ref, json.dumps(feature).encode()))
    feature_path.write_text("\n".join(lines))
    _SOURCE_PATHS.clear()
    _SOURCE_PATHS.update({pand_id: str(feature_path) for pand_id in pand_ids})
    return refs_with_bytes


def _stub_index(refs_with_bytes: list) -> MagicMock:
    """Build a mock OpenedIndex from a list of (ref, bytes) pairs."""
    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_bounds_summary.return_value.package_count = len(refs_with_bytes)
    refs = [r for r, _ in refs_with_bytes]
    bytes_map = {r.model_id: b for r, b in refs_with_bytes}

    mock_idx.package_ref_page_after_record_id.side_effect = lambda after, limit: (
        refs if after is None else []
    )
    mock_idx.package_source_paths.side_effect = lambda page: [
        _SOURCE_PATHS[ref.model_id] for ref in page
    ]
    mock_idx.read_package.side_effect = lambda ref: json.loads(bytes_map[ref.model_id])
    return mock_idx


def test_save_cjfiles(tmp_path):
    """save_cjfiles writes the expected b3_bouwlagen values for each output feature."""
    pand_ids = [
        "NL.IMBAG.Pand.0307100000340455",
        "NL.IMBAG.Pand.0307100000351286",
        "NL.IMBAG.Pand.0307100000364333",
    ]
    tile_id = "10/434/716"
    refs_with_bytes = _make_refs(tmp_path, pand_ids, tile_id)
    mock_idx = _stub_index(refs_with_bytes)
    resource = CityIndexResource(dataset_dir=str(tmp_path / "stages" / "party_walls"))

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

    # Output is under stages/floors_estimation/{z}/{x}/{y}/{y}.city.jsonl
    output_file = tmp_path / "stages/floors_estimation/10/434/716/716.city.jsonl"
    lines = output_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    assert json.loads(lines[0])["type"] == "CityJSON"
    features = {}
    for line in lines[1:]:
        feature = json.loads(line)
        features[feature["id"]] = feature

    assert (
        features["NL.IMBAG.Pand.0307100000340455"]["CityObjects"][
            "NL.IMBAG.Pand.0307100000340455"
        ]["attributes"]["b3_bouwlagen"]
        == 3
    )
    assert (
        features["NL.IMBAG.Pand.0307100000351286"]["CityObjects"][
            "NL.IMBAG.Pand.0307100000351286"
        ]["attributes"]["b3_bouwlagen"]
        is None
    )
    assert (
        features["NL.IMBAG.Pand.0307100000364333"]["CityObjects"][
            "NL.IMBAG.Pand.0307100000364333"
        ]["attributes"]["b3_bouwlagen"]
        is None
    )
