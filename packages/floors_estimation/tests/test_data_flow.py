"""Integration test: party_walls stage -> floors_estimation stage.

Runs save_cjfiles directly to verify stage-to-stage handoff
and that upstream attributes survive enrichment.
"""

import json
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


def test_party_walls_to_floors_estimation(tmp_path):
    """save_cjfiles reads party_walls stage data and writes floors_estimation output."""
    pand_ids = [
        "NL.IMBAG.Pand.0307100000377456",
        "NL.IMBAG.Pand.0307100000364333",
    ]

    tile_id = "10/434/716"
    feature_path = tmp_path / "stages" / "party_walls" / tile_id / "716.city.jsonl"
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [json.dumps(_make_root())]

    # Seed party_walls stage files (z/x/y/y.city.jsonl)
    for pand_id in pand_ids:
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
        lines.append(json.dumps(content))

    feature_path.write_text("\n".join(lines), encoding="utf-8")

    file_store = FileStoreResource(root_dir=str(tmp_path))
    resource = CityIndexResource(dataset_dir=str(tmp_path / "stages" / "party_walls"))

    refs = []
    bytes_map = {}
    for pand_id in pand_ids:
        ref = cityjson_index.PackageRef(record_id=0, model_id=pand_id)
        refs.append(ref)
        bytes_map[pand_id] = json.dumps(
            {
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
        ).encode()

    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_bounds_summary.return_value.package_count = len(pand_ids)
    mock_idx.package_ref_page_after_record_id.side_effect = lambda after, limit: (
        refs if after is None else []
    )
    mock_idx.package_source_paths.side_effect = lambda page: [
        str(feature_path) for _ in page
    ]
    mock_idx.read_package.side_effect = lambda ref: json.loads(bytes_map[ref.model_id])
    mock_idx.get_json.side_effect = lambda fid: (
        json.loads(bytes_map[fid]) if fid in bytes_map else None
    )

    inferenced_floors = pd.DataFrame(
        {
            "identificatie": [pand_ids[0], pand_ids[1]],
            "floors_int": [3.0, 2.0],
        }
    ).set_index("identificatie")

    with patch(
        "bag3d.floors_estimation.assets.floors_estimation.open_ready_index",
        return_value=mock_idx,
    ):
        save_cjfiles(
            FloorsEstimationIOConfig(concurrency=1),
            inferenced_floors,
            resource,
            file_store,
        )

    # Verify output file at stages/floors_estimation/{z}/{x}/{y}/{y}.city.jsonl
    floors_dir = tmp_path / "stages" / "floors_estimation"
    assert floors_dir.is_dir()

    output_file = floors_dir / tile_id / "716.city.jsonl"
    assert output_file.exists()

    features = {}
    lines = output_file.read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[0])["type"] == "CityJSON"
    for line in lines[1:]:
        feature = json.loads(line)
        features[feature["id"]] = feature

    assert (
        features[pand_ids[0]]["CityObjects"][pand_ids[0]]["attributes"]["b3_bouwlagen"]
        == 3
    )
    assert (
        features[pand_ids[1]]["CityObjects"][pand_ids[1]]["attributes"]["b3_bouwlagen"]
        == 2
    )

    for pand_id in pand_ids:
        attrs = features[pand_id]["CityObjects"][pand_id]["attributes"]
        assert attrs["b3_opp_scheidingsmuur"] == 10.0
        assert attrs["b3_opp_buitenmuur"] == 20.0
