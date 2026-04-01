"""Integration test: party_walls stage -> floors_estimation stage.

Runs save_cjfiles directly to verify stage-to-stage handoff
and that upstream attributes survive enrichment.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import cjindex
import pandas as pd

from bag3d.common.resources.cjindex import CityIndexResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.floors_estimation.assets.floors_estimation import (
    FloorsEstimationIOConfig,
    save_cjfiles,
)


def _make_party_walls_feature(path: Path, pand_id: str) -> None:
    """Create a CityJSONFeature with party wall attributes (upstream output)."""
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


def test_party_walls_to_floors_estimation(tmp_path):
    """save_cjfiles reads party_walls stage data and writes floors_estimation output."""
    pand_ids = [
        "NL.IMBAG.Pand.0307100000377456",
        "NL.IMBAG.Pand.0307100000364333",
    ]

    # Seed party_walls stage files (z/x/y/<pand>.city.jsonl)
    for pand_id in pand_ids:
        feature_path = (
            tmp_path
            / "stages"
            / "party_walls"
            / "0"
            / "0"
            / "0"
            / f"{pand_id}.city.jsonl"
        )
        _make_party_walls_feature(feature_path, pand_id)

    file_store = FileStoreResource(root_dir=str(tmp_path))
    resource = CityIndexResource(dataset_dir=str(tmp_path / "stages" / "party_walls"))

    refs = []
    bytes_map = {}
    for pand_id in pand_ids:
        feature_path = (
            tmp_path
            / "stages"
            / "party_walls"
            / "0"
            / "0"
            / "0"
            / f"{pand_id}.city.jsonl"
        )
        ref = cjindex.FeatureRef(feature_id=pand_id, source_path=str(feature_path))
        refs.append(ref)
        bytes_map[pand_id] = feature_path.read_bytes()

    mock_idx = MagicMock()
    mock_idx.status.return_value = MagicMock(needs_reindex=False)
    mock_idx.feature_ref_count.return_value = len(pand_ids)
    mock_idx.feature_ref_page.side_effect = lambda offset, limit: refs[
        offset : offset + limit
    ]
    mock_idx.read_feature_json.side_effect = lambda ref: json.loads(
        bytes_map[ref.feature_id]
    )
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

    # Verify output files at stages/floors_estimation/{z_level}/{pand}.city.jsonl
    floors_dir = tmp_path / "stages" / "floors_estimation"
    assert floors_dir.is_dir()

    for pand_id, expected_floors in [(pand_ids[0], 3), (pand_ids[1], 2)]:
        output_file = floors_dir / "0" / f"{pand_id}.city.jsonl"
        assert output_file.exists(), f"Missing output for {pand_id}"

        feature = json.loads(output_file.read_text())
        attrs = feature["CityObjects"][pand_id]["attributes"]

        # Floors estimation attribute was added
        assert attrs["b3_bouwlagen"] == expected_floors

        # Upstream party wall attributes are preserved
        assert attrs["b3_opp_scheidingsmuur"] == 10.0
        assert attrs["b3_opp_buitenmuur"] == 20.0
