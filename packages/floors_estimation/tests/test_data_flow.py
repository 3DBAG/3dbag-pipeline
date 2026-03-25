"""Integration test: party_walls stage -> floors_estimation stage.

Chains features_file_index -> save_cjfiles to verify stage-to-stage handoff
and that upstream attributes survive enrichment.
"""

import json
from pathlib import Path

import pandas as pd

from bag3d.common.resources.files import FileStoreResource
from bag3d.floors_estimation.assets.floors_estimation import (
    FloorsEstimationConfig,
    FloorsEstimationIOConfig,
    features_file_index,
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
    """Chain features_file_index -> save_cjfiles: party_walls stage feeds floors_estimation stage."""
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

    # Step 1: features_file_index reads from party_walls stage
    index = features_file_index(FloorsEstimationConfig(concurrency=1), file_store)

    assert isinstance(index, dict)
    assert len(index) == len(pand_ids)
    for pand_id in pand_ids:
        assert pand_id in index
        assert "stages/party_walls" in str(index[pand_id])

    # Step 2: save_cjfiles consumes index + inference results, writes to floors_estimation stage
    inferenced_floors = pd.DataFrame(
        {
            "identificatie": [pand_ids[0], pand_ids[1]],
            "floors_int": [3.0, 2.0],
        }
    ).set_index("identificatie")

    save_cjfiles(
        FloorsEstimationIOConfig(concurrency=1),
        inferenced_floors,
        index,
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
