import json
from pathlib import Path

import pandas as pd

from bag3d.common.resources.files import FileStoreResource
from bag3d.floors_estimation.assets.floors_estimation import (
    FloorsEstimationConfig,
    FloorsEstimationIOConfig,
    features_file_index,
    make_chunks,
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


def test_features_file_index(tmp_path):
    """features_file_index maps pand_id -> path for .city.jsonl files in party_walls stage."""
    pand_ids = [
        "NL.IMBAG.Pand.0307100000377456",
        "NL.IMBAG.Pand.0307100000364333",
    ]
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
    result = features_file_index(FloorsEstimationConfig(), file_store)

    assert isinstance(result, dict)
    assert len(result) == len(pand_ids)
    for pand_id in pand_ids:
        assert pand_id in result
        assert "party_walls" in str(result[pand_id])


def test_make_chunks():
    """Can we make data chunks from a dictionary of id:path pairs?"""
    data = {
        "id1": Path("path1"),
        "id2": Path("path2"),
        "id3": Path("path3"),
        "id4": Path("path4"),
        "id5": Path("path5"),
        "id6": Path("path6"),
    }

    chunks = make_chunks(data, 3)
    assert next(chunks) == {
        "id1": Path("path1"),
        "id2": Path("path2"),
        "id3": Path("path3"),
    }
    assert next(chunks) == {
        "id4": Path("path4"),
        "id5": Path("path5"),
        "id6": Path("path6"),
    }

    chunks2 = make_chunks(data, 4)

    assert next(chunks2) == {
        "id1": Path("path1"),
        "id2": Path("path2"),
        "id3": Path("path3"),
        "id4": Path("path4"),
    }
    assert next(chunks2) == {"id5": Path("path5"), "id6": Path("path6")}


def test_save_cjfiles(
    tmp_path,
):
    """save_cjfiles writes the expected b3_bouwlagen values for each output feature."""
    # Build mock features in stages/party_walls/
    pand_ids = [
        "NL.IMBAG.Pand.0307100000340455",
        "NL.IMBAG.Pand.0307100000351286",
        "NL.IMBAG.Pand.0307100000364333",
    ]
    mock_index = {}
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
        mock_index[pand_id] = feature_path

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
    save_cjfiles(
        FloorsEstimationIOConfig(),
        inferenced_floors,
        mock_index,
        file_store,
    )

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
