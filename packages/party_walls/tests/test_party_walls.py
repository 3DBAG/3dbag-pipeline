import json

from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    features_file_index,
    party_walls_nl,
)
from bag3d.common.resources.files import FileStoreResource


def _make_feature_file(path, pand_id: str) -> None:
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


def test_features_file_index(tmp_path):
    """features_file_index maps pand_id -> path for all .city.jsonl files."""
    recon_dir = tmp_path / "stages" / "reconstruction"
    # Create z/x/y/objects/<pand_id>/reconstruct/<pand_id>.city.jsonl structure
    pand_ids = [
        "NL.IMBAG.Pand.0307100000308298",
        "NL.IMBAG.Pand.0307100000368987",
    ]
    for pand_id in pand_ids:
        feature_path = (
            recon_dir
            / "0"
            / "0"
            / "0"
            / "objects"
            / pand_id
            / "reconstruct"
            / f"{pand_id}.city.jsonl"
        )
        _make_feature_file(feature_path, pand_id)

    file_store = FileStoreResource(root_dir=str(tmp_path))
    result = features_file_index(PartyWallsConfig(), file_store)

    assert isinstance(result, dict)
    assert len(result) == len(pand_ids)
    for pand_id in pand_ids:
        assert pand_id in result
        assert result[pand_id].exists()


def test_party_walls_nl_empty_tile(tmp_path):
    """party_walls_nl returns [] when no features are found for the tile."""
    from unittest.mock import MagicMock

    from dagster import build_asset_context
    from bag3d.common.resources.version import ReleaseVersionResource

    tile_id = "10/434/716"
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version="test_version")

    # features_file_index has features only for a different tile
    recon_dir = tmp_path / "stages" / "reconstruction"
    pand_id = "NL.IMBAG.Pand.0307100000308298"
    feature_path = (
        recon_dir
        / "0"
        / "0"
        / "0"
        / "objects"
        / pand_id
        / "reconstruct"
        / f"{pand_id}.city.jsonl"
    )
    _make_feature_file(feature_path, pand_id)

    mock_db = MagicMock()

    with build_asset_context(partition_key=tile_id) as context:
        result = party_walls_nl(
            context,
            PartyWallsConfig(),
            {pand_id: feature_path},
            mock_db,
            file_store,
            version,
        )

    assert result == []
    # DB should not be queried when tile is empty
    mock_db.connection.get_dict.assert_not_called()
