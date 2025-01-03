import pytest
from bag3d.party_walls.assets.party_walls import (
    cityjsonfeatures_with_party_walls_nl,
    distribution_tiles_files_index,
    features_file_index,
    party_walls_nl,
)

TILE_IDS = ("10/564/624", "10/564/626", "10/566/624", "10/566/626", "9/560/624")


def test_distribution_tiles_files_index(context):
    """Can we parse the CityJSON tiles and return valid data?"""

    result = distribution_tiles_files_index(context)
    assert len(result.tree.geometries) == len(TILE_IDS)
    assert len(result.paths_array) == len(TILE_IDS)
    result_tile_ids = tuple(sorted(result.export_results.keys()))
    assert result_tile_ids == TILE_IDS


@pytest.mark.slow
def test_party_walls(context, mock_distribution_tiles_files_index):
    """Can we compute the party walls and other statistics?"""

    result = party_walls_nl(context, mock_distribution_tiles_files_index)
    assert not result.empty


def test_features_file_index(context):
    """Can we find and map all the 5825 cityjson feature files of the test data?"""
    result = features_file_index(context=context)
    assert len(result) == 407


@pytest.mark.slow
def test_cityjsonfeatures_with_party_walls_nl(
    context, mock_party_walls_nl, mock_features_file_index
):
    """Can we create cityjsonfeatures with the party wall data?"""
    result = cityjsonfeatures_with_party_walls_nl(
        context=context,
        party_walls_nl=mock_party_walls_nl,
        features_file_index=mock_features_file_index,
    )
    assert result[0].stem == "NL.IMBAG.Pand.0307100000308298.city"
    assert result[0].suffix == ".jsonl"
