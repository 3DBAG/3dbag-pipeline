import pytest
from pathlib import Path
from typing import cast
from pandas import DataFrame
from bag3d.party_walls.assets.party_walls import (
    PartyWallsConfig,
    TilesFilesIndex,
    cityjsonfeatures_with_party_walls_nl,
    distribution_tiles_files_index,
    features_file_index,
    party_walls_nl,
)

TILE_IDS = ("0/0/0",)


def test_distribution_tiles_files_index(file_store_resource, version):
    """Can we parse the CityJSON tiles and return valid data?"""

    result = cast(
        TilesFilesIndex,
        distribution_tiles_files_index(file_store_resource, version),
    )
    assert len(result.tree.geometries) == len(TILE_IDS)
    assert len(result.paths_array) == len(TILE_IDS)
    result_tile_ids = tuple(sorted(result.export_results.keys()))
    assert result_tile_ids == TILE_IDS


@pytest.mark.slow
def test_party_walls(context, database, mock_distribution_tiles_files_index):
    """Can we compute the party walls and other statistics?"""

    result = cast(
        DataFrame,
        party_walls_nl(context, mock_distribution_tiles_files_index, database),
    )
    assert not result.empty


def test_features_file_index(file_store_fastssd_resource):
    """Can we find and map all the cityjson feature files of the test data?"""
    result = cast(
        dict[str, Path],
        features_file_index(PartyWallsConfig(), file_store_fastssd_resource),
    )
    assert len(result) == 415


@pytest.mark.slow
def test_cityjsonfeatures_with_party_walls_nl(
    context, file_store_fastssd_resource, mock_party_walls_nl, mock_features_file_index
):
    """Can we create cityjsonfeatures with the party wall data?"""
    result = cast(
        list[Path],
        cityjsonfeatures_with_party_walls_nl(
            context,
            mock_party_walls_nl,
            mock_features_file_index,
            file_store_fastssd_resource,
        ),
    )
    assert result[0].stem == "NL.IMBAG.Pand.0307100000308298.city"
    assert result[0].suffix == ".jsonl"
