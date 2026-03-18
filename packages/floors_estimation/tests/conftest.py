from io import StringIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.floors_estimation.resources import ModelStoreResource
from dagster import build_op_context

pytest_plugins = ["bag3d.common.testing.conftest_plugin"]


@pytest.fixture
def version():
    yield ReleaseVersionResource(version="test_version")


@pytest.fixture
def file_store_tmp(tmp_path):
    yield tmp_path


@pytest.fixture
def model_store() -> ModelStoreResource:
    """Mock model store resource for testing."""
    return MagicMock(spec=ModelStoreResource)


@pytest.fixture
def context():
    yield build_op_context(
        partition_key="0/0/0",
    )


@pytest.fixture
def context_with_data():
    yield build_op_context(
        partition_key="0/0/0",
    )


@pytest.fixture(scope="session")
def mock_preprocessed_features():
    csv_text = """
    identificatie,no_vertices,perimeter,no_units,net_area,building_function,no_neighbours_100,no_adjacent_neighbours,cbs_percent_multihousehold,cbs_pop_per_km2,cbs_dist_to_horeca,buildingtype,construction_year,roof_type,h_roof_50p,h_roof_70p,h_roof_max,h_roof_min,area_roof,area_ext_walls,area_party_walls,area_ground,volume_lod22,volume_lod12
    NL.IMBAG.Pand.0307100000340455,9,55.44252687545277,3,198,1,37,4.0,96,5887,50,2,1927,1,11.029999732971191,11.043000221252441,11.10099983215332,8.204999923706055,160.08,234.48,186.19,156.61,1123.7303466796875,1244.3280029296875
    NL.IMBAG.Pand.0307100000378340,7,21.663911358502567,1,70,0,39,2.0,96,5887,50,4,1922,1,10.729000091552734,10.956000328063965,11.395000457763672,9.892000198364258,34.1,35.98,127.68,23.14,192.20730590820312,204.35214233398438
    NL.IMBAG.Pand.0307100000522025,6,41.48225146934291,1,138,0,124,4.0,77,9573,45,4,1760,1,8.456000328063965,9.131999969482422,10.48900032043457,6.5229997634887695,118.67,91.83,120.32,92.79,526.0918579101562,614.935791015625
    NL.IMBAG.Pand.0307100000351286,19,311.01548191691313,128,6929,1,5,,77,7350,20,2,1979,1,18.472000122070312,24.054000854492188,27.615999221801758,18.336999893188477,2098.48,5430.01,0.0,1976.27,33547.6953125,39757.82421875
    NL.IMBAG.Pand.0307100000353630,11,47.068882927884324,1,194,0,92,4.0,70,9628,120,4,1800,1,11.687000274658203,12.315999984741211,13.361000061035156,6.389999866485596,132.48,147.19,174.05,101.76,762.90283203125,923.0503540039062
    """

    return pd.read_csv(StringIO(csv_text.strip()))


@pytest.fixture
def mock_features_file_index(tmp_path):
    return {
        "NL.IMBAG.Pand.0307100000340455": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000340455.city.jsonl",
        "NL.IMBAG.Pand.0307100000364333": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000364333.city.jsonl",
        "NL.IMBAG.Pand.0307100000378340": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000378340.city.jsonl",
        "NL.IMBAG.Pand.0307100000522025": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000522025.city.jsonl",
        "NL.IMBAG.Pand.0307100000351286": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000351286.city.jsonl",
        "NL.IMBAG.Pand.0307100000522233": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000522233.city.jsonl",
        "NL.IMBAG.Pand.0307100000353630": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000353630.city.jsonl",
        "NL.IMBAG.Pand.0307100000312499": tmp_path
        / "3DBAG/party_walls_features/0/0/0/NL.IMBAG.Pand.0307100000312499.city.jsonl",
    }


@pytest.fixture(scope="session")
def mock_inferenced_floors():
    csv_text = """
            identificatie,no_vertices,perimeter,no_units,net_area,building_function,no_neighbours_100,no_adjacent_neighbours,cbs_percent_multihousehold,cbs_pop_per_km2,cbs_dist_to_horeca,buildingtype,construction_year,roof_type,h_roof_50p,h_roof_70p,h_roof_max,h_roof_min,area_roof,area_ext_walls,area_party_walls,area_ground,volume_lod22,volume_lod12,floors,floors_int
            NL.IMBAG.Pand.0307100000340455,9,55.44252687545277,3,198,1,37,4.0,96,5887,50,2,1927,1,11.029999732971191,11.04300022125244,11.10099983215332,8.204999923706055,160.08,234.48,186.19,156.61,1123.7303466796875,1244.3280029296875,3.178904466477873,3.0
            NL.IMBAG.Pand.0307100000378340,7,21.663911358502567,1,70,0,39,2.0,96,5887,50,4,1922,1,10.729000091552734,10.956000328063965,11.395000457763672,9.892000198364258,34.1,35.98,127.68,23.14,192.20730590820312,204.3521423339844,2.7363718856599504,3.0
            NL.IMBAG.Pand.0307100000522025,6,41.48225146934291,1,138,0,124,4.0,77,9573,45,4,1760,1,8.456000328063965,9.131999969482422,10.48900032043457,6.5229997634887695,118.67,91.83,120.32,92.79,526.0918579101562,614.935791015625,2.2665033871332767,2.0
            NL.IMBAG.Pand.0307100000351286,19,311.0154819169132,128,6929,1,5,,77,7350,20,2,1979,1,18.472000122070312,24.054000854492188,27.61599922180176,18.33699989318848,2098.48,5430.01,0.0,1976.27,33547.6953125,39757.82421875,7.52333945870127,8.0
            NL.IMBAG.Pand.0307100000353630,11,47.06888292788432,1,194,0,92,4.0,70,9628,120,4,1800,1,11.687000274658203,12.315999984741213,13.361000061035156,6.389999866485596,132.48,147.19,174.05,101.76,762.90283203125,923.0503540039062,3.1525948513138182,3.0
            """

    return pd.read_csv(StringIO(csv_text.strip()))
