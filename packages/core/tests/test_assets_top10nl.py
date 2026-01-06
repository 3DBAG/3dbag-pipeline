import pytest
from bag3d.core.assets.top10nl.download import extract_top10nl


@pytest.mark.slow
def test_extract_top10nl(context_top10nl):
    """Does the complete asset work?"""
    res = extract_top10nl(context_top10nl)
    assert res.value.exists()
