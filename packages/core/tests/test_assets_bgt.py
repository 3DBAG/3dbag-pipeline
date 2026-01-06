import pytest
from bag3d.core.assets.bgt.download import extract_bgt


@pytest.mark.slow
def test_extract_bgt(context_bgt):
    """Does the complete asset work?"""
    res = extract_bgt(context_bgt)
    assert res.value.exists()
