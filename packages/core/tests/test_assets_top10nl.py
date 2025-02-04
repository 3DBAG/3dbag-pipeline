import pytest
from bag3d.core.assets.top10nl import download


@pytest.mark.slow
def test_extract_top10nl(context_top10nl):
    """Does the complete asset work?"""
    res = download.extract_top10nl(context_top10nl)
    assert res.value.exists()
    context_top10nl.resources.file_store.file_store.rm(force=True)
