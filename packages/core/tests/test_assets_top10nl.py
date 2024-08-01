import pytest
from bag3d.core.assets.top10nl import download


@pytest.mark.slow
def test_extract_top10nl(context):
    """Does the complete asset work?"""
    res = download.extract_top10nl(context)
    assert res.value.exists()
    context.resources.file_store.rm(force=True)
