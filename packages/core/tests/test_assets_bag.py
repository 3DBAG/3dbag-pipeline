from dagster import build_op_context
from bag3d.core.assets.bag.download import bagextract_metadata


def test_get_extract_metadata(test_data_dir):
    init_context = build_op_context({})

    metadata = bagextract_metadata(init_context,
                                   test_data_dir / "lvbag-extract"
                                   )
    assert metadata is not None
    assert metadata[0]["Gebied"] == "NLD"
    assert metadata[0]["Timeliness"] == "2022-10-08"
    assert metadata[1] == "08102022"