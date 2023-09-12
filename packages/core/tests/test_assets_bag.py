from dagster import build_op_context

from bag3d.core.assets.bag.download import bagextract_metadata


def test_get_extract_metadata(database):
    temp_dir, conn = database
    init_context = build_op_context({})
    metadata = bagextract_metadata(init_context,
                                   temp_dir / "lvbag-extract" / "Leveringsdocument-BAG-Extract.xml"
                                   )
    print(metadata)
