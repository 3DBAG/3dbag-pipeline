from zipfile import ZipFile

import pytest

from bag3d.core.assets.integration_data import filter_bag_extract


@pytest.mark.parametrize("archived", [False, True])
def test_filter_bag_extract_handles_expanded_layer(tmp_path, archived):
    source = tmp_path / "source"
    layer = source / "9999VBO08102026"
    layer.mkdir(parents=True)
    xml = (
        b'<root xmlns:gml="http://www.opengis.net/gml">'
        b'<stand id="inside"><gml:pos>122000 486000 0</gml:pos></stand>'
        b'<stand id="outside"><gml:pos>140000 500000 0</gml:pos></stand>'
        b"</root>"
    )
    nested = layer / "nested"
    nested.mkdir()
    (nested / "vbo.xml").write_bytes(xml)
    (layer / "empty").mkdir()
    if archived:
        with ZipFile(source / f"{layer.name}.zip", "w") as archive:
            archive.writestr("nested/vbo.xml", xml)
        # When both exist, use the archive rather than stale expanded files.
        (nested / "vbo.xml").write_bytes(b"stale expanded data")
    metadata = b"<Leveringsdocument/>"
    (source / "Leveringsdocument-BAG-Extract.xml").write_bytes(metadata)

    destination = tmp_path / "destination"
    assert filter_bag_extract(source, destination) == destination
    with ZipFile(destination / f"{layer.name}.zip") as archive:
        assert archive.namelist() == ["nested/vbo.xml"]
        filtered = archive.read("nested/vbo.xml")
    assert b'inside' in filtered
    assert b'outside' not in filtered
    assert not (destination / layer.name).exists()
    assert (destination / "Leveringsdocument-BAG-Extract.xml").read_bytes() == metadata


def test_filter_bag_extract_preserves_expanded_nonspatial_layer(tmp_path):
    source = tmp_path / "source"
    layer = source / "9999NUM08102026"
    layer.mkdir(parents=True)
    xml = b"<root><stand><identificatie>123</identificatie></stand></root>"
    (layer / "num.xml").write_bytes(xml)

    destination = filter_bag_extract(source, tmp_path / "destination")

    with ZipFile(destination / f"{layer.name}.zip") as archive:
        assert archive.read("num.xml") == xml
