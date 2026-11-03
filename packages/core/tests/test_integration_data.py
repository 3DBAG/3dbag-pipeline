from zipfile import ZipFile

import pytest

from bag3d.core.assets.integration_data import filter_bag_extract

DELIVERY = (
    b'<Leveringsdocument xmlns="urn:bag:test">'
    b"<SelectieGegevens><LVC-Extract>"
    b"<StandTechnischeDatum>2026-10-08</StandTechnischeDatum>"
    b"</LVC-Extract></SelectieGegevens></Leveringsdocument>"
)


def test_filter_bag_extract_filters_archive(tmp_path):
    source = tmp_path / "source"
    layer = source / "9999VBO08102026"
    source.mkdir()
    xml = (
        b'<root xmlns:gml="http://www.opengis.net/gml">'
        b'<stand id="inside"><gml:pos>122000 486000 0</gml:pos></stand>'
        b'<stand id="outside"><gml:pos>140000 500000 0</gml:pos></stand>'
        b"</root>"
    )
    with ZipFile(source / f"{layer.name}.zip", "w") as archive:
        archive.writestr("nested/vbo.xml", xml)
    metadata = DELIVERY
    (source / "Leveringsdocument-BAG-Extract.xml").write_bytes(metadata)

    destination = tmp_path / "destination"
    assert filter_bag_extract(source, destination, "08102026") == destination
    with ZipFile(destination / f"{layer.name}.zip") as archive:
        assert archive.namelist() == ["nested/vbo.xml"]
        filtered = archive.read("nested/vbo.xml")
    assert b"inside" in filtered
    assert b"outside" not in filtered
    assert not (destination / layer.name).exists()
    assert (destination / "Leveringsdocument-BAG-Extract.xml").read_bytes() == metadata


def test_filter_bag_extract_preserves_nonspatial_archive(tmp_path):
    source = tmp_path / "source"
    layer = source / "9999NUM08102026"
    source.mkdir()
    xml = b"<root><stand><identificatie>123</identificatie></stand></root>"
    with ZipFile(source / f"{layer.name}.zip", "w") as archive:
        archive.writestr("num.xml", xml)
    (source / "Leveringsdocument-BAG-Extract.xml").write_bytes(DELIVERY)

    destination = filter_bag_extract(source, tmp_path / "destination", "08102026")

    with ZipFile(destination / f"{layer.name}.zip") as archive:
        assert archive.read("num.xml") == xml


def test_filter_bag_extract_selects_delivery_date_and_removes_stale_outputs(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "Leveringsdocument-BAG-Extract.xml").write_bytes(DELIVERY)
    # Older and newer archives must be skipped without opening them.
    for date in ("08072026", "08092026", "08112026"):
        (source / f"9999VBO{date}.zip").write_bytes(b"not a ZIP")
        (source / f"9999PND{date}").mkdir()
        (source / f"GEM-WPL-RELATIE-{date}.zip").write_bytes(b"old relations")
    current = "9999NUM08102026.zip"
    with ZipFile(source / current, "w") as archive:
        archive.writestr("num.xml", b"<root/>")
    destination = tmp_path / "destination"
    destination.mkdir()
    (destination / "9999VBO08092026.zip").write_bytes(b"stale output")
    stale_layer = destination / "9999PND08072026"
    stale_layer.mkdir()
    (stale_layer / "pnd.xml").write_bytes(b"stale output")
    (destination / "GEM-WPL-RELATIE-08092026.zip").write_bytes(b"stale relations")

    filter_bag_extract(source, destination, "08102026")

    assert {path.name for path in destination.iterdir()} == {
        current,
        "Leveringsdocument-BAG-Extract.xml",
    }
    assert (source / "9999VBO08092026.zip").exists()
    assert (source / "9999PND08072026").exists()


def test_filter_bag_extract_rejects_legacy_expanded_layer(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "Leveringsdocument-BAG-Extract.xml").write_bytes(b"<root/>")
    (source / "9999VBO08102026").mkdir()
    destination = tmp_path / "destination"

    with pytest.raises(ValueError, match="Rematerialize extract_bag"):
        filter_bag_extract(source, destination, "08102026")

    assert not (destination / "9999VBO08102026.zip").exists()
