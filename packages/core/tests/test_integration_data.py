from unittest.mock import Mock
from zipfile import ZipFile

import pytest
from bag3d.common.resources.executables import (
    CommandResult,
    GDALResource,
    LASToolsResource,
)

from bag3d.core.assets.integration_data import _validate_outputs, filter_bag_extract

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


@pytest.fixture
def validate_pointclouds(tmp_path):
    bag = tmp_path / "bag"
    bag.mkdir()
    bgt = tmp_path / "bgt.zip"
    top10nl = tmp_path / "top10nl.zip"
    for path, member in ((bgt, "bgt_pand.gml"), (top10nl, "top10nl_gebouw.gml")):
        with ZipFile(path, "w") as archive:
            archive.writestr(member, b"<root/>")
    gdal = Mock(
        spec=GDALResource,
        runner=Mock(run=Mock(return_value=CommandResult(0, "Extent: (1, 2) - (3, 4)"))),
    )
    lastools = Mock(
        spec=LASToolsResource,
        runner=Mock(
            run=Mock(
                return_value=CommandResult(0, "min x y z: 1 2 0\nmax x y z: 3 4 5\n")
            )
        ),
    )

    def validate(roots):
        return _validate_outputs(
            tmp_path,
            bag,
            bgt,
            top10nl,
            tmp_path / "buurtkaart.gpkg",
            roots,
            (0, 0, 10, 10),
            gdal,
            lastools,
        )

    return validate, lastools.runner.run


@pytest.mark.parametrize("roots_kind", ["repeated", "overlapping", "aliased"])
def test_validate_outputs_validates_each_pointcloud_once(
    tmp_path, validate_pointclouds, roots_kind
):
    validate, run = validate_pointclouds
    pointclouds = tmp_path / "pointclouds"
    paths = [pointclouds / "AHN3" / "tile.laz", pointclouds / "AHN4" / "tile.LAS"]
    # Create files in reverse order to check deterministic validation order.
    for path in reversed(paths):
        path.parent.mkdir(parents=True)
        path.write_bytes(b"pointcloud")
    (pointclouds / "ignore.txt").write_text("not a pointcloud")
    (pointclouds / "directory.laz").mkdir()
    roots = [pointclouds] * 4
    if roots_kind == "overlapping":
        roots = [pointclouds / "AHN4", pointclouds, pointclouds / "AHN3"]
    elif roots_kind == "aliased":
        alias = tmp_path / "alias"
        alias.symlink_to(pointclouds, target_is_directory=True)
        roots = [pointclouds, alias]

    result = validate(iter(roots))

    assert [call.kwargs["local_path"] for call in run.call_args_list] == paths
    assert result["extents"]["pointclouds"] == {
        str(path.relative_to(tmp_path)): [1.0, 2.0, 3.0, 4.0] for path in paths
    }
    assert len(result["extents"]["vectors"]) == 3


def test_validate_outputs_handles_empty_pointcloud_roots(
    tmp_path, validate_pointclouds
):
    validate, run = validate_pointclouds
    pointclouds = tmp_path / "pointclouds"
    pointclouds.mkdir()

    assert validate([pointclouds] * 4)["extents"]["pointclouds"] == {}
    run.assert_not_called()


@pytest.mark.parametrize(
    ("command_result", "error", "message"),
    [
        (CommandResult(1, "", "failed"), RuntimeError, "lasinfo failed"),
        (CommandResult(0, "ERROR: invalid file"), RuntimeError, "lasinfo failed"),
        (CommandResult(0, "min x y z: 1 2 0"), RuntimeError, "no complete XY bounds"),
        (
            CommandResult(0, "min x y z: 1 2 0\nmax x y z: 100 100 5\n"),
            ValueError,
            "outside the AOI",
        ),
    ],
)
def test_validate_outputs_rejects_invalid_pointclouds(
    tmp_path, validate_pointclouds, command_result, error, message
):
    validate, run = validate_pointclouds
    pointclouds = tmp_path / "pointclouds"
    pointclouds.mkdir()
    (pointclouds / "tile.laz").write_bytes(b"pointcloud")
    run.return_value = command_result

    with pytest.raises(error, match=message):
        validate([pointclouds] * 4)

    run.assert_called_once()
