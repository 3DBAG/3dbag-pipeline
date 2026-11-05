from unittest.mock import Mock
from zipfile import ZipFile

import pytest
from bag3d.common.resources.executables import (
    CommandResult,
    GDALResource,
    LASToolsResource,
)

from bag3d.core.assets.integration_data import (
    _native_archive_extents,
    _validate_outputs,
    filter_bag_extract,
)

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
        b'<stand id="inside"><gml:pos>141000 486000 0</gml:pos></stand>'
        b'<stand id="outside"><gml:pos>122000 486000 0</gml:pos></stand>'
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
    gdal = Mock(
        spec=GDALResource,
        runner=Mock(
            run=Mock(
                return_value=CommandResult(0, "Extent (geometrie2d): (1, 2) - (3, 4)")
            )
        ),
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


def test_native_archive_extents_uses_vsizip_and_xsd(tmp_path):
    """Native BGT/TOP10NL archives must be read via /vsizip/ with the dataset XSD."""
    archive = tmp_path / "bgt.zip"
    runner = Mock(
        run=Mock(
            return_value=CommandResult(0, "Extent (geometrie2d): (1, 2) - (3, 4)")
        )
    )

    extents = _native_archive_extents(runner, archive)

    assert extents == [(1.0, 2.0, 3.0, 4.0)]
    call = runner.run.call_args
    assert call.kwargs["local_path"] == archive
    assert call.kwargs["exe_name"] == "ogrinfo"
    assert call.kwargs["kwargs"]["member"] == "bgt_pand.gml"
    assert call.kwargs["kwargs"]["feature_type"] == "pand"
    assert call.kwargs["kwargs"]["xsd"].startswith("http")
    assert "XSD=" in call.args[0]
    assert "/vsizip/" in call.args[0]


def test_manifest_declares_downloaded_ahn_partitions(tmp_path, monkeypatch):
    import hashlib
    import json
    from types import SimpleNamespace
    from typing import Any, cast

    from bag3d.core.assets import integration_data

    # Current indexes use "url" for every AHN version. Include an unselected
    # tile to ensure the manifest only declares files present in this fixture.
    indexes = {}
    expected = {}
    for version in (3, 4, 5):
        path = tmp_path / "pointclouds" / f"AHN{version}" / "C_25GN1.LAZ"
        path.parent.mkdir(parents=True)
        path.write_bytes(f"clipped AHN{version}".encode())
        indexes[version] = {
            "25gn1": {"url": f"https://example.test/AHN{version}/{path.name}"},
            "01cz1": {"url": f"https://example.test/AHN{version}/C_01CZ1.LAZ"},
        }
        expected[str(version)] = {
            "25gn1": {
                "path": str(path.relative_to(tmp_path)),
                "url": indexes[version]["25gn1"]["url"],
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        }
    monkeypatch.setattr(integration_data, "_validate_outputs", lambda *args: {})
    generate = cast(
        Any, integration_data.integration_manifest.op.compute_fn
    ).decorated_fn
    sources = ("bag", "bgt", "top10nl", "cbs_key_figures", "cbs_buurtkaart")
    kwargs = {f"integration_{name}": tmp_path / name for name in sources}
    kwargs.update({f"extract_{name}": None for name in sources})
    kwargs.update({f"integration_ahn{version}": tmp_path for version in (3, 4, 5, 6)})
    kwargs.update({f"tile_index_ahn{version}": indexes[version] for version in indexes})
    manifest_path = generate(
        integration_data_store=SimpleNamespace(path=tmp_path),
        tile_index_ahn6={},
        gdal=None,
        lastools=None,
        **kwargs,
    )
    manifest = json.loads(manifest_path.read_text())
    assert manifest["fixture_version"] == "3"
    assert manifest["clipping"]["vectors"]["bgt"] == "pdok_aoi_whole_objects"
    assert manifest["clipping"]["vectors"]["top10nl"] == "pdok_aoi_whole_objects"
    assert manifest["ahn"]["partitions"] == {**expected, "6": {}}
    for partitions in expected.values():
        entry = partitions["25gn1"]
        assert manifest["files"][entry["path"]] == {
            "size": entry["size"],
            "sha256": entry["sha256"],
        }


@pytest.mark.parametrize("dataset", ["bgt", "top10nl"])
def test_native_fixture_preserves_entire_archive(tmp_path, dataset):
    from types import SimpleNamespace
    from bag3d.core.assets import integration_data

    member = "arbitrary/nested.data"
    xml = b"arbitrary content"
    source = tmp_path / "source.zip"
    with ZipFile(source, "w") as archive:
        archive.writestr(member, xml)
        archive.writestr("original-schema.xsd", b"original companion schema")
    destination = tmp_path / "fixture" / dataset / f"{dataset}.zip"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"previous fixture")
    fn = getattr(integration_data, f"integration_{dataset}").op.compute_fn.decorated_fn
    output = fn(
        integration_data_store=SimpleNamespace(path=tmp_path / "fixture"),
        **{f"extract_{dataset}": source},
    )
    assert output == destination
    assert destination.read_bytes() == source.read_bytes()
    with ZipFile(destination) as archive:
        assert archive.read(member) == xml
        assert archive.read("original-schema.xsd") == b"original companion schema"


def test_native_copy_failure_preserves_previous_fixture(tmp_path, monkeypatch):
    from bag3d.core.assets import integration_data

    source = tmp_path / "source.zip"
    with ZipFile(source, "w") as archive:
        archive.writestr("bgt_pand.gml", b"original")
    target = tmp_path / "fixture.zip"
    target.write_bytes(b"previous")

    def fail(source, destination):
        destination.write_bytes(b"partial")
        raise OSError("copy interrupted")

    monkeypatch.setattr(integration_data.shutil, "copyfile", fail)
    with pytest.raises(OSError, match="interrupted"):
        integration_data._copy_archive(source, target)
    assert target.read_bytes() == b"previous"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["fixture.zip", "source.zip"]


def test_integration_job_supplies_aoi_for_manual_and_scheduled_runs():
    import dagster as dg
    from bag3d.common.testing.mock_resources import MOCK_RESOURCES
    from bag3d.core.code_location import all_assets
    from bag3d.core.jobs import job_integration_data, job_bgt
    from bag3d.core.schedules import integration_data_schedule
    from bag3d.core.assets.integration_data import AOI_WKT

    defs = dg.Definitions(
        assets=all_assets,
        jobs=[job_integration_data, job_bgt],
        schedules=[integration_data_schedule],
        resources=MOCK_RESOURCES,
    )
    job = defs.resolve_job_def("integration_data")
    for config in ({}, {"ops": {"integration_bag": {"config": {"cbs_year": "2024"}}}}):
        resolved = dg.validate_run_config(job, config)
        for op in ("extract_bgt", "extract_top10nl"):
            assert resolved["ops"][op]["config"]["geofilter"] == AOI_WKT
    with dg.build_schedule_context(repository_def=defs.get_repository_def()) as context:
        result = integration_data_schedule.evaluate_tick(context)
        assert result.run_requests
        request = result.run_requests[0]
    resolved = dg.validate_run_config(job, request.run_config)
    assert resolved["ops"]["extract_bgt"]["config"]["geofilter"] == AOI_WKT
    assert resolved["ops"]["extract_top10nl"]["config"]["geofilter"] == AOI_WKT
    assert (
        dg.validate_run_config(defs.resolve_job_def("bgt"), {})["ops"]["extract_bgt"][
            "config"
        ]["geofilter"]
        is None
    )


@pytest.mark.parametrize("dataset", ["bgt", "top10nl"])
@pytest.mark.parametrize(
    "output,error",
    [
        (
            "Extent: (-100, -100) - (-50, -50)\nExtent (other): (50, 50) - (100, 100)",
            None,
        ),
        ("Extent: (100, 100) - (110, 110)", ValueError),
        ("Extent: (5, 5) - (1, 1)", ValueError),
        ("Extent: (1, 1) - (1e999, 5)", ValueError),
        ("no extents", RuntimeError),
    ],
)
def test_validate_outputs_uses_combined_generic_dataset_extent(
    tmp_path, dataset, output, error
):
    bag = tmp_path / "bag"
    bag.mkdir()
    paths = {name: tmp_path / f"{name}.zip" for name in ("bgt", "top10nl")}

    def inspect(*args, local_path, **kwargs):
        return CommandResult(
            0, output if local_path == paths[dataset] else "Extent: (1, 1) - (9, 9)"
        )

    gdal = Mock(runner=Mock(run=Mock(side_effect=inspect)))

    def validate():
        return _validate_outputs(
            tmp_path,
            bag,
            paths["bgt"],
            paths["top10nl"],
            tmp_path / "cbs.gpkg",
            [],
            (0, 0, 10, 10),
            gdal,
            Mock(),
        )

    if error:
        with pytest.raises(error, match="extent|Extent"):
            validate()
    else:
        result = validate()
        assert len(result["extents"]["vectors"][f"{dataset}.zip"]) == 2
        for call in gdal.runner.run.call_args_list:
            assert call.kwargs["exe_name"] == "ogrinfo"
