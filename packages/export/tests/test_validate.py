import csv
import gzip
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from bag3d.common.resources.executables import CommandResult
from bag3d.common.resources.files import FileStoreResource, TilesDirResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.version import ReleaseVersionResource
from dagster import Output

from bag3d.export.assets.export import validate
from bag3d.export.assets.export.attributes import (
    AttributeValidationOutcome,
    AttributeValidationResultOne,
    AttributeValidationResults,
    cityobject_validate_attributes,
    gpkg_validate_attributes,
)
from bag3d.export.assets.export.validate import (
    CityJSONFileResults,
    ValidationConfig,
    _matches_lod_report,
    _record_cityjson_report,
)


@pytest.fixture
def specs():
    return Specs3DBAGResource()


def test_compressed_tiles_validation_uses_configured_directory(tmp_path, monkeypatch):
    file_store = FileStoreResource(root_dir=str(tmp_path))
    version = ReleaseVersionResource(version="local-run")
    tiles_dir = TilesDirResource(name="custom-tiles")
    export_dir = file_store.stage_subdir("export", version.version)
    tile = export_dir / tiles_dir.name / "10/434/716"
    tile.parent.mkdir(parents=True)

    export_index = export_dir / "export_index.csv"
    export_index.write_text("tile_id\n10/434/716\n")
    metadata = export_dir / "metadata.json"
    metadata.write_text('{"identificationInfo":{"citation":{"edition":"2026.11.25"}}}')

    tile.with_suffix(".city.json.gz").write_bytes(gzip.compress(b'{"CityObjects":{}}'))
    tile.with_suffix(".gpkg.gz").write_bytes(gzip.compress(b"test geopackage"))
    with zipfile.ZipFile(tile.with_name("10-434-716-obj.zip"), "w") as archive:
        for lod in ("12", "13", "22"):
            archive.writestr(f"716-LoD{lod}-3D.obj", "")
            archive.writestr(f"716-LoD{lod}-3D.obj.mtl", "")
    with zipfile.ZipFile(tile.with_name("10-434-716-ifc.zip"), "w") as archive:
        for lod in ("0", "1.2", "1.3", "2.2"):
            archive.writestr(f"716-{lod}.ifc", "")

    def fake_val3dity(runner, path, report_path):
        assert path.is_file()
        report_path.touch()
        return CommandResult(0, ""), {"features": [], "validity": True}

    validation = SimpleNamespace(runner=MagicMock())
    validation.runner.run.return_value = CommandResult(0, "SUMMARY valid")
    gdal = SimpleNamespace(runner=MagicMock())
    monkeypatch.setattr(validate, "_read_val3dity_report", fake_val3dity)
    monkeypatch.setattr(
        validate, "inspect_geopackage", lambda *args: ({"layers": []}, {}, [])
    )
    monkeypatch.setattr(validate, "inspect_tileset", lambda *args: [])

    tilesets = export_dir / "ogc3dtiles"
    with patch.object(validate, "ProcessPoolExecutor") as executor:
        executor.return_value.__enter__.return_value.map.side_effect = map
        output = validate.compressed_tiles_validation(
            ValidationConfig(concurrency=1),
            export_index,
            metadata,
            tilesets / "lod12",
            tilesets / "lod13",
            tilesets / "lod22",
            file_store,
            version,
            gdal,
            validation,
            tiles_dir,
        )

    assert isinstance(output, Output)
    assert output.value == export_dir / "validate_compressed_files.csv"
    with output.value.open(newline="") as stream:
        row = next(csv.DictReader(stream))
    prefix = "https://data.3dbag.nl/20261125/custom-tiles/10/434/716/10-434-716"
    assert row["cj_download"] == f"{prefix}.city.json.gz"
    assert row["gpkg_download"] == f"{prefix}.gpkg.gz"
    assert row["obj_download"] == f"{prefix}-obj.zip"
    assert row["ifc_download"] == f"{prefix}-ifc.zip"


class TestAttributeValidationOutcome:
    """Test AttributeValidationOutcome enum."""

    def test_enum_values(self):
        """Test that enum values are correctly defined."""
        assert AttributeValidationOutcome.OK.value == 0
        assert AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES.value == 1
        assert AttributeValidationOutcome.INCORRECT_DATA_TYPE.value == 5

    def test_is_error(self):
        """Test is_error class method."""
        assert not AttributeValidationOutcome.is_error(AttributeValidationOutcome.OK)
        assert AttributeValidationOutcome.is_error(
            AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES
        )


class TestAttributeValidationResultOne:
    """Test AttributeValidationResultOne dataclass."""

    def test_creation_and_frozen(self):
        """Test creating instance and that it's frozen."""
        result = AttributeValidationResultOne(
            attribute_name="test_attr",
            outcome=AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES,
        )
        assert result.attribute_name == "test_attr"
        assert result.outcome == AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES

        # Test frozen
        with pytest.raises(AttributeError):
            result.attribute_name = "new_name"  # pyright: ignore[reportAttributeAccessIssue]


class TestAttributeValidationResults:
    """Test AttributeValidationResults class."""

    def test_init_and_all_ok(self):
        """Test initialization and all_ok method."""
        results = AttributeValidationResults()
        assert results.results == {}
        assert results.all_ok()

    def test_add_errors(self):
        """Test adding various types of errors."""
        results = AttributeValidationResults()

        # OK outcomes are not added
        ok_result = AttributeValidationResultOne(
            attribute_name="attr1", outcome=AttributeValidationOutcome.OK
        )
        results.add_error(ok_result)
        assert results.all_ok()

        # Single error
        error1 = AttributeValidationResultOne(
            attribute_name="attr1",
            outcome=AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES,
        )
        results.add_error(error1)
        assert not results.all_ok()
        assert error1 in results.results["attr1"]

        # Comma-separated attribute names
        error2 = AttributeValidationResultOne(
            attribute_name="attr2,attr3",
            outcome=AttributeValidationOutcome.BUILDING_MISSING_ATTRIBUTES,
        )
        results.add_error(error2)
        assert "attr2" in results.results
        assert "attr3" in results.results
        assert error2 in results.results["attr2"]
        assert error2 in results.results["attr3"]

    def test_repr(self):
        """Test string representation."""
        results = AttributeValidationResults()
        error = AttributeValidationResultOne(
            attribute_name="attr1",
            outcome=AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES,
        )
        results.add_error(error)

        repr_str = repr(results)
        assert "attr1" in repr_str
        assert "1" in repr_str  # The enum value


class TestCityobjectValidateAttributes:
    """Test cityobject_validate_attributes function."""

    def test_building_validation(self, specs):
        """Test validation and the filesystem bundle passed to Tyler."""
        manifest = specs.attribute_specification
        assert manifest.is_file()
        bundle = json.loads(manifest.read_text(encoding="utf-8"))
        assert bundle["contractVersion"] == "1.0"
        for relative_path in [bundle["semantic"], *bundle["projections"].values()]:
            assert (manifest.parent / relative_path).is_file()
        # Valid CityObject with correct attributes
        co_valid = {
            "type": "Building",
            "attributes": {
                "b3_bag_bag_overlap": 0.95,
                "b3_bouwlagen": 3,
                "b3_h_dak_min": 10.5,
            },
        }
        results = list(cityobject_validate_attributes(specs, co_valid))
        # Should have some results since we're only including a few attributes
        assert any(
            r.outcome == AttributeValidationOutcome.BUILDING_MISSING_ATTRIBUTES
            for r in results
        )

        # CityObject with extra attributes
        co_extra = {"type": "Building", "attributes": {"invalid_attr": "value"}}
        results = list(cityobject_validate_attributes(specs, co_extra))
        assert any(
            r.outcome == AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES
            for r in results
        )
        assert any("invalid_attr" in r.attribute_name for r in results)

        # CityObject with wrong data type
        co_wrong_type = {
            "type": "Building",
            "attributes": {"b3_bouwlagen": "3"},  # Should be int, not str
        }
        results = list(cityobject_validate_attributes(specs, co_wrong_type))
        assert any(
            r.outcome == AttributeValidationOutcome.INCORRECT_DATA_TYPE for r in results
        )
        for field, value, expected in (
            ("b3_bouwlagen", False, AttributeValidationOutcome.INCORRECT_DATA_TYPE),
            ("identificatie", None, AttributeValidationOutcome.INCORRECT_NULLABLE),
            (
                "b3_val3dity_lod22",
                '["error"]',
                AttributeValidationOutcome.INCORRECT_DATA_TYPE,
            ),
            (
                "b3_dak_type",
                "invalid-roof-category",
                AttributeValidationOutcome.INCORRECT_VALUE,
            ),
        ):
            outcomes = list(
                cityobject_validate_attributes(
                    specs, {"type": "Building", "attributes": {field: value}}
                )
            )
            assert any(
                result.attribute_name == field and result.outcome == expected
                for result in outcomes
            )
        missing = list(cityobject_validate_attributes(specs, {"type": "Building"}))
        assert any(
            result.outcome == AttributeValidationOutcome.BUILDING_MISSING_ATTRIBUTES
            for result in missing
        )

    def test_semantic_surface_validation(self, specs):
        """Test validation of semantic surface attributes."""
        co_with_semantics = {
            "type": "Building",
            "geometry": [
                {
                    "semantics": {
                        "surfaces": [
                            {"type": "RoofSurface", "extra_semantic_attr": "value"}
                        ]
                    }
                }
            ],
        }

        results = list(cityobject_validate_attributes(specs, co_with_semantics))
        assert any(
            r.outcome == AttributeValidationOutcome.SURFACE_EXTRA_ATTRIBUTES
            for r in results
        )


class TestGpkgValidateAttributes:
    """Test gpkg_validate_attributes function."""

    def test_building_layer_validation(self, specs):
        """Test validation of building layers in GPKG."""
        # Valid GPKG info
        gpkg_info_valid = {
            "layers": [
                {
                    "name": "building_multisurface_lod0",
                    "fields": [
                        {"name": "fid", "type": "Integer64", "nullable": False},
                        {
                            "name": "attributes__b3_bag_bag_overlap",
                            "type": "Real",
                            "nullable": True,
                        },
                    ],
                }
            ]
        }
        results = list(gpkg_validate_attributes(specs, gpkg_info_valid))
        # Will have missing attributes since we only include two fields
        assert any(
            r.outcome == AttributeValidationOutcome.BUILDING_MISSING_ATTRIBUTES
            for r in results
        )

        # GPKG with extra fields
        gpkg_info_extra = {
            "layers": [
                {
                    "name": "building_multisurface_lod0",
                    "fields": [
                        {"name": "extra_field", "type": "String", "nullable": True}
                    ],
                }
            ]
        }
        results = list(gpkg_validate_attributes(specs, gpkg_info_extra))
        assert any(
            r.outcome == AttributeValidationOutcome.BUILDING_EXTRA_ATTRIBUTES
            for r in results
        )
        assert any("extra_field" in r.attribute_name for r in results)

    def test_surface_layer_validation(self, specs):
        """Test validation of surface layers in GPKG."""
        gpkg_info = {
            "layers": [
                {
                    "name": "semantics",  # This is a surface layer
                    "fields": [
                        {"name": "fid", "type": "Integer64", "nullable": False},
                        {
                            "name": "extra_surface_field",
                            "type": "String",
                            "nullable": True,
                        },
                    ],
                }
            ]
        }
        results = list(gpkg_validate_attributes(specs, gpkg_info))
        # Should detect extra attributes as surface layer errors
        assert any(
            r.outcome == AttributeValidationOutcome.SURFACE_EXTRA_ATTRIBUTES
            for r in results
        )

    def test_field_type_validation(self, specs):
        """Test validation of field types and nullable settings."""
        gpkg_info = {
            "layers": [
                {
                    "name": "building_multisurface_lod0",
                    "fields": [
                        {
                            "name": "attributes__b3_val3dity_lod22",
                            "type": "StringList",
                            "nullable": True,
                        },  # Wrong type
                        {
                            "name": "attributes__identificatie",
                            "type": "String",
                            "nullable": True,
                        },  # Wrong nullable
                        {"name": "identificatie", "type": "String", "nullable": False},
                    ],
                }
            ]
        }
        results = list(gpkg_validate_attributes(specs, gpkg_info))

        # Check for type and nullable errors
        type_errors = [
            r
            for r in results
            if r.outcome == AttributeValidationOutcome.INCORRECT_DATA_TYPE
        ]
        nullable_errors = [
            r
            for r in results
            if r.outcome == AttributeValidationOutcome.INCORRECT_NULLABLE
        ]

        assert len(type_errors) == 1
        assert len(nullable_errors) == 1


@pytest.mark.parametrize(
    ("value", "expected", "valid"),
    [
        ("[]", set(), True),
        ("[101,302]", {101, 302}, True),
        ([], set(), True),
        ([101, 302], {101, 302}, True),
        (None, None, True),
        (None, set(), False),
        ("bad", set(), False),
        ("[true]", {1}, False),
        ("null", None, False),
    ],
)
def test_lod_error_representation(specs, value, expected, valid):
    assert _matches_lod_report(value, expected) is valid
    results = cityobject_validate_attributes(
        specs, {"type": "Building", "attributes": {"b3_val3dity_lod22": value}}
    )
    incorrect = any(
        r.attribute_name == "b3_val3dity_lod22"
        and r.outcome == AttributeValidationOutcome.INCORRECT_DATA_TYPE
        for r in results
    )
    assert incorrect is (value in ("bad", "[true]", "null"))


def test_flat_report_maps_errors_to_each_parts_actual_lod():
    objects = {
        "building": {
            "type": "Building",
            "children": ["a", "b"],
            "attributes": {"b3_val3dity_lod12": "[]", "b3_val3dity_lod22": "[302,601]"},
        },
        "a": {"type": "BuildingPart", "geometry": [{"lod": "2.2"}, {"lod": "1.2"}]},
        "b": {"type": "BuildingPart", "geometry": [{"lod": "1.2"}, {"lod": "2.2"}]},
    }
    report = {
        "features": [
            {
                "type": "Building",
                "id": "building",
                "validity": False,
                "errors": [
                    {"code": 302, "id": "coid=a|geom=0|shell=0|face="},
                    {"code": 601, "id": "coid=a|geom=0&&coid=b|geom=1"},
                ],
            }
        ]
    }
    results = CityJSONFileResults()
    _record_cityjson_report(results, report, objects)
    assert results.nr_invalid_building == 1
    assert results.nr_invalid_buildingpart_lod12 == 0
    assert results.nr_invalid_buildingpart_lod22 == 2
    assert results.errors_lod12 == []
    assert results.errors_lod22 == [302, 601]
    assert results.nr_mismatch_errors_lod12 == results.nr_mismatch_errors_lod22 == 0
    assert results.nr_mismatch_errors_lod13 == 0
