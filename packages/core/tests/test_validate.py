import pytest

from bag3d.common.resources.executables import CommandRunner
from bag3d.core.assets.export.validate import (
    obj,
    gpkg,
    cityjson,
    AttributeValidationOutcome,
    AttributeValidationResultOne,
    AttributeValidationResults,
    cityobject_validate_attributes,
    gpkg_validate_attributes,
)


def test_obj(resources, test_data_dir):
    res = obj(
        system=CommandRunner(),
        validation_runner=resources["validation"].runner,
        dirpath=test_data_dir / "validation_input/",
        file_id="0-0-0",
        planarity_n_tol=20.0,
        planarity_d2p_tol=0.001,
        snap_tol=0.0001,
        url_root="https://data.3dbag.nl",
        version="test",
    )
    assert res.zip_ok
    assert not res.file_ok


def test_gpkg(resources, test_data_dir):
    res = gpkg(
        system=CommandRunner(),
        gdal_runner=resources["gdal"].runner,
        dirpath=test_data_dir / "validation_input/",
        file_id="0-0-0",
        url_root="https://data.3dbag.nl",
        version="test",
        specs=resources["specs"],
    )
    assert res.zip_ok
    assert res.file_ok
    assert res.nr_building == 413
    assert res.nr_buildingpart == 414
    assert res.nr_invalid_2d_geom == 0


def test_cityjson(resources, test_data_dir):
    res = cityjson(
        system=CommandRunner(),
        validation_runner=resources["validation"].runner,
        dirpath=test_data_dir / "validation_input/",
        file_id="0-0-0",
        planarity_n_tol=20.0,
        planarity_d2p_tol=0.001,
        url_root="https://data.3dbag.nl",
        version="test",
        specs=resources["specs"],
        snap_tol=0.0001,
    )
    assert res.zip_ok
    assert res.sha256 is not None
    assert not res.file_ok
    assert res.schema_valid
    assert res.schema_warnings


def test_obj_missing(resources_missing, test_data_dir):
    with pytest.raises(Exception):
        _ = obj(
            system=CommandRunner(),
            validation_runner=resources_missing["validation"].runner,
            dirpath=test_data_dir / "validation_input/",
            file_id="0-0-0",
            planarity_n_tol=20.0,
            planarity_d2p_tol=0.001,
            snap_tol=0.0001,
            url_root="https://data.3dbag.nl",
            version="test",
        )


def test_gpkg_missing(resources_missing, test_data_dir):
    with pytest.raises(Exception):
        _ = gpkg(
            system=CommandRunner(),
            gdal_runner=resources_missing["gdal"].runner,
            dirpath=test_data_dir / "validation_input/",
            file_id="0-0-0",
            url_root="https://data.3dbag.nl",
            version="test",
            specs=resources_missing["specs"],
        )


def test_cityjson_missing(resources_missing, test_data_dir):
    with pytest.raises(Exception):
        _ = cityjson(
            system=CommandRunner(),
            validation_runner=resources_missing["validation"].runner,
            dirpath=test_data_dir / "validation_input/",
            file_id="0-0-0",
            planarity_n_tol=20.0,
            planarity_d2p_tol=0.001,
            url_root="https://data.3dbag.nl",
            version="test",
            specs=resources_missing["specs"],
            snap_tol=0.0001,
        )


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
            result.attribute_name = "new_name"


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

    def test_building_validation(self, resources):
        """Test validation of building attributes."""
        specs = resources["specs"]

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

    def test_semantic_surface_validation(self, resources):
        """Test validation of semantic surface attributes."""
        specs = resources["specs"]

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

    def test_building_layer_validation(self, resources):
        """Test validation of building layers in GPKG."""
        specs = resources["specs"]

        # Valid GPKG info
        gpkg_info_valid = {
            "layers": [
                {
                    "name": "lod12_3d",
                    "fields": [
                        {"name": "fid", "type": "Integer64", "nullable": False},
                        {
                            "name": "b3_bag_bag_overlap",
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
                    "name": "lod12_3d",
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

    def test_surface_layer_validation(self, resources):
        """Test validation of surface layers in GPKG."""
        specs = resources["specs"]

        gpkg_info = {
            "layers": [
                {
                    "name": "lod22_2d",  # This is a surface layer
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

    def test_field_type_validation(self, resources):
        """Test validation of field types and nullable settings."""
        specs = resources["specs"]

        gpkg_info = {
            "layers": [
                {
                    "name": "lod12_3d",
                    "fields": [
                        {
                            "name": "labels",
                            "type": "StringList",
                            "nullable": False,
                        },  # Wrong type
                        {
                            "name": "b3_pand_deel_id",
                            "type": "Integer",
                            "nullable": True,
                        },  # Wrong nullable
                        {"name": "identificatie", "type": "String", "nullable": False},
                    ],
                }
            ]
        }
        results = list(gpkg_validate_attributes(specs, gpkg_info))
        print(list(str(x) for x in results))

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
