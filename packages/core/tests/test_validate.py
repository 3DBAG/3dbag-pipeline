from bag3d.core.assets.export import validate
import pytest


def test_obj(context, test_data_dir):
    res = validate.obj(
        context.resources.validation.app,
        test_data_dir / "validation_input/",
        "10-564-624",
        planarity_n_tol=20.0,
        planarity_d2p_tol=0.001,
        url_root="https://data.3dbag.nl",
        version="test",
    )
    assert res.zip_ok
    assert not res.file_ok


def test_gpkg(context, test_data_dir):
    res = validate.gpkg(
        context.resources.gdal.app,
        test_data_dir / "validation_input/",
        "10-564-624",
        "https://data.3dbag.nl",
        "test",
    )
    assert res.zip_ok
    assert res.file_ok
    assert res.nr_building == 419
    assert res.nr_buildingpart == 422
    assert res.nr_invalid_2d_geom == 0


def test_cityjson(context, test_data_dir):
    res = validate.cityjson(
        context.resources.validation.app,
        test_data_dir / "validation_input/",
        "10-564-624",
        planarity_n_tol=20.0,
        planarity_d2p_tol=0.001,
        url_root="https://data.3dbag.nl",
        version="test",
        specs=context.resources.specs,
    )
    assert res.zip_ok
    assert res.sha256 is not None
    assert not res.file_ok


def test_obj_missing(context_missing, test_data_dir):
    with pytest.raises(Exception):
        _ = validate.obj(
            context_missing.resources.validation.app,
            test_data_dir / "validation_input/",
            "10-564-624",
            planarity_n_tol=20.0,
            planarity_d2p_tol=0.001,
            url_root="https://data.3dbag.nl",
            version="test",
        )


def test_gpkg_missing(context_missing, test_data_dir):
    with pytest.raises(Exception):
        _ = validate.gpkg(
            context_missing.resources.gdal.app,
            test_data_dir / "validation_input/",
            "10-564-624",
            "https://data.3dbag.nl",
            "test",
        )


def test_cityjson_missing(context_missing, test_data_dir):
    with pytest.raises(Exception):
        _ = validate.cityjson(
            context_missing.resources.validation.app,
            test_data_dir / "validation_input/",
            "10-564-624",
            planarity_n_tol=20.0,
            planarity_d2p_tol=0.001,
            url_root="https://data.3dbag.nl",
            version="test",
        )
