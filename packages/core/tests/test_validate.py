from bag3d.core.assets.export import validate


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


def test_cityjson(context, test_data_dir):
    res = validate.cityjson(
        context.resources.validation.app,
        test_data_dir / "validation_input/",
        "10-564-624",
        planarity_n_tol=20.0,
        planarity_d2p_tol=0.001,
        url_root="https://data.3dbag.nl",
        version="test",
    )
    assert res.zip_ok
    assert res.sha256 is not None
    assert not res.file_ok
