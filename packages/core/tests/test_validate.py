from bag3d.core.assets.export import validate


def test_obj(context, test_data_dir):
    r = validate.obj(
        context.resources.validation.app,
        test_data_dir
        / "reconstruction_input/3DBAG/export_test_version/tiles/10/564/624/",
        "10-564-624",
        planarity_n_tol=20.0,
        planarity_d2p_tol=0.001,
        url_root="https://data.3dbag.nl",
        version="test",
    )
    print(r)
