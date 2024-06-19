from pathlib import Path
from subprocess import run

from bag3d.common import resources
from dagster import build_init_resource_context
from pytest import mark


@mark.parametrize(
    ("config", "data_dir", "filename"),
    (
        (
            {
                "docker": {
                    "image": "osgeo/gdal:alpine-small-3.5.2",
                    "mount_point": "/tmp",
                }
            },
            "test_data_dir",
            Path("top10nl.zip"),
        ),
        ({"exes": {"ogrinfo": "ogrinfo"}}, "test_data_dir", Path("top10nl.zip")),
    ),
    ids=["docker", "local"],
)
def test_gdal(config, data_dir, filename, request):
    """Use GDAL in a docker image"""
    init_context = build_init_resource_context(config=config)
    gdal_exe = resources.executables.gdal(init_context)
    if "docker" in config:
        assert gdal_exe.with_docker
    elif "exes" in config:
        assert not gdal_exe.with_docker
    local_path = request.getfixturevalue(data_dir) / filename
    return_code, output = gdal_exe.execute(
        "ogrinfo", "{exe} -so -al /vsizip/{local_path}", local_path=local_path
    )
    # assert "released" in output
    print(output)


def test_file_store_init_temp():
    """Can we create a local temporary directory with the correct permissions?"""
    init_context = build_init_resource_context(config={})
    res = resources.files.file_store(init_context)
    assert res.data_dir.exists()
    with (res.data_dir / "file.txt").open("w") as fo:
        fo.write("test")
    with (res.data_dir / "file.txt").open("r") as fo:
        pass
    res.rm(force=True)


def test_file_store_init_data_dir():
    """Can we use an existing directory?"""
    tmp = resources.FileStore.mkdir_temp(temp_dir_id="70784c0e")
    init_context = build_init_resource_context(config={"data_dir": str(tmp)})
    res = resources.files.file_store(init_context)
    assert res.data_dir.exists()
    assert res.data_dir.exists()
    with (res.data_dir / "file.txt").open("w") as fo:
        fo.write("test")
    with (tmp / "file.txt").open("r") as fo:
        fo.read()
    res.rm(force=True)


def test_db_connection_init(database):
    """Can we initialize a local database resource?"""
    db = database
    init_context = build_init_resource_context(
        config={
            "port": db.port,
            "user": db.user,
            "password": db.password,
            "dbname": db.dbname,
        },
        resources={"container": resources.container.configured({})},
    )
    res = resources.database.db_connection(init_context)
    q = res.get_query("select version();")
    assert "PostgreSQL" in q[0][0]
    assert res.user == db.user


def test_docker_hub():
    """Is it OK when we read the config values from environment variables that are not
    defined?"""
    init_context = build_init_resource_context(
        config={
            "username": None,
            "password": None,
        }
    )
    res = resources.docker_hub(init_context)
