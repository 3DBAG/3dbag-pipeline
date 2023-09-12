from subprocess import run
from pathlib import Path

from pytest import mark
from dagster import build_init_resource_context
import docker

from bag3d.common import resources


@mark.parametrize(
    ("config", "local_path"),
    (
            ({"docker": {"image": "osgeo/gdal:alpine-small-3.5.2",
                         "mount_point": "/tmp"}}, Path("/data/top10nl.zip")),
            ({"exes": {"ogrinfo": "ogrinfo"}}, Path("/data/top10nl.zip"))
    ),
    ids=["docker", "local"]
)
def test_gdal(config, local_path):
    """Use GDAL in a docker image"""
    init_context = build_init_resource_context(
        config=config
    )
    gdal_exe = resources.executables.gdal(init_context)
    if "docker" in config:
        assert gdal_exe.with_docker
    elif "exes" in config:
        assert not gdal_exe.with_docker
    return_code, output = gdal_exe.execute("ogrinfo",
                                           "{exe} -so -al /vsizip/{local_path}",
                                           local_path=local_path)
    # assert "released" in output
    print(output)


def test_file_store_init_temp():
    """Can we create a local temporary directory with the correct permissions?"""
    init_context = build_init_resource_context(
        config={}
    )
    res = resources.files.file_store(init_context)
    assert res.data_dir.exists()
    with (res.data_dir / "file.txt").open("w") as fo:
        fo.write("test")
    with (res.data_dir / "file.txt").open("r") as fo:
        pass
    res.rm(force=True)


def test_file_store_init_data_dir():
    """Can we use an existing directory?"""
    tmp = resources.FileStore.mkdir_temp(temp_dir_id='70784c0e')
    init_context = build_init_resource_context(
        config={"data_dir": str(tmp)}
    )
    res = resources.files.file_store(init_context)
    assert res.data_dir.exists()
    assert res.data_dir.exists()
    with (res.data_dir / "file.txt").open("w") as fo:
        fo.write("test")
    with (tmp / "file.txt").open("r") as fo:
        fo.read()
    res.rm(force=True)


def test_docker_db_connection_init():
    """Can we initialize a database in a docker container?"""
    user = "db3dbag_user"
    pw = str("db3dbag_1234")
    db = "postgres"
    init_context = build_init_resource_context(
        config={
            "docker": {
                "image_id": "balazsdukai/3dbag-sample-data:latest",
                "environment": {
                    "POSTGRES_USER": user,
                    "POSTGRES_DB": db,
                    "POSTGRES_PASSWORD": pw,
                },
            },
            "port": 5562,
            "user": user,
            "password": pw,
            "dbname": db
        },
        resources={"container": resources.container.configured({})}
    )
    res = resources.database.db_connection(init_context)

    docker_client = docker.from_env()
    container = docker_client.containers.get(res.container_id)

    out = run(
        ["psql", "-c", "select PostGIS_full_version();"],
        env={
            "PGHOST": "localhost",
            "PGPORT": "5562",
            "PGUSER": user,
            "PGDATABASE": db,
            "PGPASSWORD": pw},
        capture_output=True)
    q = res.get_query("select version();")
    container.remove(force=True, v=True)

    assert out.returncode == 0
    assert "POSTGIS" in out.stdout.decode("utf-8")
    assert "PostgreSQL" in q[0][0]
    assert res.user == user


def test_db_connection_init(database):
    """Can we initialize a local database resource?"""
    data_dir, db = database
    init_context = build_init_resource_context(
        config={
            "port": db.port,
            "user": db.user,
            "password": db.password,
            "dbname": db.dbname
        },
        resources={
            "container": resources.container.configured({})
        }
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
