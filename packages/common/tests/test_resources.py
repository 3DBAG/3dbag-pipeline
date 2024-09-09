import os
from pathlib import Path

import pytest
from bag3d.common import resources
from dagster import build_init_resource_context
from bag3d.common.resources.executables import GdalResource, DockerConfig, DOCKER_GDAL_IMAGE


def test_gdal(test_data_dir):
    """Use GDAL in a docker image"""

    gdal_docker = GdalResource(docker_cfg=DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp"))
    assert gdal_docker.with_docker

    gdal_local = GdalResource(exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
                              exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
                              exe_sozip=os.getenv("EXE_PATH_SOZIP"))
    assert not gdal_local.with_docker

    local_path = test_data_dir / Path("top10nl.zip")
    return_code, output = gdal_docker.gdal.execute(
        "ogrinfo", "{exe} -so -al /vsizip/{local_path}", local_path=local_path
    )
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
    tmp = resources.files.FileStore.mkdir_temp(temp_dir_id="70784c0e")
    init_context = build_init_resource_context(config={"data_dir": str(tmp)})
    res = resources.files.file_store(init_context)
    assert res.data_dir.exists()
    assert res.data_dir.exists()
    with (res.data_dir / "file.txt").open("w") as fo:
        fo.write("test")
    with (tmp / "file.txt").open("r") as fo:
        fo.read()
    res.rm(force=True)


@pytest.mark.xfail("Test is probably false positive")
def test_file_store(tmp_path):
    f_store = resources.files.FileStore(
        data_dir=tmp_path / "test", docker_volume_id="foo"
    )
    f_store.rm


def test_db_connection_init(database):
    """Can we initialize a local database resource?"""
    db = database
    init_context = build_init_resource_context(
        config={
            "port": int(db.port),
            "user": db.user,
            "password": db.password,
            "dbname": db.dbname,
        }
    )
    res = resources.database.db_connection(init_context)
    q = res.get_query("select version();")
    assert "PostgreSQL" in q[0][0]
    assert res.user == db.user
