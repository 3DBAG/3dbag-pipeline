import os
from pathlib import Path

import pytest
from bag3d.common import resources
from dagster import build_init_resource_context
from bag3d.common.resources.executables import (
    DOCKER_GDAL_IMAGE,
    DOCKER_PDAL_IMAGE,
    GdalResource,
    PdalResource,
    DockerConfig,
)
from bag3d.common.utils.geodata import pdal_info


def test_gdal_docker(test_data_dir):
    """Use GDAL in a docker image"""
    gdal = GdalResource(
        docker_cfg=DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")
    )
    assert gdal.with_docker

    local_path = test_data_dir / Path("top10nl.zip")
    return_code, output = gdal.gdal.execute(
        "ogrinfo", "{exe} -so -al /vsizip/{local_path}", local_path=local_path
    )
    print(output)


def test_gdal_local(test_data_dir):
    """Use local GDAL installation"""
    gdal = GdalResource(
        exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
        exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
        exe_sozip=os.getenv("EXE_PATH_SOZIP"),
    )

    assert not gdal.with_docker

    local_path = test_data_dir / Path("top10nl.zip")
    return_code, output = gdal.gdal.execute(
        "ogrinfo", "{exe} -so -al /vsizip/{local_path}", local_path=local_path
    )
    print(output)


def test_pdal_docker(test_data_dir):
    """Use PDAL in a docker image"""
    pdal = PdalResource(
        docker_cfg=DockerConfig(image=DOCKER_PDAL_IMAGE, mount_point="/tmp")
    )
    assert pdal.with_docker
    filepath = test_data_dir / "pointcloud/AHN3/tiles_200m/t_1042098.laz"
    output = pdal_info(pdal.pdal, filepath, with_all=True)


def test_pdal_local(test_data_dir):
    """Use local PDAL installation"""
    pdal = PdalResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))
    assert not pdal.with_docker
    filepath = test_data_dir / "pointcloud/AHN3/tiles_200m/t_1042098.laz"
    output = pdal_info(pdal.pdal, filepath, with_all=True)


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
