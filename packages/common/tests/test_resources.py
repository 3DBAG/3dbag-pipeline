import os
from pathlib import Path

import pytest
from bag3d.common import resources
from dagster import build_init_resource_context
from bag3d.common.resources.executables import (
    DOCKER_GDAL_IMAGE,
    DOCKER_PDAL_IMAGE,
    GDALResource,
    PDALResource,
    DockerConfig,
    LASToolsResource,
)
from bag3d.common.utils.geodata import pdal_info


def test_gdal_docker(test_data_dir):
    """Use GDAL in a docker image"""
    gdal_resource = GDALResource(
        docker_cfg=DockerConfig(image=DOCKER_GDAL_IMAGE, mount_point="/tmp")
    )
    assert gdal_resource.with_docker

    gdal = gdal_resource.app

    local_path = test_data_dir / Path("top10nl.zip")
    return_code, output = gdal.execute(
        "ogrinfo", "{exe} -so -al /vsizip/{local_path}", local_path=local_path
    )
    assert return_code == 0


@pytest.mark.needs_tools
def test_gdal_local(test_data_dir):
    """Use local GDAL installation"""
    gdal_resource = GDALResource(
        exe_ogr2ogr=os.getenv("EXE_PATH_OGR2OGR"),
        exe_ogrinfo=os.getenv("EXE_PATH_OGRINFO"),
        exe_sozip=os.getenv("EXE_PATH_SOZIP"),
    )

    assert not gdal_resource.with_docker

    gdal = gdal_resource.app

    local_path = test_data_dir / Path("top10nl.zip")
    return_code, output = gdal.execute(
        "ogrinfo", "{exe} -so -al /vsizip/{local_path}", local_path=local_path
    )
    assert return_code == 0


def test_pdal_docker(laz_files_ahn3_dir):
    """Use PDAL in a docker image"""
    pdal = PDALResource(
        docker_cfg=DockerConfig(image=DOCKER_PDAL_IMAGE, mount_point="/tmp")
    )
    assert pdal.with_docker
    filepath = laz_files_ahn3_dir / "t_1042098.laz"
    return_code, output = pdal_info(pdal.app, filepath, with_all=True)
    assert return_code == 0


@pytest.mark.needs_tools
def test_pdal_local(laz_files_ahn3_dir):
    """Use local PDAL installation"""
    pdal = PDALResource(exe_pdal=os.getenv("EXE_PATH_PDAL"))
    assert not pdal.with_docker
    filepath = laz_files_ahn3_dir / "t_1042098.laz"
    return_code, output = pdal_info(pdal.app, filepath, with_all=True)
    assert return_code == 0


@pytest.mark.needs_tools
def test_lastools(laz_files_ahn3_dir):
    lastools_resource = LASToolsResource(
        exe_lasindex=os.getenv("EXE_PATH_LASINDEX"),
        exe_las2las=os.getenv("EXE_PATH_LAS2LAS"),
    )
    assert not lastools_resource.with_docker

    lastools = lastools_resource.app

    filepath = laz_files_ahn3_dir / "t_1042098.laz"

    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-append",
        "-tile_size",
        "100",
        "-dont_reindex",
    ]
    return_code, output = lastools.execute(
        "lasindex", " ".join(cmd_list), local_path=filepath
    )

    assert return_code == 0


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
