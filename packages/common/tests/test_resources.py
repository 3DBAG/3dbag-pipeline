from pathlib import Path

from bag3d.common.resources.database import DatabaseResource
from bag3d.common import resources
from dagster import EnvVar
from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
    LASToolsResource,
)
from bag3d.common.utils.geodata import pdal_info


def test_gdal_local(test_data_dir):
    """Use local GDAL installation"""
    gdal_resource = GDALResource(
        exe_ogr2ogr=EnvVar("EXE_PATH_OGR2OGR").get_value(),
        exe_ogrinfo=EnvVar("EXE_PATH_OGRINFO").get_value(),
        exe_sozip=EnvVar("EXE_PATH_SOZIP").get_value(),
    )

    assert not gdal_resource.with_docker

    gdal = gdal_resource.app

    local_path = test_data_dir / Path("top10nl.zip")
    return_code, output = gdal.execute(
        "ogrinfo", "{exe} -so -al /vsizip/{local_path}", local_path=local_path
    )
    assert return_code == 0


def test_pdal_local(laz_files_ahn3_dir):
    """Use local PDAL installation"""
    pdal = PDALResource(exe_pdal=EnvVar("EXE_PATH_PDAL").get_value())
    assert not pdal.with_docker
    filepath = laz_files_ahn3_dir / "t_1042098.laz"
    return_code, output = pdal_info(pdal.app, filepath, with_all=True)
    assert return_code == 0


def test_lastools(laz_files_ahn3_dir):
    lastools_resource = LASToolsResource(
        exe_lasindex=EnvVar("EXE_PATH_LASINDEX").get_value(),
        exe_las2las=EnvVar("EXE_PATH_LAS2LAS").get_value(),
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
    """Can we create a local temporary directory with random id
    with the correct permissions?"""
    res = resources.files.FileStoreResource().file_store
    path = Path(res.data_dir)
    assert path.exists()
    with (res.data_dir / "file.txt").open("w") as fo:
        fo.write("test")
    with (res.data_dir / "file.txt").open("r") as fo:
        assert fo.read() == "test"
    res.rm(force=True)
    assert not res.data_dir
    assert not path.exists()


def test_file_store_init_temp_with_id():
    """Can we create a local temporary directory with input id
    with the correct permissions?"""
    res = resources.files.FileStoreResource(dir_id="myID").file_store
    path = Path(res.data_dir)
    assert path.exists()
    assert str(res.data_dir)[-4:] == "myID"
    with (res.data_dir / "file.txt").open("w") as fo:
        fo.write("test")
    with (res.data_dir / "file.txt").open("r") as fo:
        assert fo.read() == "test"
    res.rm(force=True)
    assert not res.data_dir
    assert not path.exists()


def test_file_store_init_data_dir(tmp_path):
    """Can we use an existing directory?"""
    res = resources.files.FileStoreResource(data_dir=tmp_path).file_store
    path = Path(res.data_dir)
    assert path.exists()
    assert path == tmp_path
    with (path / "file.txt").open("w") as fo:
        fo.write("test")
    with (tmp_path / "file.txt").open("r") as fo:
        assert fo.read() == "test"
    res.rm(force=True)
    assert not path.exists()


def test_db_connection_init():
    """Can we initialize a local database resource?"""
    db = DatabaseResource(
        host=EnvVar("BAG3D_PG_HOST").get_value(),
        user=EnvVar("BAG3D_PG_USER").get_value(),
        password=EnvVar("BAG3D_PG_PASSWORD").get_value(),
        port=EnvVar("BAG3D_PG_PORT").get_value(),
        dbname=EnvVar("BAG3D_PG_DATABASE").get_value(),
    ).connect
    q = db.get_query("select version();")
    assert "PostgreSQL" in q[0][0]
