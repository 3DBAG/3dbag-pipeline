from pathlib import Path

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.specs import Specs3DBAGResource
from dagster import EnvVar
from psycopg.sql import SQL
from bag3d.common.resources.executables import (
    GDALResource,
    PDALResource,
    LASToolsResource,
)
from bag3d.common.utils.geodata import pdal_info


def test_specs_3dbag():
    """Can we load the 3DBAG attributes specs?"""
    specs = Specs3DBAGResource()
    assert len(specs.attributes) > 0


def test_gdal_local(test_data_dir):
    """Use local GDAL installation"""
    gdal_resource = GDALResource(
        exe_ogr2ogr=EnvVar("EXE_PATH_OGR2OGR").get_value() or "",
        exe_ogrinfo=EnvVar("EXE_PATH_OGRINFO").get_value() or "",
        exe_sozip=EnvVar("EXE_PATH_SOZIP").get_value() or "",
    )

    assert not gdal_resource.with_docker

    gdal = gdal_resource.runner

    local_path = test_data_dir / Path("top10nl.zip")
    result = gdal.run(
        "{exe} -so -al /vsizip/{local_path}",
        exe_name="ogrinfo",
        local_path=local_path,
    )
    assert result.success


def test_pdal_local(sample_laz_file):
    """Use local PDAL installation"""
    pdal = PDALResource(exe_pdal=EnvVar("EXE_PATH_PDAL").get_value() or "")
    assert not pdal.with_docker
    return_code, output = pdal_info(pdal.runner, sample_laz_file, with_all=True)
    assert return_code == 0


def test_lastools(sample_laz_file):
    lastools_resource = LASToolsResource(
        exe_lasindex=EnvVar("EXE_PATH_LASINDEX").get_value() or "",
        exe_las2las=EnvVar("EXE_PATH_LAS2LAS").get_value() or "",
        exe_lasinfo=EnvVar("EXE_PATH_LASINFO").get_value() or "",
    )
    assert not lastools_resource.with_docker

    lastools = lastools_resource.runner

    cmd_list = [
        "{exe}",
        "-i {local_path}",
        "-append",
        "-tile_size",
        "100",
        "-dont_reindex",
    ]
    result = lastools.run(
        " ".join(cmd_list),
        exe_name="lasindex",
        local_path=sample_laz_file,
    )

    assert result.success


def test_file_store_init_data_dir(tmp_path):
    """Can we use an existing directory?"""
    res = FileStoreResource(data_dir=str(tmp_path))
    path = res.path
    assert path.exists()
    assert path == tmp_path
    with (path / "file.txt").open("w") as fo:
        fo.write("test")
    with (tmp_path / "file.txt").open("r") as fo:
        assert fo.read() == "test"
    res.rm(force=True)
    assert not path.exists()


def test_production_db_init():
    """Can we initialize a local database resource?"""
    db = DatabaseResource(
        host=EnvVar("BAG3D_PG_HOST").get_value() or "",
        user=EnvVar("BAG3D_PG_USER").get_value() or "",
        password=EnvVar("BAG3D_PG_PASSWORD").get_value(),
        port=int(EnvVar("BAG3D_PG_PORT").get_value() or "5432"),
        dbname=EnvVar("BAG3D_PG_DATABASE").get_value() or "",
    ).connection
    q = db.get_query(SQL("select version();"))
    assert "PostgreSQL" in q[0][0]
