"""Native extract regression against GDAL and a disposable PostGIS database.

Set BAG3D_TEST_GDAL_BIN and BAG3D_TEST_POSTGRES_DSN explicitly. These tests
replace the staging/output tables in that database; never use a deployment DB.
"""

import os
from pathlib import Path
from zipfile import ZipFile
from typing import Any, cast

import pytest
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import GDALResource
from dagster import Failure
from psycopg import connect
from psycopg.conninfo import conninfo_to_dict

from bag3d.core.assets.bgt.load import bgt_pandactueelbestaand, stage_bgt_pand
from bag3d.core.assets.integration_data import (
    AOI_WKT,
    _aoi_bbox,
    _copy_native_gml_archive,
    _native_gml_extents,
)
from bag3d.core.assets.top10nl.load import stage_top10nl_gebouw, top10nl_gebouw
from bag3d.core.fixture_contract import validate_native_extent

pytestmark = pytest.mark.skipif(
    not os.environ.get("BAG3D_TEST_GDAL_BIN")
    or not os.environ.get("BAG3D_TEST_POSTGRES_DSN"),
    reason="requires explicitly configured GDAL and disposable PostGIS",
)


def test_native_archives_load_through_production_assets(tmp_path):
    parameters = {
        key: str(value)
        for key, value in conninfo_to_dict(
            os.environ["BAG3D_TEST_POSTGRES_DSN"]
        ).items()
        if value is not None
    }
    database = DatabaseResource(
        host=parameters["host"],
        user=parameters["user"],
        password=parameters.get("password"),
        dbname=parameters["dbname"],
        port=int(parameters.get("port", "5432")),
    )
    binaries = Path(os.environ["BAG3D_TEST_GDAL_BIN"])
    gdal = GDALResource(
        exe_ogrinfo=str(binaries / "ogrinfo"),
        exe_ogr2ogr=str(binaries / "ogr2ogr"),
        exe_sozip=str(binaries / "sozip"),
    )
    fixtures = {}
    for dataset, layer in (("bgt", "pand"), ("top10nl", "gebouw")):
        source = tmp_path / f"source_{dataset}.zip"
        member = f"{dataset}_{layer}.gml"
        with ZipFile(source, "w") as archive:
            archive.write(Path(__file__).parent / "data" / member, member)
        fixture = _copy_native_gml_archive(
            source, tmp_path / dataset / f"{dataset}.zip", dataset
        )
        assert fixture.read_bytes() == source.read_bytes()
        for extent in _native_gml_extents(fixture, dataset, gdal):
            validate_native_extent(extent, _aoi_bbox(AOI_WKT), dataset)
        fixtures[dataset] = fixture

    stage = cast(Any, stage_bgt_pand.op.compute_fn).decorated_fn(
        database, gdal, fixtures["bgt"]
    )
    cast(Any, bgt_pandactueelbestaand.op.compute_fn).decorated_fn(database, stage.value)
    stage = cast(Any, stage_top10nl_gebouw.op.compute_fn).decorated_fn(
        database, gdal, fixtures["top10nl"]
    )
    cast(Any, top10nl_gebouw.op.compute_fn).decorated_fn(database, stage.value)

    with (
        connect(os.environ["BAG3D_TEST_POSTGRES_DSN"]) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT identificatiebagpnd, bgt_status, lv_publicatiedatum IS NOT NULL, GeometryType(geometrie2d), GeometryType(wkb_geometry) FROM stage_bgt.pand"
        )
        assert cursor.fetchone() == (
            "0363100012171540",
            "bestaand",
            True,
            "MULTISURFACE",
            "POINT",
        )
        cursor.execute(
            "SELECT count(*), bool_and(ST_IsValid(geometrie)) FROM bgt.pandactueelbestaand"
        )
        assert cursor.fetchone() == (1, True)
        cursor.execute("SELECT count(*) FROM stage_top10nl.gebouw")
        assert cursor.fetchone() == (2,)
        cursor.execute(
            "SELECT typegebouw, ST_IsValid(geometrie_vlak) FROM top10nl.gebouw"
        )
        assert cursor.fetchone() == (["overig", "kerk"], True)

    # Reproduce the former conversion, then verify that its import fails the asset.
    rewritten = tmp_path / "rewritten.gml"
    conversion = gdal.runner.run(
        "{exe} -f GML '{output}' '/vsizip/{local_path}/bgt_pand.gml'",
        exe_name="ogr2ogr",
        local_path=fixtures["bgt"],
        kwargs={"output": rewritten},
    )
    assert conversion.success
    invalid = tmp_path / "rewritten.zip"
    with ZipFile(invalid, "w") as archive:
        archive.write(rewritten, "bgt_pand.gml")
    try:
        cast(Any, stage_bgt_pand.op.compute_fn).decorated_fn(database, gdal, invalid)
    except Failure as error:
        assert "geometrie2d" in str(error)
    else:
        raise AssertionError("Rewritten fixture import must fail the staging asset")
