from pathlib import Path

import pytest
from pgutils import PostgresTableIdentifier

from bag3d.common.utils.geodata import (
    geojson_poly_to_wkt,
    ogr2postgres,
    parse_ogrinfo,
)


def test_parse_ogrinfo():
    """Can we parse the STDOUT of ogrinfo?"""
    ogrinfo_stdout = """
INFO: Open of `/vsizip//tmp/bgt.zip/bgt_pand.gml'
      using driver `GML' successful.

Layer name: Pand
Geometry (nummeraanduidingreeks_1.positie_1.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_1.positie_2.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_1.positie_3.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_2.positie_1.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_2.positie_2.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_2.positie_3.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_3.positie_1.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_3.positie_2.plaatsingspunt): Point
Geometry (nummeraanduidingreeks_3.positie_3.plaatsingspunt): Point
Geometry (geometrie2d): Multi Surface
Feature Count: 20581
Extent (nummeraanduidingreeks_1.positie_1.plaatsingspunt): (135933.791000, 455926.736000) - (138033.705000, 458084.209000)
Extent (nummeraanduidingreeks_2.positie_1.plaatsingspunt): (135913.169000, 455948.842000) - (138014.418000, 457905.693000)
Extent (nummeraanduidingreeks_3.positie_1.plaatsingspunt): (135983.398000, 455947.784000) - (137975.558000, 457744.678000)
Extent (geometrie2d): (135811.380000, 455897.793000) - (138037.957000, 458171.237000)
SRS WKT (nummeraanduidingreeks_1.positie_1.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_1.positie_2.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_1.positie_3.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_2.positie_1.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_2.positie_2.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_2.positie_3.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_3.positie_1.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_3.positie_2.plaatsingspunt):
(unknown)
SRS WKT (nummeraanduidingreeks_3.positie_3.plaatsingspunt):
(unknown)
SRS WKT (geometrie2d):
(unknown)
Geometry Column 1 = nummeraanduidingreeks_1.positie_1.plaatsingspunt
Geometry Column 2 = nummeraanduidingreeks_1.positie_2.plaatsingspunt
Geometry Column 3 = nummeraanduidingreeks_1.positie_3.plaatsingspunt
Geometry Column 4 = nummeraanduidingreeks_2.positie_1.plaatsingspunt
Geometry Column 5 = nummeraanduidingreeks_2.positie_2.plaatsingspunt
Geometry Column 6 = nummeraanduidingreeks_2.positie_3.plaatsingspunt
Geometry Column 7 = nummeraanduidingreeks_3.positie_1.plaatsingspunt
Geometry Column 8 = nummeraanduidingreeks_3.positie_2.plaatsingspunt
Geometry Column 9 = nummeraanduidingreeks_3.positie_3.plaatsingspunt
Geometry Column 10 NOT NULL = geometrie2d
gml_id: String (0.0) NOT NULL
objectBeginTijd: Date (0.0)
objectEindTijd: Date (0.0)
identificatie.namespace: String (0.0)
identificatie.lokaalID: String (0.0)
tijdstipRegistratie: DateTime (0.0)
eindRegistratie: DateTime (0.0)
LV-publicatiedatum: DateTime (0.0)
bronhouder: String (0.0)
inOnderzoek: Integer(Boolean) (0.0)
relatieveHoogteligging: Integer (0.0)
bgt-status: String (0.0)
plus-status: String (0.0)
identificatieBAGPND: String (0.0)
nummeraanduidingreeks_1.tekst: String (0.0)
nummeraanduidingreeks_1.positie_1.hoek: Real (0.0)
nummeraanduidingreeks_1.positie_2.hoek: Real (0.0)
nummeraanduidingreeks_1.positie_3.hoek: Real (0.0)
nummeraanduidingreeks_1.identificatieBAGVBOLaagsteHuisnummer: String (0.0)
nummeraanduidingreeks_1.identificatieBAGVBOHoogsteHuisnummer: String (0.0)
nummeraanduidingreeks_2.tekst: String (0.0)
nummeraanduidingreeks_2.positie_1.hoek: Real (0.0)
nummeraanduidingreeks_2.positie_2.hoek: Real (0.0)
nummeraanduidingreeks_2.positie_3.hoek: Real (0.0)
nummeraanduidingreeks_2.identificatieBAGVBOLaagsteHuisnummer: String (0.0)
nummeraanduidingreeks_2.identificatieBAGVBOHoogsteHuisnummer: String (0.0)
nummeraanduidingreeks_3.tekst: String (0.0)
nummeraanduidingreeks_3.positie_1.hoek: Real (0.0)
nummeraanduidingreeks_3.positie_2.hoek: Real (0.0)
nummeraanduidingreeks_3.positie_3.hoek: Real (0.0)
nummeraanduidingreeks_3.identificatieBAGVBOLaagsteHuisnummer: String (0.0)
nummeraanduidingreeks_3.identificatieBAGVBOHoogsteHuisnummer: String (0.0)
    """
    layername, layerinfo = parse_ogrinfo(ogrinfo_stdout, "pand")

    assert layername == "pand"
    assert layerinfo["Feature Count [pand]"] == 20581


def test_geojson_poly_to_wkt():
    geometry = {
        "coordinates": [
            [
                [45000, 387500],
                [45000, 393750],
                [50000, 393750],
                [50000, 387500],
                [45000, 387500],
            ]
        ],
        "type": "Polygon",
    }
    wkt = geojson_poly_to_wkt(geometry)
    assert (
        wkt
        == "POLYGON((45000 387500,45000 393750,50000 393750,50000 387500,45000 387500))"
    )


class _FakeConnection:
    dsn = "host=localhost dbname=test"


class _FakeComputationDb:
    connection = _FakeConnection()


class _FakeOgrResult:
    def __init__(self, success, stderr="", stdout=""):
        self.success = success
        self.stderr = stderr
        self.stdout = stdout


class _FakeOgrRunner:
    def __init__(self, success, stderr="", stdout=""):
        self._result = _FakeOgrResult(success, stderr, stdout)
        self.command = None
        self.kwargs = None

    def run(self, command, *, exe_name=None, kwargs=None, local_path=None, logger=None):
        self.command = command
        self.kwargs = kwargs
        return self._result


def test_ogr2postgres_raises_on_failure():
    runner = _FakeOgrRunner(False, stderr="null value in column ...")
    with pytest.raises(RuntimeError, match="ogr2ogr failed"):
        ogr2postgres(
            gdal_runner=runner,  # type: ignore[arg-type]
            dataset="bgt",
            extract_path=Path("/tmp/extract"),
            feature_type="pand",
            xsd="http://example.com/test.xsd",
            new_table=PostgresTableIdentifier("stage_bgt", "pand"),
            computation_db=_FakeComputationDb(),  # type: ignore[arg-type]
            where="geometrie2d IS NOT NULL",
        )
    assert runner.kwargs is not None
    assert runner.kwargs["where_clause"] == '-where "geometrie2d IS NOT NULL" '


def test_ogr2postgres_no_where_clause():
    runner = _FakeOgrRunner(False, stderr="boom")
    with pytest.raises(RuntimeError, match="ogr2ogr failed"):
        ogr2postgres(
            gdal_runner=runner,  # type: ignore[arg-type]
            dataset="bgt",
            extract_path=Path("/tmp/extract"),
            feature_type="pand",
            xsd="http://example.com/test.xsd",
            new_table=PostgresTableIdentifier("stage_bgt", "pand"),
            computation_db=_FakeComputationDb(),  # type: ignore[arg-type]
        )
    assert runner.kwargs is not None
    assert runner.kwargs["where_clause"] == ""
