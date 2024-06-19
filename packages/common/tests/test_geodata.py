from pathlib import Path
from pprint import pprint

from bag3d.common.resources import gdal
from bag3d.common.utils.geodata import (add_info, geojson_poly_to_wkt,
                                        ogr2postgres, ogrinfo, parse_ogrinfo)
from dagster import build_op_context
from pgutils import PostgresTableIdentifier
from pytest import mark


@mark.parametrize(
    "config",
    ({"exes": {"ogrinfo": "ogrinfo"}}, {"docker": {"image": ""}}),
    ids=["exes", "docker"],
)
def test_info_exes(config, docker_gdal_image, test_data_dir):
    """Run ogrinfo with local exe and with docker"""
    if "docker" in config:
        config["docker"]["image"] = docker_gdal_image
    context = build_op_context(resources={"gdal": gdal.configured(config)})
    p = Path(f"{test_data_dir}/top10nl.zip")
    res = dict(
        ogrinfo(
            context=context,
            dataset="top10nl",
            extract_path=p,
            feature_types=[
                "gebouw",
            ],
            xsd="https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd",
        )
    )
    assert "gebouw" in res


@mark.skip(reason="Fails for BGT")
@mark.parametrize(
    "data",
    (
        (
            "top10nl.zip",
            "top10nl",
            [
                "gebouw",
            ],
            "https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd",
        ),
        (
            "bgt.zip",
            "bgt",
            ["pand", "wegdeel"],
            "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd",
        ),
    ),
    ids=lambda val: val[1],
)
def test_info_data(data, context, test_data_dir):
    """Can we run ogrinfo on all datasets?"""
    path, dataset, feature_types, xsd = data
    metadata = {
        "Extract Path": "path",
        "Download URL": "url",
        "Size [Mb]": 2.0,
        "timeliness": {"2022-10-08": feature_types},
    }
    res = ogrinfo(
        context=context,
        dataset=dataset,
        extract_path=Path(f"{test_data_dir}/{path}"),
        feature_types=feature_types,
        xsd=xsd,
    )
    assert res["gebouw"]["Feature Count [gebouw]"] == 1071
    add_info(metadata, res)
    assert metadata["Size [Mb]"] == 2.0
    assert metadata["Timeliness [gebouw]"] == "2022-10-08"


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


@mark.parametrize(
    "data",
    (
        (
            "top10nl.zip",
            "top10nl",
            [
                "gebouw",
            ],
            "https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd",
        ),
        (
            "bgt.zip",
            "bgt",
            ["pand", "wegdeel"],
            "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd",
        ),
    ),
    ids=lambda val: val[1],
)
def test_ogr2postgres(data, baseregisters_context, test_data_dir):
    path, dataset, feature_types, xsd = data
    res = ogr2postgres(
        context=baseregisters_context,
        dataset=dataset,
        extract_path=Path(f"{test_data_dir}/{path}"),
        feature_type=feature_types[0],
        xsd=xsd,
        new_table=PostgresTableIdentifier("public", feature_types[0]),
    )
    assert (
        res["Database.Schema.Table"] == f"baseregisters_test.public.{feature_types[0]}"
    )


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
