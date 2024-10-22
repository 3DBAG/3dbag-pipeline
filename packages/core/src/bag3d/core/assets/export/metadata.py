import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable
from uuid import uuid1
from copy import deepcopy

from dagster import (
    AssetKey,
    Output,
    asset,
    AssetExecutionContext,
    DagsterEventType,
    EventRecordsFilter,
)
from psycopg.sql import SQL

from bag3d.common.utils.files import bag3d_export_dir, geoflow_crop_dir
from bag3d.common.utils.dagster import format_date
from bag3d.common.utils.files import check_export_results
from bag3d.common.resources import resource_defs


def get_info_per_cityobject(
    cityjson: dict, cityobject_info: dict, attribute_names: Iterable
) -> Dict[str, Dict]:
    """Given a CityJSON object as a dict, it returns information about
    the available LoD level per city object and the requested attributes.
    The output is a dictionary
    with the cityobject ids as keys. The value is a dictionary with
    the available lod-levels, and attributes as follows:
    {'NL.IMBAG.Pand.0614100000003764':{'1.2': 0, '1.3': 0, '2.2': 0, 'has_geometry': False, '...': ...}}
    """
    all_objects = {}
    for coid, co in cityjson["CityObjects"].items():
        geometry = co.get("geometry")
        if geometry and len(geometry) > 0:
            cityobject_info["has_geometry"] = True
            for g in geometry:
                cityobject_info[g["lod"]] = 1
        co_attributes = co.get("attributes")
        if co_attributes:
            for aname in attribute_names:
                cityobject_info[aname] = co_attributes.get(aname)
        all_objects[coid] = cityobject_info
    return all_objects


def features_to_csv(
    output_csv: Path,
    features: Dict[str, Dict[str, int]],
    cityobject_info: dict,
    lods: list,
) -> None:
    """Creates a csv with the city object id and the city object information"""
    fieldnames = ["id", "identificatie", "lod_0", "lod_12", "lod_13", "lod_22"]
    fieldnames += [k for k in cityobject_info.keys() if k not in lods]
    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for feature, coinfo in features.items():
            row = {
                "id": feature,
                "identificatie": feature[:30],
                "lod_0": coinfo["0"],
                "lod_12": coinfo["1.2"],
                "lod_13": coinfo["1.3"],
                "lod_22": coinfo["2.2"],
            }
            row.update({k: v for k, v in coinfo.items() if k not in lods})
            writer.writerow(row)


@asset(
    deps={AssetKey(("reconstruction", "reconstructed_building_models_nl"))},
    required_resource_keys={"file_store", "file_store_fastssd", "db_connection"},
)
def feature_evaluation(context):
    """Compare the reconstruction output to the input, for each feature.
    Check if all LoD-s are generated for the feature and include some attributes from
    the CityObjects"""
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.file_store.data_dir
    )
    output_dir = bag3d_export_dir(context.resources.file_store.file_store.data_dir)
    output_csv = output_dir.joinpath("reconstructed_features.csv")
    conn = context.resources.db_connection.connect

    lods = ("0", "1.2", "1.3", "2.2")
    attributes_to_include = (
        "b3_pw_selectie_reden",
        "b3_pw_bron",
        "b3_puntdichtheid_ahn3",
        "b3_puntdichtheid_ahn4",
        "b3_mutatie_ahn3_ahn4",
        "b3_nodata_fractie_ahn3",
        "b3_nodata_fractie_ahn4",
        "b3_nodata_radius_ahn3",
        "b3_nodata_radius_ahn4",
    )
    cityobject_info = {lod: 0 for lod in lods}
    cityobject_info["has_geometry"] = False
    cityobject_info.update(dict((a, None) for a in attributes_to_include))

    reconstructed_buildings = set()
    cityobjects = {}
    for path in Path(reconstructed_root_dir).rglob("*.city.jsonl"):
        reconstructed_buildings.add(path.stem[:-5])
        with open(path, "r") as f:
            cityjson = json.load(f)
            codata = get_info_per_cityobject(
                cityjson, deepcopy(cityobject_info), attributes_to_include
            )
        cityobjects.update(codata)
    context.log.debug(f"len(reconstructed_buildings)={len(reconstructed_buildings)}")
    context.log.debug(f"len(cityobjects)={len(cityobjects)}")

    res = conn.get_query(
        SQL("""
        SELECT identificatie
        FROM reconstruction_input.reconstruction_input;
        """)
    )
    input_buildings = set([row[0] for row in res])
    context.log.debug(f"len(input_buildings)={len(input_buildings)}")

    not_reconstructed = input_buildings.difference(reconstructed_buildings)
    context.log.debug(f"len(not_reconstructed)={len(not_reconstructed)}")

    for feature in not_reconstructed:
        cityobjects[feature] = cityobject_info

    features_to_csv(output_csv, cityobjects, cityobject_info, lods)

    return output_csv


@asset(
    deps={AssetKey(("export", "reconstruction_output_multitiles_nl"))},
    required_resource_keys={"file_store"},
)
def export_index(context):
    """Index of the distribution tiles.

    Parses the quadtree.tsv file output by *tyler* and checks if all formats exist for
    a tile. If a tile does not have any features in the quadtree, it is not included.
    Output it written to export_index.csv.
    """
    path_export_dir = bag3d_export_dir(context.resources.file_store.file_store.data_dir)
    path_tiles_dir = path_export_dir.joinpath("tiles")
    path_export_index = path_export_dir.joinpath("export_index.csv")
    path_quadtree_tsv = path_export_dir.joinpath("quadtree.tsv")

    with path_export_index.open("w") as fw:
        fieldnames = ["tile_id", "has_cityjson", "has_gpkg", "has_obj", "wkt"]
        csvwriter = csv.DictWriter(fw, fieldnames=fieldnames, extrasaction="ignore")
        csvwriter.writeheader()
        export_results_gen = check_export_results(path_quadtree_tsv, path_tiles_dir)
        csvwriter.writerows(dict(export_result) for export_result in export_results_gen)
    return path_export_index


ASSET_DEPENDENCIES_FOR_METADATA = [
    AssetKey(("bag", "extract_bag")),
    AssetKey(("bag", "bag_pandactueelbestaand")),
    AssetKey(("top10nl", "extract_top10nl")),
    AssetKey(("top10nl", "top10nl_gebouw")),
    AssetKey(("input", "reconstruction_input")),
]


@asset(deps=ASSET_DEPENDENCIES_FOR_METADATA, required_resource_keys={"file_store"})
def metadata(context: AssetExecutionContext):
    """3D BAG metadata for distribution.
    Metadata schema follows the Dutch metadata profile for geographical data,
    https://geonovum.github.io/Metadata-ISO19115/.

    For extended ISO lineage, see 19115-2, https://wiki.esipfed.org/ISO_Lineage. This
    has XML examples. And also https://wiki.esipfed.org/Data_Understanding_-_Provenance_(ISO-19115-1).
    """
    date_3dbag = format_date(date.today(), version=False)
    version_3dbag = f"v{format_date(date.today(), version=True)}"
    uuid_3dbag = str(uuid1())

    asset_keys = ASSET_DEPENDENCIES_FOR_METADATA
    instance = context.instance

    # Get only the last asset materialization, because if an asset is materialized then
    # is has succeeded.
    process_step_list = []
    for asset_key in asset_keys:
        event_record_list = instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
            ),
            limit=1,
        )
        if len(event_record_list) > 0:
            event_record = event_record_list[0]

            # Just because the extract_top10nl asset has a 'Feature Count [gebouw]' metadata
            # member instead of 'Rows'
            rows = event_record.asset_materialization.metadata.get("Rows")
            process_step_list.append(
                {
                    "name": ".".join(asset_key.path),
                    "runId": event_record.run_id,
                    "featureCount": rows.value if rows is not None else None,
                    "dateTime": datetime.fromtimestamp(event_record.timestamp)
                    .date()
                    .isoformat(),
                    "dataVersion": event_record.asset_materialization.tags[
                        "dagster/data_version"
                    ],
                }
            )

    top10NLdates = [
        ps["dataVersion"]
        for ps in process_step_list
        if ps["name"] == "top10nl.extract_top10nl"
    ]
    top10NLdate = None
    if len(top10NLdates) > 0:
        top10NLdate = top10NLdates[0]

    bagdates = [
        ps["dataVersion"] for ps in process_step_list if ps["name"] == "bag.extract_bag"
    ]
    bagdate = None
    if len(bagdates) > 0:
        bagdate = bagdates[0]

    metadata = {
        "identificationInfo": {
            "citation": {
                "title": "3D BAG",
                "date": date_3dbag,
                "dateType": "creation",
                "edition": version_3dbag,
                "identifier": uuid_3dbag,
            },
            "abstract": "De 3D BAG is een up-to-date landsdekkende dataset met 3D gebouwmodellen van Nederland. De 3D BAG is open data. Het bevat 3D modellen op verscheidene detailniveaus welke zijn gegenereerd door de combinatie van twee open datasets: de pand-gegevens uit de BAG en de hoogtegegevens uit de AHN. De 3D BAG wordt regelmatig geüpdatet met de meest recente openlijk beschikbare pand- en hoogtegegevens.",
            "pointOfContact": {
                "organisationName": "3D BAG",
                "contactInfo": {
                    "address": {
                        "country": "Nederland",
                        "electronicMailAddress": "info@3dbag.nl",
                    },
                    "onlineResource": "https://3dbag.nl",
                },
                "role": "pointOfContact",
            },
            "resourceConstraints": [
                {
                    "accessConstraints": "otherRestrictions",
                    "otherConstraints": [
                        {
                            "href": "http://creativecommons.org/licenses/by/4.0/?ref=chooser-v1",
                            "text": "Naamensvermelding verplicht, 3D BAG door de 3D geoinformation onderzoeksgroep (TU Delft) en 3DGI",
                        }
                    ],
                }
            ],
        },
        "language": "dut",
        "referenceSystemInfo": [
            {"referenceSystemIdentifier": "https://www.opengis.net/def/crs/EPSG/0/7415"}
        ],
        "dataQualityInfo": {
            "lineage": {
                "processStep": process_step_list,
                "source": [
                    {
                        "source": {
                            "name": "BAG 2.0 Extract",
                            "description": "Basisregistratie Adressen en Gebouwen (BAG) 2.0 Extract.",
                            "author": "Het Kadaster",
                            "website": "https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract",
                            "dateType": "creation",
                            "date": bagdate,
                            "licence": "http://creativecommons.org/publicdomain/mark/1.0/deed.nl",
                        },
                    },
                    {
                        "source": {
                            "name": "TOP10NL",
                            "description": "Basisregistratie Topografie (BRT) TOP10NL gebouwen laag, gedownload van de PDOK download API, gebruikt voor informatie over kassen en warenhuizen.",
                            "author": "Het Kadaster",
                            "website": "https://www.kadaster.nl/zakelijk/producten/geo-informatie/topnl",
                            "dateType": "access",
                            "date": top10NLdate,
                            "licence": "http://creativecommons.org/licenses/by/4.0/deed.nl",
                        },
                    },
                    {
                        "source": {
                            "name": "AHN3",
                            "description": "Actueel Hoogtebestaand Nederland (AHN) 3 puntenwolk (LAZ), gebruikt voor de hoogte-informatie voor de gebouwmodellen.",
                            "author": "Het Waterschapshuis",
                            "website": "https://www.ahn.nl",
                            "date": ["2014", "2019"],
                            "dateType": "creation",
                            "licence": "https://creativecommons.org/publicdomain/zero/1.0/deed.nl",
                        },
                    },
                    {
                        "source": {
                            "name": "AHN4",
                            "description": "Actueel Hoogtebestaand Nederland (AHN) 4 puntenwolk (LAZ), gebruikt voor de hoogte-informatie voor de gebouwmodellen.",
                            "author": "Het Waterschapshuis",
                            "website": "https://www.ahn.nl",
                            "date": ["2020", "2022"],
                            "dateType": "creation",
                            "licence": "https://creativecommons.org/publicdomain/zero/1.0/deed.nl",
                        },
                    },
                    {
                        "source": {
                            "name": "AHN5",
                            "description": "Actueel Hoogtebestaand Nederland (AHN) 5 puntenwolk (LAZ), gebruikt voor de hoogte-informatie voor de gebouwmodellen.",
                            "author": "Het Waterschapshuis",
                            "website": "https://www.ahn.nl",
                            "date": ["2023", "2025"],
                            "dateType": "creation",
                            "licence": "http://creativecommons.org/licenses/by/4.0/deed.nl",
                        },
                    },
                ],
                "software": [
                    {
                        "name": "geoflow-bundle",
                        "version": resource_defs["geoflow"].app.version("geof"),
                        "repository": "https://github.com/geoflow3d/geoflow-bundle",
                        "description": "3D building model reconstruction",
                    },
                    {
                        "name": "roofer",
                        "version": resource_defs["roofer"].app.version("crop"),
                        "repository": "https://github.com/3DGI/roofer",
                        "description": "Point cloud selection and cropping",
                    },
                    {
                        "name": "tyler",
                        "version": resource_defs["tyler"].app.version("tyler"),
                        "repository": "https://github.com/3DGI/tyler",
                        "description": "Generating GeoPackage, OBJ and CityJSON tiles",
                    },
                    {
                        "name": "tyler-db",
                        "version": resource_defs["tyler"].app.version("tyler-db"),
                        "repository": "https://github.com/3DGI/tyler/tree/postgres-footprints",
                        "description": "Input tiling",
                    },
                    {
                        "name": "GDAL",
                        "version": resource_defs["gdal"].app.version("ogr2ogr"),
                        "repository": "https://gdal.org/",
                        "description": "Data loading with ogr2ogr",
                    },
                    {
                        "name": "PDAL",
                        "version": resource_defs["pdal"].app.version("pdal"),
                        "repository": "https://pdal.io",
                        "description": "Computing point cloud metadata",
                    },
                    {
                        "name": "LASTools",
                        "version": resource_defs["lastools"].app.version("lasindex"),
                        "repository": "https://lastools.github.io/",
                        "description": "Point cloud tiling and indexing",
                    },
                ],
            },
        },
    }
    output_dir = bag3d_export_dir(context.resources.file_store.file_store.data_dir)
    outfile = output_dir.joinpath("metadata.json")
    with outfile.open("w") as fo:
        json.dump(metadata, fo)
    return Output(outfile, metadata=metadata)
