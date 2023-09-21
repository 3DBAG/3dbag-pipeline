import csv
import json
from datetime import date
from pathlib import Path
from typing import Dict
from uuid import uuid1

from dagster import AssetKey, Output, asset
from psycopg.sql import SQL

from bag3d.common.utils.files import bag3d_export_dir, geoflow_crop_dir
from bag3d.common.utils.dagster import format_date, get_upstream_data_version
from bag3d.common.resources.temp_until_configurableresource import geoflow_version, \
    roofer_version, tyler_version, tyler_db_version
from bag3d.common.utils.files import check_export_results


def get_lods_per_cityobject(path: Path) -> Dict[str, Dict]:
    """ Given the path to a jsonl file, it returns the information about
        the available LoD level per city object. The output is a dictionary
        with the cityobject ids as keys. The value is a dictionary with 
        the available lod-levels, as follows:
        {'NL.IMBAG.Pand.0614100000003764':{'1.2': 0, '1.3': 0, '2.2': 0}}
    """
    all_objects = {}
    with open(path) as f:
        cityjson = json.load(f)
        for object in cityjson['CityObjects']:
            if 'parents' in cityjson['CityObjects'][object].keys():
                lods = {'1.2': 0, '1.3': 0, '2.2': 0}
                for geometry in cityjson['CityObjects'][object]['geometry']:
                    lods[geometry['lod']] = 1
                all_objects[object] = lods
    return all_objects


def features_to_csv(output_csv: Path,
                    features: Dict[str, Dict[str, int]]) -> None:
    """ Creates a csv with the city object id and three bool columns
        indicating the presence of lod 1.2, 1.3 and 2.2
    """
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile,
                                fieldnames=['id',
                                            'identificatie',
                                            'lod_12',
                                            'lod_13',
                                            'lod_22'])
        writer.writeheader()
        for feature, lods in features.items():
            writer.writerow({'id': feature,
                             'identificatie': feature[:30],
                             'lod_12': lods['1.2'],
                             'lod_13': lods['1.3'],
                             'lod_22': lods['2.2']})


@asset(
    non_argument_deps={
        AssetKey(("reconstruction", "reconstructed_building_models_nl"))
    },
    required_resource_keys={"file_store", "file_store_fastssd", "db_connection"}
)
def feature_evaluation(context):
    """Compare the reconstruction output to the input, for each feature.
    Check if all LoD-s are generated for the feature."""
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.data_dir)
    output_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    output_csv = output_dir.joinpath("reconstructed_features.csv")
    conn = context.resources.db_connection

    reconstructed_buildings = set()
    cityobjects = {}
    for path in Path(reconstructed_root_dir).rglob('*.city.jsonl'):
        reconstructed_buildings.add(path.stem[:-5])
        cityobjects.update(get_lods_per_cityobject(path))
    context.log.debug(f"len(reconstructed_buildings)={len(reconstructed_buildings)}")
    context.log.debug(f"len(cityobjects)={len(cityobjects)}")

    res = conn.get_query(
        SQL("""
        SELECT identificatie
        FROM reconstruction_input.reconstruction_input;
        """))
    input_buildings = set([row[0] for row in res])
    context.log.debug(f"len(input_buildings)={len(input_buildings)}")

    not_reconstructed = input_buildings.difference(reconstructed_buildings)
    context.log.debug(f"len(not_reconstructed)={len(not_reconstructed)}")

    for feature in not_reconstructed:
        cityobjects[feature] = {'1.2': 0, '1.3': 0, '2.2': 0}

    features_to_csv(output_csv, cityobjects)

    return output_csv


@asset(
    non_argument_deps={
        AssetKey(("export", "reconstruction_output_multitiles_nl"))
    },
    required_resource_keys={"file_store"}
)
def export_index(context):
    """Index of the distribution tiles."""
    path_export_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    path_tiles_dir = path_export_dir.joinpath("tiles")
    path_export_index = path_export_dir.joinpath("export_index.csv")
    path_quadtree_tsv = path_export_dir.joinpath("quadtree.tsv")

    with path_export_index.open("w") as fw:
        fieldnames = ["tile_id", "has_cityjson", "has_gpkg", "has_obj", "wkt"]
        csvwriter = csv.DictWriter(fw, fieldnames=fieldnames)
        csvwriter.writeheader()
        export_results_gen = check_export_results(path_quadtree_tsv, path_tiles_dir)
        csvwriter.writerows(dict(export_result)
                            for export_result in export_results_gen)
    return path_export_index


@asset(
    required_resource_keys={"file_store"}
)
def metadata(context):
    """3D BAG metadata for distribution.
    Metadata schema follows the Dutch metadata profile for geographical data,
    https://geonovum.github.io/Metadata-ISO19115/.

    For extended ISO lineage, see 19115-2, https://wiki.esipfed.org/ISO_Lineage. This
    has XML examples. And also https://wiki.esipfed.org/Data_Understanding_-_Provenance_(ISO-19115-1).
    """
    date_3dbag = format_date(date.today(), version=False)
    version_3dbag = f"v{format_date(date.today(), version=True)}"
    uuid_3dbag = str(uuid1())

    data_version_extract_bag = get_upstream_data_version(context, AssetKey(
        ("bag", "extract_bag")))
    data_version_top10nl = get_upstream_data_version(context, AssetKey(
        ("top10nl", "extract_top10nl")))

    metadata = {
        "identificationInfo": {
            "citation": {
                "title": "3D BAG",
                "date": date_3dbag,
                "dateType": "creation",
                "edition": version_3dbag,
                "identifier": uuid_3dbag
            },
            "abstract": "De 3D BAG is een up-to-date landsdekkende dataset met 3D gebouwmodellen van Nederland. De 3D BAG is open data. Het bevat 3D modellen op verscheidene detailniveaus welke zijn gegenereerd door de combinatie van twee open datasets: de pand-gegevens uit de BAG en de hoogtegegevens uit de AHN. De 3D BAG wordt regelmatig geüpdatet met de meest recente openlijk beschikbare pand- en hoogtegegevens.",
            "pointOfContact": {
                "organisationName": "3D BAG",
                "contactInfo": {
                    "address": {
                        "country": "Nederland",
                        "electronicMailAddress": "info@3dbag.nl",
                    },
                    "onlineResource": "https://3dbag.nl"
                },
                "role": "pointOfContact"
            },
            "resourceConstraints": [
                {
                    "accessConstraints": "otherRestrictions",
                    "otherConstraints": [
                        {
                            "href": "http://creativecommons.org/licenses/by/4.0/?ref=chooser-v1",
                            "text": "Naamensvermelding verplicht, 3D BAG door de 3D geoinformation onderzoeksgroep (TU Delft) en 3DGI"
                        }
                    ]
                }
            ],
        },
        "language": "dut",
        "referenceSystemInfo": [
            {
                "referenceSystemIdentifier": "https://www.opengis.net/def/crs/EPSG/0/7415"
            }
        ],
        "dataQualityInfo": {
            "lineage": {
                "source": [
                    {
                        "source": {
                            "description": "Basisregistratie Adressen en Gebouwen (BAG) 2.0 Extract.",
                            "author": "Het Kadaster",
                            "website": "https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract",
                            "date": data_version_extract_bag,
                            "dateType": "creation",
                            "licence": "http://creativecommons.org/publicdomain/mark/1.0/deed.nl"
                        },
                    },
                    {
                        "source": {
                            "description": "Basisregistratie Topografie (BRT) TOP10NL gebouwen laag, gedownload van de PDOK download API, gebruikt voor informatie over kassen en warenhuizen.",
                            "author": "Het Kadaster",
                            "website": "https://www.kadaster.nl/zakelijk/producten/geo-informatie/topnl",
                            "date": data_version_top10nl,
                            "dateType": "access",
                            "licence": "http://creativecommons.org/licenses/by/4.0/deed.nl"
                        },
                    },
                    {
                        "source": {
                            "description": "Actueel Hoogtebestaand Nederland (AHN) 3 puntenwolk (LAZ), gebruikt voor de hoogte-informatie voor de gebouwmodellen.",
                            "author": "Het Waterschapshuis",
                            "website": "https://www.ahn.nl",
                            "date": ["2014", "2019"],
                            "dateType": "creation",
                            "licence": "http://creativecommons.org/licenses/by/4.0/deed.nl"
                        },
                    },
                    {
                        "source": {
                            "description": "Actueel Hoogtebestaand Nederland (AHN) 4 puntenwolk (LAZ), gebruikt voor de hoogte-informatie voor de gebouwmodellen.",
                            "author": "Het Waterschapshuis",
                            "website": "https://www.ahn.nl",
                            "date": ["2020", "2022"],
                            "dateType": "creation",
                            "licence": "http://creativecommons.org/licenses/by/4.0/deed.nl"
                        },
                    },
                ],
                "software": [
                    {
                        "name": "geoflow-bundle",
                        "version": geoflow_version(),
                        "repository": "https://github.com/geoflow3d/geoflow-bundle",
                        "description": "3D building model reconstruction"
                    },
                    {
                        "name": "roofer",
                        "version": roofer_version(),
                        "repository": "https://github.com/3DGI/roofer",
                        "description": "Point cloud selection and cropping"
                    },
                    {
                        "name": "tyler",
                        "version": tyler_version(),
                        "repository": "https://github.com/3DGI/tyler",
                        "description": "Generating GeoPackage, OBJ and CityJSON tiles"
                    },
                    {
                        "name": "tyler-db",
                        "version": tyler_db_version(),
                        "repository": "https://github.com/3DGI/tyler/tree/postgres-footprints",
                        "description": "Input tiling"
                    },
                ]
            },
        }

    }

    output_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    outfile = output_dir.joinpath("metadata.json")
    with outfile.open("w") as fo:
        json.dump(metadata, fo)
    return Output(outfile, metadata=metadata)
