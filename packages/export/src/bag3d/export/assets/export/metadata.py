import csv
import json
from collections.abc import Iterable
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid1

from bag3d.common.resources import resource_defs
from bag3d.common.resources.cjindex import (
    CityIndexResource,
    iter_package_refs,
    open_ready_index,
    read_package_feature_json,
)
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.common.utils.dagster import format_date
from bag3d.common.utils.files import check_export_results
from bag3d.common.utils.manifest import get_tool_metadata
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetRecordsFilter,
    Output,
    asset,
    get_dagster_logger,
)
from psycopg.sql import SQL

logger = get_dagster_logger("export.metadata")

# (manifest_key, resource_key, executable, version_cmd)
_SOFTWARE_TOOLS = [
    ("roofer", "roofer", "roofer", None),
    ("tyler", "tyler", "tyler", None),
    ("tyler-db", "tyler", "tyler-db", None),
    ("gdal", "gdal", "ogr2ogr", None),
    ("pdal", "pdal", "pdal", None),
    ("lastools", "lastools", "lasindex", "-version"),
]


def _build_software_list() -> list[dict]:
    """Build the software list from manifest metadata and runtime versions."""
    software = []
    for manifest_key, resource_key, executable, version_cmd in _SOFTWARE_TOOLS:
        meta = get_tool_metadata(manifest_key)
        kwargs = {}
        if version_cmd is not None:
            kwargs["version_cmd"] = version_cmd
        version = resource_defs[resource_key].runner.version(executable, **kwargs)
        if manifest_key == "pdal":
            version = version.replace("-", "").replace(",", "")
        software.append(
            {
                "name": meta.get("display_name", manifest_key),
                "version": version,
                "repository": meta["repository"],
                "description": meta["description"],
            }
        )
    return software


def get_info_per_cityobject(
    cityjson: dict, cityobject_info: dict, attribute_names: Iterable
) -> dict[str, dict]:
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
    features: dict[str, dict[str, int]],
    cityobject_info: dict,
    lods: list,
) -> None:
    """Creates a csv with the city object id and the city object information"""
    fieldnames = ["id", "identificatie", "lod_0", "lod_12", "lod_13", "lod_22"]
    fieldnames += [k for k in cityobject_info if k not in lods]
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


_PAGE_SIZE = 1000


@asset(
    deps={AssetKey(("reconstruction", "reconstructed_building_models"))},
)
def feature_evaluation(
    file_store: FileStoreResource,
    computation_db: DatabaseResource,
    version: ReleaseVersionResource,
    reconstruction_index: CityIndexResource,
) -> Path:
    """Compare the reconstruction output to the input, for each feature.
    Check if all LoD-s are generated for the feature and include some attributes from
    the CityObjects"""
    output_dir = file_store.stage_subdir("export", version.version)
    output_csv = output_dir.joinpath("reconstructed_features.csv")
    conn = computation_db.connection

    lods = ("0", "1.2", "1.3", "2.2")
    attributes_to_include = (
        "b3_pw_selectie_reden",
        "b3_pw_bron",
        "b3_puntdichtheid_ahn3",
        "b3_puntdichtheid_ahn4",
        "b3_puntdichtheid_ahn5",
        "b3_mutatie_ahn3_ahn4",
        "b3_mutatie_ahn4_ahn5",
        "b3_nodata_fractie_ahn3",
        "b3_nodata_fractie_ahn4",
        "b3_nodata_fractie_ahn5",
        "b3_nodata_radius_ahn3",
        "b3_nodata_radius_ahn4",
        "b3_nodata_radius_ahn5",
    )
    cityobject_info = {lod: 0 for lod in lods}
    cityobject_info["has_geometry"] = False
    cityobject_info.update({a: None for a in attributes_to_include})  # type: ignore[arg-type]

    reconstructed_buildings = set()
    cityobjects = {}

    idx = open_ready_index(reconstruction_index)
    for refs in iter_package_refs(idx, _PAGE_SIZE):
        for ref in refs:
            reconstructed_buildings.add(ref.model_id)
            cityjson = read_package_feature_json(idx, ref)
            codata = get_info_per_cityobject(
                cityjson, deepcopy(cityobject_info), attributes_to_include
            )
            cityobjects.update(codata)
    idx.close()

    logger.debug(f"len(reconstructed_buildings)={len(reconstructed_buildings)}")
    logger.debug(f"len(cityobjects)={len(cityobjects)}")

    res = conn.get_query(
        SQL("""
        SELECT identificatie
        FROM reconstruction_input.reconstruction_input;
        """)
    )
    input_buildings = {row[0] for row in res}
    logger.debug(f"len(input_buildings)={len(input_buildings)}")

    not_reconstructed = input_buildings.difference(reconstructed_buildings)
    logger.debug(f"len(not_reconstructed)={len(not_reconstructed)}")

    # Save not_reconstructed buildings to a text file
    not_reconstructed_file = output_dir.joinpath("not_reconstructed_buildings.txt")
    with not_reconstructed_file.open("w") as f:
        for building_id in sorted(not_reconstructed):
            f.write(f"{building_id}\n")
    logger.info(
        f"Saved {len(not_reconstructed)} not reconstructed building IDs to {not_reconstructed_file}"
    )

    for feature in not_reconstructed:
        cityobjects[feature] = cityobject_info

    features_to_csv(output_csv, cityobjects, cityobject_info, list(lods))

    return output_csv


@asset(
    ins={"merged_quadtree": AssetIn(key=AssetKey(("export", "merged_quadtree")))},
)
def export_index(
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    merged_quadtree: Path,
) -> Path:
    """Index of the distribution tiles.

    Parses the quadtree.tsv file output by *tyler* and checks if all formats exist for
    a tile. If a tile does not have any features in the quadtree, it is not included.
    Output it written to export_index.csv.
    """
    path_export_dir = file_store.stage_subdir("export", version.version)
    path_export_index = path_export_dir.joinpath("export_index.csv")

    with path_export_index.open("w") as fw:
        fieldnames = ["tile_id", "has_cityjson", "has_gpkg", "has_obj", "wkt"]
        csvwriter = csv.DictWriter(fw, fieldnames=fieldnames, extrasaction="ignore")
        csvwriter.writeheader()
        export_results_gen = check_export_results(merged_quadtree, path_export_dir)
        csvwriter.writerows(dict(export_result) for export_result in export_results_gen)
    return path_export_index


ASSET_DEPENDENCIES_FOR_METADATA = [
    AssetKey(("bag", "extract_bag")),
    AssetKey(("bag", "bag_pandactueelbestaand")),
    AssetKey(("top10nl", "extract_top10nl")),
    AssetKey(("top10nl", "top10nl_gebouw")),
    AssetKey(("input", "reconstruction_input")),
]


@asset(
    deps=ASSET_DEPENDENCIES_FOR_METADATA,
)
def metadata(
    context: AssetExecutionContext,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
) -> Output[Path]:
    """3DBAG metadata for distribution.
    Metadata schema follows the Dutch metadata profile for geographical data,
    https://geonovum.github.io/Metadata-ISO19115/.

    For extended ISO lineage, see 19115-2, https://wiki.esipfed.org/ISO_Lineage. This
    has XML examples. And also https://wiki.esipfed.org/Data_Understanding_-_Provenance_(ISO-19115-1).
    """
    date_3dbag = format_date(datetime.now(tz=UTC).date(), version=False)
    version_3dbag = f"v{format_date(datetime.now(tz=UTC).date(), version=True)}"
    uuid_3dbag = str(uuid1())

    asset_keys = ASSET_DEPENDENCIES_FOR_METADATA
    instance = context.instance

    # Get only the last asset materialization, because if an asset is materialized then
    # is has succeeded.
    process_step_list = []
    for asset_key in asset_keys:
        event_record_list = instance.fetch_materializations(
            records_filter=AssetRecordsFilter(
                asset_key=asset_key,
            ),
            limit=1,
        ).records
        if len(event_record_list) > 0:
            event_record = event_record_list[0]
            materialization = event_record.asset_materialization
            if materialization is not None:
                # Just because the extract_top10nl asset has a 'Feature Count [gebouw]' metadata
                # member instead of 'Rows'
                rows = materialization.metadata.get("Rows")
                tags = materialization.tags or {}
                process_step_list.append(
                    {
                        "name": ".".join(asset_key.path),
                        "runId": event_record.run_id,
                        "featureCount": rows.value if rows is not None else None,
                        "dateTime": datetime.fromtimestamp(
                            event_record.event_log_entry.timestamp, tz=UTC
                        )
                        .date()
                        .isoformat(),
                        "dataVersion": tags.get("dagster/data_version"),
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
                "title": "3DBAG",
                "date": date_3dbag,
                "dateType": "creation",
                "edition": version_3dbag,
                "identifier": uuid_3dbag,
            },
            "abstract": "De 3DBAG is een up-to-date landsdekkende dataset met 3D gebouwmodellen van Nederland. De 3DBAG is open data. Het bevat 3D modellen op verscheidene detailniveaus welke zijn gegenereerd door de combinatie van twee open datasets: de pand-gegevens uit de BAG en de hoogtegegevens uit de AHN. De 3DBAG wordt regelmatig geüpdatet met de meest recente openlijk beschikbare pand- en hoogtegegevens.",
            "pointOfContact": {
                "organisationName": "3DBAG",
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
                            "text": "Naamensvermelding verplicht, 3DBAG door de 3D geoinformation onderzoeksgroep (TU Delft) en 3DGI",
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
                "software": _build_software_list(),
            },
        },
    }
    output_dir = file_store.stage_subdir("export", version.version)
    outfile = output_dir.joinpath("metadata.json")
    with outfile.open("w") as fo:
        json.dump(metadata, fo)
    return Output(outfile, metadata=metadata)
