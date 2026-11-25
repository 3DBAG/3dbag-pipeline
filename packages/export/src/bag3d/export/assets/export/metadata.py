import csv
import json
from collections.abc import Iterable
from copy import deepcopy
from datetime import UTC, datetime
from importlib.metadata import version as _package_version
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
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
)
from psycopg.sql import SQL

logger = get_dagster_logger("export.metadata")


def read_export_edition(metadata_path: Path) -> str:
    """Read the release edition from the generated metadata file."""
    try:
        with metadata_path.open("r", encoding="utf-8") as stream:
            edition = json.load(stream)["identificationInfo"]["citation"]["edition"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError(
            f"Could not read identificationInfo.citation.edition from {metadata_path}. "
            "Check the file or materialize export/metadata again."
        ) from exc
    if not isinstance(edition, str) or not edition.strip():
        raise ValueError(
            f"Expected a non-empty release edition in {metadata_path}, got {edition!r}. "
            "Check export/metadata."
        )
    return edition


def read_indexed_tile_ids(index_path: Path) -> list[str]:
    with index_path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if "tile_id" not in (reader.fieldnames or ()):
            raise ValueError(
                f"Export index {index_path} is missing the tile_id column. "
                "Materialize export/export_index again."
            )
        tile_ids = []
        for row in reader:
            tile_id = row["tile_id"]
            if not tile_id:
                raise ValueError(
                    f"Missing tile_id in export index {index_path} at line {reader.line_num}. "
                    "Check export/export_index."
                )
            tile_ids.append(tile_id)
    return tile_ids


# (manifest_key, resource_key, executable, version_cmd)
_SOFTWARE_TOOLS = [
    ("roofer", "roofer", "roofer", None),
    ("tyler", "tyler", "tyler", None),
    ("tyler-db", "tyler", "tyler-db", None),
    ("gdal", "gdal", "ogr2ogr", None),
    ("pdal", "pdal", "pdal", None),
    ("lastools", "lastools", "lasindex", "-version"),
]


def _python_library_versions() -> list[dict]:
    """Versions of the Python libraries used by the IFC export.

    Unlike the tools in ``_SOFTWARE_TOOLS``, these are Python libraries without a
    command-line executable, so their versions are read from package metadata.
    """
    return [
        {
            "name": "IfcOpenShell",
            "version": _package_version("ifcopenshell"),
            "repository": "https://github.com/IfcOpenShell/IfcOpenShell",
            "description": "IFC file writing for the CityJSON to IFC conversion",
        },
        {
            "name": "cjio",
            "version": _package_version("cjio"),
            "repository": "https://github.com/cityjson/cjio",
            "description": "CityJSON parsing for the IFC conversion",
        },
    ]


def _build_software_list() -> list[dict]:
    """Build the software list from manifest metadata and runtime versions."""
    software = []
    for manifest_key, resource_key, executable, version_cmd in _SOFTWARE_TOOLS:
        meta = get_tool_metadata(manifest_key)
        kwargs = {}
        if version_cmd is not None:
            kwargs["version_cmd"] = version_cmd
        try:
            version = resource_defs[resource_key].runner.version(executable, **kwargs)
        except Exception as exc:
            raise RuntimeError(
                f"Could not read the {executable} version for export metadata. "
                f"Check the {resource_key} executable configuration."
            ) from exc
        if not version.strip():
            logger.warning(
                "%s returned an empty version for export metadata. "
                "Check the executable configured under %s.",
                executable,
                resource_key,
            )
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
    software.extend(_python_library_versions())
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
) -> Output[Path]:
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

    logger.info(
        "Evaluating reconstruction features from %s against reconstruction_input.reconstruction_input",
        reconstruction_index.dataset_dir,
    )
    idx = open_ready_index(reconstruction_index)
    try:
        for refs in iter_package_refs(idx, _PAGE_SIZE):
            for ref in refs:
                reconstructed_buildings.add(ref.model_id)
                try:
                    cityjson = read_package_feature_json(idx, ref)
                    codata = get_info_per_cityobject(
                        cityjson, deepcopy(cityobject_info), attributes_to_include
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Could not evaluate CityJSON model {ref.model_id} from "
                        f"the reconstruction index at {reconstruction_index.dataset_dir}."
                    ) from exc
                cityobjects.update(codata)
    finally:
        idx.close()

    res = conn.get_query(
        SQL("""
        SELECT identificatie
        FROM reconstruction_input.reconstruction_input;
        """)
    )
    input_buildings = {row[0] for row in res}
    not_reconstructed = input_buildings.difference(reconstructed_buildings)

    # Save not_reconstructed buildings to a text file
    not_reconstructed_file = output_dir.joinpath("not_reconstructed_buildings.txt")
    with not_reconstructed_file.open("w") as f:
        for building_id in sorted(not_reconstructed):
            f.write(f"{building_id}\n")

    for feature in not_reconstructed:
        cityobjects[feature] = cityobject_info

    features_to_csv(output_csv, cityobjects, cityobject_info, list(lods))

    return Output(
        output_csv,
        metadata={
            "artifact path": MetadataValue.path(output_csv),
            "input buildings": len(input_buildings),
            "reconstructed buildings": len(reconstructed_buildings),
            "unreconstructed buildings": len(not_reconstructed),
            "CSV rows": len(cityobjects),
            "unreconstructed building IDs": MetadataValue.path(not_reconstructed_file),
        },
    )


@asset(
    ins={"merged_quadtree": AssetIn(key=AssetKey(("export", "merged_quadtree")))},
)
def export_index(
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    merged_quadtree: Path,
) -> Output[Path]:
    """Index of the distribution tiles.

    Parses the quadtree.tsv file output by *tyler* and checks if all formats exist for
    a tile. If a tile does not have any features in the quadtree, it is not included.
    The index is written to export_index.csv.
    """
    path_export_dir = file_store.stage_subdir("export", version.version)
    path_export_index = path_export_dir.joinpath("export_index.csv")

    format_fields = ("has_cityjson", "has_gpkg", "has_ifc", "has_obj")
    indexed_tiles = 0
    missing_formats: dict[str, int] = {field: 0 for field in format_fields}
    with path_export_index.open("w") as fw:
        fieldnames = ["tile_id", *format_fields, "wkt"]
        csvwriter = csv.DictWriter(fw, fieldnames=fieldnames, extrasaction="ignore")
        csvwriter.writeheader()
        try:
            for result in check_export_results(merged_quadtree, path_export_dir):
                row = dict(result)
                csvwriter.writerow(row)
                indexed_tiles += 1
                for field in format_fields:
                    missing_formats[field] += not row[field]
        except (OSError, KeyError, ValueError) as exc:
            raise RuntimeError(
                f"Could not build {path_export_index} from quadtree {merged_quadtree} "
                f"and tile directory {path_export_dir / 't'}."
            ) from exc
    if not indexed_tiles:
        logger.warning(
            "No exported tiles from %s were indexed in %s. Check that Tyler "
            "wrote files for nonempty quadtree leaves under %s.",
            merged_quadtree,
            path_export_index,
            path_export_dir / "t",
        )
    if (
        missing_formats["has_cityjson"]
        or missing_formats["has_gpkg"]
        or missing_formats["has_obj"]
    ):
        logger.warning(
            "Incomplete Tyler exports in %s. Missing CityJSON on %d tiles, "
            "GeoPackage on %d, and complete OBJ outputs on %d. "
            "Use the has_* columns to identify the tile IDs and inspect the corresponding Tyler runs.",
            path_export_index,
            missing_formats["has_cityjson"],
            missing_formats["has_gpkg"],
            missing_formats["has_obj"],
        )
    return Output(
        path_export_index,
        metadata={
            "artifact path": MetadataValue.path(path_export_index),
            "quadtree": MetadataValue.path(merged_quadtree),
            "indexed tiles": indexed_tiles,
            "indexed tiles missing each format": MetadataValue.json(missing_formats),
        },
    )


ASSET_DEPENDENCIES_FOR_METADATA = [
    AssetKey(("bag", "extract_bag")),
    AssetKey(("bag", "bag_pandactueelbestaand_filtered")),
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
    missing_lineage = []
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
                continue
        missing_lineage.append(asset_key.to_user_string())

    if missing_lineage:
        logger.warning(
            "Release %s metadata has no lineage materializations for %s. "
            "Verify that those assets were materialized in this Dagster instance.",
            version_3dbag,
            ", ".join(missing_lineage),
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
    return Output(
        outfile,
        metadata={
            "artifact path": MetadataValue.path(outfile),
            "release": version_3dbag,
            **metadata,
        },
    )
