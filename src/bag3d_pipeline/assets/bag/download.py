from datetime import datetime
from typing import Tuple
from copy import deepcopy

from dagster import asset, Output, OpExecutionContext, Field, DataVersion
from lxml import objectify

from bag3d_pipeline.core import (bbox_from_wkt, download_file, unzip, drop_table,
                                 create_schema, postgrestable_metadata)
from bag3d_pipeline.custom_types import PostgresTableIdentifier, Path


# TODO: The LVBAG schemas are at
#  https://developer.kadaster.nl/schemas/lvbag-extract-v20200601.zip. We can extract
#  all the object and field descriptions from these schemas and integrate it into the
#  3D BAG (eg viewer).


@asset(
    required_resource_keys={"file_store"}
)
def extract_bag(context) -> Output[Tuple[Path, dict, str]]:
    """Download the latest LVBAG extract from PDOK.

    Extract URL: https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip

    The lvbag-extract-nl.zip is a nested zip, and it is uncompressed one level in the
    `extract_dir` directory, yielding the following tree:

    ```
    .
    ├── 9999InOnderzoek08102022.zip
    ├── 9999Inactief08102022.zip
    ├── 9999LIG08102022.zip
    ├── 9999NUM08102022.zip
    ├── 9999NietBag08102022.zip
    ├── 9999OPR08102022.zip
    ├── 9999PND08102022.zip
    ├── 9999STA08102022.zip
    ├── 9999VBO08102022.zip
    ├── 9999WPL08102022.zip
    ├── GEM-WPL-RELATIE-08102022.zip
    └── Leveringsdocument-BAG-Extract.xml
    ```
    """
    extract_url = "https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip"
    extract_zip = Path(context.resources.file_store.data_dir / "lvbag-extract-nl.zip")
    extract_dir = Path(context.resources.file_store.data_dir / "lvbag-extract")
    # chunk_size: https://stackoverflow.com/a/23397581
    download_file(extract_url, extract_zip, chunk_size=1024 * 1024)
    unzip(extract_zip, extract_dir)

    extract_dir_size = 0
    for child in extract_dir.iterdir():
        extract_dir_size += child.stat().st_size / 1e6

    metadata, shortdate = bagextract_metadata(context, extract_dir)
    meta_ext = deepcopy(metadata)
    meta_ext["Extract Size [Mb]"] = round(extract_dir_size, 2)
    return Output((extract_dir, metadata, shortdate), metadata=meta_ext,
                  data_version=DataVersion(metadata["Timeliness"]))


@asset(
    config_schema={
        "geofilter": Field(
            str,
            description="WKT of the polygonal extent. Will be converted to a BBOX.",
            is_required=False
        ),
    },
    required_resource_keys={"file_store", "db_connection", "gdal"})
def stage_bag_woonplaats(context, extract_bag) -> Output[PostgresTableIdentifier]:
    """Load the Woonplaats layer from the BAG extract."""
    extract_dir, metadata, shortdate = extract_bag
    new_schema = "stage_lvbag"
    layer = "woonplaats"
    metadata, new_table = stage_bag_layer(context=context, layer=layer,
                                          new_schema=new_schema, metadata=metadata,
                                          shortdate=shortdate,
                                          extract_dir=extract_dir)
    return Output(new_table, metadata=metadata)


@asset(
    config_schema={
        "geofilter": Field(
            str,
            description="WKT of the polygonal extent. Will be converted to a BBOX.",
            is_required=False
        ),
    },
    required_resource_keys={"file_store", "db_connection", "gdal"})
def stage_bag_verblijfsobject(context, extract_bag) -> Output[PostgresTableIdentifier]:
    """Load the Verblijfsobject layer from the BAG extract."""
    extract_dir, metadata, shortdate = extract_bag
    new_schema = "stage_lvbag"
    layer = "verblijfsobject"
    metadata, new_table = stage_bag_layer(context=context, layer=layer,
                                          new_schema=new_schema, metadata=metadata,
                                          shortdate=shortdate,
                                          extract_dir=extract_dir)
    return Output(new_table, metadata=metadata)


@asset(
    config_schema={
        "geofilter": Field(
            str,
            description="WKT of the polygonal extent. Will be converted to a BBOX.",
            is_required=False
        ),
    },
    required_resource_keys={"file_store", "db_connection", "gdal"})
def stage_bag_pand(context, extract_bag) -> Output[PostgresTableIdentifier]:
    """Load the Pand layer from the BAG extract."""
    extract_dir, metadata, shortdate = extract_bag
    new_schema = "stage_lvbag"
    layer = "pand"
    metadata, new_table = stage_bag_layer(context=context, layer=layer,
                                          new_schema=new_schema, metadata=metadata,
                                          shortdate=shortdate,
                                          extract_dir=extract_dir)
    return Output(new_table, metadata=metadata)


@asset(
    config_schema={
        "geofilter": Field(
            str,
            description="WKT of the polygonal extent. Will be converted to a BBOX.",
            is_required=False
        ),
    },
    required_resource_keys={"file_store", "db_connection", "gdal"})
def stage_bag_openbareruimte(context, extract_bag) -> Output[PostgresTableIdentifier]:
    """Load the Openbareruimte layer from the BAG extract."""
    extract_dir, metadata, shortdate = extract_bag
    new_schema = "stage_lvbag"
    layer = "openbareruimte"
    metadata, new_table = stage_bag_layer(context=context, layer=layer,
                                          new_schema=new_schema, metadata=metadata,
                                          shortdate=shortdate,
                                          extract_dir=extract_dir)
    return Output(new_table, metadata=metadata)


@asset(
    config_schema={
        "geofilter": Field(
            str,
            description="WKT of the polygonal extent. Will be converted to a BBOX.",
            is_required=False
        ),
    },
    required_resource_keys={"file_store", "db_connection", "gdal"})
def stage_bag_nummeraanduiding(context, extract_bag) -> Output[PostgresTableIdentifier]:
    """Load the Nummeraanduiding layer from the BAG extract."""
    extract_dir, metadata, shortdate = extract_bag
    new_schema = "stage_lvbag"
    layer = "nummeraanduiding"
    metadata, new_table = stage_bag_layer(context=context, layer=layer,
                                          new_schema=new_schema, metadata=metadata,
                                          shortdate=shortdate,
                                          extract_dir=extract_dir)
    return Output(new_table, metadata=metadata)


def stage_bag_layer(context: OpExecutionContext, layer: str, new_schema: str,
                    metadata: dict, shortdate: str, extract_dir: Path):
    create_schema(context, context.resources.db_connection, new_schema)
    new_table = PostgresTableIdentifier(new_schema, layer)
    drop_table(context, context.resources.db_connection, new_table)
    success = load_bag_layer(context=context, extract_dir=extract_dir,
                             layer=layer, new_table=new_table,
                             shortdate=shortdate)
    _m = postgrestable_metadata(context, new_table)
    metadata.update(_m)
    return metadata, new_table


def load_bag_layer(context, extract_dir: Path, layer: str, shortdate: str,
                   new_table: PostgresTableIdentifier) -> bool:
    layername_map = {
        'inactief': 'inactief',
        'inonderzoek': 'inonderzoek',
        'ligplaats': 'lig',
        'nietbag': 'nietbag',
        'nummeraanduiding': 'num',
        'openbareruimte': 'opr',
        'pand': 'pnd',
        'standplaats': 'sta',
        'verblijfsobject': 'vbo',
        'woonplaats': 'wpl'
    }
    layer_dir = layername_map[layer.lower()].upper()
    kwargs = {"layer_dir": layer_dir, "shortdate": shortdate, "new_table": new_table,
              "dsn": context.resources.db_connection.dsn}

    # Create the ogr2ogr command. The order of parameters is important!
    cmd = [
        "{exe}",
        "--config PG_USE_COPY=YES",
        "-overwrite",
        "-nln {new_table}",
        "-lco UNLOGGED=ON",
        "-lco SPATIAL_INDEX=NONE",
    ]
    geofilter = context.op_config.get("geofilter")
    if geofilter:
        bbox = bbox_from_wkt(geofilter)
        cmd.append("-spat {bbox}")
        kwargs["bbox"] = " ".join(map(str, bbox))
    cmd.append('-f PostgreSQL PG:"{dsn}"')
    cmd.append("/vsizip/{local_path}/9999{layer_dir}{shortdate}.zip")
    cmd = " ".join(cmd)

    # Execute
    return_code, output = context.resources.gdal.execute(
        "ogr2ogr", cmd, kwargs=kwargs, local_path=extract_dir)
    return True if return_code == 0 else False


def bagextract_metadata(context: OpExecutionContext,
                        extract_dir: Path) -> Tuple[dict, str]:
    """Determine what type of LVBAG extract do we have, Gemeente or Nederland.

    LVBAG schema version: 20200601

    Args:
        extract_dir: Path to the BAG Extract Leveringsdocument xml file directory.

    Returns:
        A dict with the reported date of the extract ('StandTechnischeDatum') and the
        extract area ('Gebied'). The latter is either 'GEM' for Gemeente or 'NLD' for
        Nederland.
    """
    # This is a standard doc containing main info on what the extract contains
    implemented_schema_version = '20200601'

    extract_metadata = "Leveringsdocument-BAG-Extract.xml"
    extract_metadata_out = Path(extract_dir / extract_metadata)

    metadata = {}

    with extract_metadata_out.open('r') as fo:
        lvdoc = objectify.parse(fo)

    nsmap = lvdoc.getroot().nsmap

    versie = str(lvdoc.getroot().SchemaInfo.versie)
    if versie != implemented_schema_version:
        context.log.error(f"The version of the schema of the extract is different than "
                          f"what is implemented. Implemented: "
                          f"{implemented_schema_version}. Extract: {versie}.")

    LVC_Extract = lvdoc.getroot().SelectieGegevens.find(
        f"{{{nsmap['selecties-extract']}}}LVC-Extract")
    if LVC_Extract is None:
        context.log.critical(f"The LVBAG extract is not of the type 'LVC-Extract' "
                             f"(Levenscyclus en LevenscyclusVanaf).")
        raise Exception
    Gebied_Registratif = lvdoc.getroot().SelectieGegevens.find(
        f"{{{nsmap['selecties-extract']}}}Gebied-Registratief")

    metadata['StandTechnischeDatum'] = datetime.strptime(
        str(LVC_Extract.StandTechnischeDatum), "%Y-%m-%d")

    for g in Gebied_Registratif.getchildren():
        if g.tag == f"{{{nsmap['selecties-extract']}}}Gebied-GEM":
            metadata['Gebied'] = 'GEM'
            gem_id = str(g.GemeenteCollectie.Gemeente.GemeenteIdentificatie)
            # Need to 0-pad the ID, because lxml converts them to int-s...
            metadata['GemeenteIdentificatie'] = f"0{gem_id}" if len(
                gem_id) == 3 else gem_id
        elif g.tag == f"{{{nsmap['selecties-extract']}}}Gebied-NLD":
            metadata['Gebied'] = 'NLD'
        else:
            context.log.error(f"Unrecognized tag: {g.tag}")

    shortdate = metadata['StandTechnischeDatum'].strftime("%d%m%Y")
    metadata["Timeliness"] = metadata['StandTechnischeDatum'].date().isoformat()
    del metadata['StandTechnischeDatum']

    return metadata, shortdate
