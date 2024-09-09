import json
import re
from pathlib import Path
from typing import List, Tuple

from dagster import (
    OpExecutionContext,
    TableSchemaMetadataValue,
    TableSchema,
    TableColumnConstraints,
    TableColumn,
)
from pgutils import PostgresTableIdentifier

from bag3d.common.utils.database import postgrestable_metadata


def wkt_from_bbox(bbox):
    minx, miny, maxx, maxy = bbox
    return (
        f"POLYGON (({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, "
        f"{minx} {miny}))"
    )


def bbox_from_wkt(wkt):
    """Returns the BBOX of a WKT Polygon."""
    re_polygon = re.compile(r"(?<=polygon).*", re.IGNORECASE)
    m = re_polygon.search(wkt)
    if m:
        ext = m.group().strip().strip("(").strip(")").split(")")[0]
        cstr = ext.split(",")
        fcoord = tuple(map(float, cstr[0].strip(" ").split(" ")))
        minx, miny = fcoord
        maxx, maxy = fcoord
        for coord in cstr:
            x, y = tuple(map(float, coord.strip(" ").split(" ")))
            minx = x if x < minx else minx
            miny = y if y < miny else miny
            maxx = x if x > maxx else maxx
            maxy = y if y > maxy else maxy
        return minx, miny, maxx, maxy
    else:
        return None


def ogrinfo(
    context: OpExecutionContext,
    dataset: str,
    extract_path: Path,
    feature_types: list,
    xsd: str,
):
    """Runs ogrinfo on the zipped extract."""
    gdal = context.resources.gdal
    cmd = " ".join(
        [
            "{exe}",
            "-so",
            "-al",
            '-oo XSD="{xsd}"',
            "-oo WRITE_GFS=NO",
            "/vsizip/{local_path}/{dataset}_{feature_type}.gml {feature_type}",
        ]
    )

    info = {}
    for feature_type in feature_types:
        kwargs = {"xsd": xsd, "dataset": dataset, "feature_type": feature_type}
        return_code, output = gdal.execute(
            "ogrinfo", command=cmd, kwargs=kwargs, local_path=extract_path
        )
        if return_code == 0:
            layername, layerinfo = parse_ogrinfo(output, feature_type)
            info[str(layername)] = layerinfo
    return info


def parse_ogrinfo(ogrinfo_stdout: str, feature_type: str) -> (str, dict):
    """Parses the stdout of ogrinfo into a dictionary."""
    layerinfo = {}
    inf = ogrinfo_stdout.split("Layer name: ")[1]
    layername = inf.split("\n")[0].lower()
    if layername != feature_type:
        raise ValueError(
            f"Encountered a {layername} layer, " f"but expected {feature_type}"
        )

    re_feature_count = re.compile(r"(?<=Feature Count: )\d+")
    ft = feature_type.lower()
    layerinfo[f"Feature Count [{ft}]"] = int(re_feature_count.search(inf)[0])
    layerinfo[f"Extent [{ft}]"] = dict(
        (geom, wkt) for geom, wkt in parse_ogrinfo_extent(inf)
    )
    schema = parse_ogrinfo_attributes(inf[inf.find("gml_id") :])
    layerinfo[f"Schema [{ft}]"] = TableSchemaMetadataValue(schema)
    return layername, layerinfo


def parse_ogrinfo_attributes(attributes_str: str) -> TableSchema:
    """Parses the attributes list of the ogrinfo stdout into a :py:class:`TableSchema`"""
    alist = attributes_dict(attributes_str)
    schema = attributes_schema(alist)
    return schema


def attributes_dict(attributes_str: str) -> List[dict]:
    """
    [
        {
        "name": attribute name,
        "type": value type,
        "constraints" value constraints
        },
    ...
    ]
    """
    ret = []
    _astr = attributes_str.strip("\n").strip().split("\n")
    for i in _astr:
        adict = {}
        aname, specs = i.split(":")
        try:
            # 'String (0.0) NOT NULL'
            atype, contstraints = specs.strip().split(")")
        except ValueError:
            # 'inOnderzoek: Integer(Boolean) (0.0)'
            atype, contstraints = specs.strip(), ""
        adict["name"] = aname
        adict["type"] = atype + ")"
        if contstraints == "":
            adict["constraints"] = None
        else:
            adict["constraints"] = TableColumnConstraints(
                other=[
                    contstraints.strip(),
                ]
            )
        ret.append(adict)
    return ret


def attributes_schema(attributes_list: List[dict]) -> TableSchema:
    cols = []
    for a in attributes_list:
        cols.append(
            TableColumn(name=a["name"], type=a["type"], constraints=a["constraints"])
        )
    return TableSchema(columns=cols)


def parse_ogrinfo_extent(info):
    re_extent = re.compile(r"Extent")
    for line in info.split("\n"):
        extent = re_extent.match(line)
        if extent:
            meta, box = line.split(": ")
            geom = meta[extent.span()[1] :].strip().replace("(", "").replace(")", "")
            bbox = [coord for m in box.split(" - ") for coord in m[1:-1].split(", ")]
            geom = "None" if geom == "" else geom
            yield geom, wkt_from_bbox(bbox)


def add_info(metadata: dict, info: dict) -> None:
    """Add the :py:func:`ogrinfo` output to the metadata returned by
    :py:func:`download_extract`.

    Args:
        metadata: Metadata returned by :py:func:`download_extract`.
        info: :py:func:`ogrinfo` output.
    """
    for layername, layerinfo in info.items():
        layer = layername.lower()
        metadata.update(layerinfo)
        for dt, ft in metadata["timeliness"].items():
            if layer in ft:
                metadata[f"Timeliness [{layer}]"] = dt
    del metadata["timeliness"]


def ogr2postgres(
    context: OpExecutionContext,
    dataset: str,
    extract_path: Path,
    feature_type: str,
    xsd: str,
    new_table: PostgresTableIdentifier,
) -> dict:
    """ogr2ogr a layer from zipped data extract from GML into Postgres.

    It was developed for loading the TOP10NL and BGT extracts that are downloaded from
    the PDOK API.

    Args:
        context: Op execution context from Dagster.
        dataset: Name of the dataset ('top10nl', 'bgt').
        extract_path: Local path to the zipped extract.
        feature_type: The feature layer to load from the ``dataset``
        xsd: Path (URL) to the XSD file.
        new_table: The name of the new Postgres table to load the data into.
    Returns:
        Runs :py:func:`postgrestable_metadata` on return and returns a dict of metadata
        of the ``new_table`` loaded with data.
    """
    gdal = context.resources.gdal
    dsn = context.resources.db_connection.dsn

    cmd = " ".join(
        [
            "{exe}",
            "--config PG_USE_COPY=YES",
            "-overwrite",
            "-nln {new_table}",
            '-oo XSD="{xsd}"',
            "-oo WRITE_GFS=NO",
            "-lco UNLOGGED=ON",
            "-lco SPATIAL_INDEX=NONE",
            '-f PostgreSQL PG:"{dsn}"',
            "/vsizip/{local_path}/{dataset}_{feature_type}.gml {feature_type}",
        ]
    )

    kwargs = {
        "new_table": new_table,
        "feature_type": feature_type,
        "dsn": dsn,
        "xsd": xsd,
        "dataset": dataset,
    }
    return_code, output = gdal.execute(
        "ogr2ogr", command=cmd, kwargs=kwargs, local_path=extract_path
    )
    if return_code == 0:
        return postgrestable_metadata(context, new_table)


def pdal_info(pdal, file_path: Path, with_all: bool = False) -> Tuple[int, dict]:
    """Run 'pdal info' on a point cloud file.

    Args:
        pdal (AppImage): The pdal AppImage executable.
        file_path: Path to the point cloud file.
        with_all: If true, run ``pdal info --all``, else run ``pdal info --metadata``.
            Defaults to ``False``.
    Returns:
        A tuple of (pdal's return code, parsed pdal info output)
    """
    cmd_list = [
        "{exe}",
        "info",
    ]
    cmd_list.append("--all") if with_all else cmd_list.append("--metadata")
    cmd_list.append("{local_path}")
    return_code, output = pdal.execute(
        "pdal", command=" ".join(cmd_list), local_path=file_path
    )

    return return_code, json.loads(output)


def geojson_poly_to_wkt(geometry) -> str:
    if geometry["type"].lower() != "polygon":
        raise ValueError(f"Must be a Polygon, but it is a {geometry['type']}.")
    outer_ring = geometry["coordinates"][0]
    outer_str = "(" + ",".join([f"{pt[0]} {pt[1]}" for pt in outer_ring]) + ")"
    if len(geometry["coordinates"]) > 0:
        inner_rings = geometry["coordinates"][1:]
    else:
        inner_rings = []
    inner_strings = []
    for inner_ring in inner_rings:
        inner_strings.append(
            "(" + ",".join([f"{pt[0]} {pt[1]}" for pt in inner_ring]) + ")"
        )
    inner_str = ",".join(inner_strings)
    if len(inner_strings) > 0:
        return f"POLYGON({outer_str},{inner_str})"
    else:
        return f"POLYGON({outer_str})"
