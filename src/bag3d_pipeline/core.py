import inspect
from datetime import datetime, date
from pathlib import Path
import re
from time import sleep
from typing import List, Mapping, Union, Tuple
from urllib.parse import urlparse, urljoin
from zipfile import ZipFile
import json
from importlib import resources

import docker
from dagster import (TableColumn, TableSchema, get_dagster_logger, OpExecutionContext,
                     MarkdownMetadataValue, RetryRequested, PathMetadataValue,
                     UrlMetadataValue, FloatMetadataValue,
                     TableSchemaMetadataValue, TableColumnConstraints, op, Field,
                     AssetKey)
from dagster._core.definitions.data_version import (
    extract_data_version_from_entry,
)
from pgutils import inject_parameters, PostgresTableIdentifier
from psycopg.sql import Composed, SQL, Identifier
import requests

from bag3d_pipeline import sql as sqlfiles


class BadArchiveError(OSError):
    pass


def get_run_id(context, short=True):
    """Return the Run ID from the execution context.

    Args:
        context: A dagster context object.
        short (bool): Return only the first 8 characters of the ID or the complete ID.
    """
    if context is None:
        return None
    if context.dagster_run is None:
        return None
    if short:
        return context.dagster_run.run_id.split("-")[0]
    else:
        return context.dagster_run.run_id.split("-")


def wkt_from_bbox(bbox):
    minx, miny, maxx, maxy = bbox
    return f"POLYGON (({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, " \
           f"{minx} {miny}))"


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


def load_sql(filename: str = None, query_params: dict = None):
    """Load SQL from a file and inject parameters if provided.

    If providing query parametes, they need to be in a dict, where the keys are the
    parameter names.

    The SQL script can contain parameters in the form of ``${...}``, which is
    understood by most database managers. This is handy for developing the SQL scripts
    in a database manager, without having to execute the pipeline.
    However, the python formatting only understands ``{...}`` placeholders, so the
    ``$`` are removed from ``${...}`` when the SQL is loaded from the file.

    Args:
        filename (str): SQL File to load (without the path) from the ``sql``
            sub-package. If None, it will load the ``.sql`` file with the name equal to
            the caller function's name.
        query_params (dict): If provided, the templated SQL is formatted with the
            parameters.

    For example:

    .. code-block:: python

        def my_func():
            load_sql() # loads my_func.sql from bag3d_pipeline.sql

    """
    _f = filename if filename is not None else f"{inspect.stack()[1].function}.sql"
    _sql = resources.read_text(sqlfiles, _f)
    _pysql = _sql.replace("${", "{")
    return inject_parameters(_pysql, query_params)


def cast_to_dagsterschema(fields: list):
    columns = [TableColumn(name=colname, type=coltype) for colname, coltype in fields]
    return TableSchema(columns=columns)


def summary_md(fields, null_count):
    logger = get_dagster_logger()
    if len(fields) != len(null_count):
        logger.error("fields and null_count are different length")
        return None
    metacols = ["column", "type", "NULLs"]
    header = " ".join(["|", " | ".join(metacols), "|"])
    header_separator = " ".join(["|", " | ".join("---" for _ in metacols), "|"])
    mdtbl = "\n".join([header, header_separator]) + "\n"
    _missing_vals = {rec["column_name"]: rec["missing_values"] for rec in null_count}
    for colname, coltype in fields:
        metarow = "| "
        metarow += f"**{colname}**" + " | "
        metarow += f"*{coltype}*" + " | "
        metarow += str(_missing_vals[colname]) + " |" + "\n"
        mdtbl += metarow
    # remove the last \n
    return mdtbl[:-1]


def postgrestable_from_query(context: OpExecutionContext, query: Composed,
                             table: PostgresTableIdentifier) -> dict:
    conn = context.resources.db_connection
    # log the query
    context.log.info(conn.print_query(query))
    # execute the query
    conn.send_query(query)
    return postgrestable_metadata(context, table)


def postgrestable_metadata(context: OpExecutionContext,
                           table: PostgresTableIdentifier) -> dict:
    conn = context.resources.db_connection
    # row count
    row_count = conn.get_count(table)
    # schema
    fields = conn.get_fields(table)
    # null count
    null_count = conn.count_nulls(table)
    # head
    head = conn.get_head(table, md=True, shorten=23)
    return {
        "Database.Schema.Table": f"{conn.dbname}.{table}",
        "Rows": row_count,
        "Head": MarkdownMetadataValue(head),
        "Summary": MarkdownMetadataValue(summary_md(fields, null_count))
    }


def download_as_str(url: str, parameters: Mapping = None) -> str:
    """Download a file as string in memory.

    Returns:
         The downloaded package as string.
    """
    resp = requests.get(url=url, params=parameters)
    if resp.status_code == 200:
        return resp.text
    else:
        raise ValueError(
            f"Failed to download JSON. HTTP Status {resp.status_code} "
            f"for {resp.url}"
        )


def download_file(url: str, target_path: Path, chunk_size: int = 1024,
                  parameters: Mapping = None) -> Union[Path, None]:
    """Download a large file and save it to disk.

    Args:
        url (str): The URL of the file to be downloaded.
        target_path (Path): Path to the target file or directory. If ``save_path`` is a
            directory, then the target file name is the last part of the ``url``.
        chunk_size (int): The ``chunk_size`` parameter passed to
            :py:func:`request.Response.iter_content. Defaults to ``1024``.
        parameters (dict): Query parameters passed to :py:func:`requests.get`.

    Returns:
        The local Path to the downloaded file, or None on failure
    """
    logger = get_dagster_logger()
    if target_path.is_dir():
        local_filename = url.split("/")[-1]
        fpath = target_path / local_filename
    else:
        fpath = target_path
    logger.info(f"Downloading from {url} to {fpath}")
    session = requests.Session()  # https://stackoverflow.com/a/63417213
    r = session.get(url, params=parameters, stream=True)
    if r.ok:
        try:
            with fpath.open('wb') as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
            return fpath
        except requests.exceptions.BaseHTTPError as e:
            logger.exception(e)
            return None
        finally:
            r.close()
    else:
        r.raise_for_status()


def unzip(file: Path, dest: Path):
    """Uncompress the whole zip archive and delete the zip.

    Args:
        file: The Path to the zip.
        dest: The Path to the destination directory.

    Returns:
        None
    """
    logger = get_dagster_logger()
    logger.info(f"Uncompressing {file} to {dest}")
    with ZipFile(file, 'r') as ezip:
        first_bad_file = ezip.testzip()
        if first_bad_file:
            raise BadArchiveError(
                f"The archive contains at least one bad file: {first_bad_file}")
        ezip.extractall(path=dest)
    logger.info(f"Deleting {file}")
    file.unlink()


def get_metadata(url_api: str):
    """Get metadata from a PDOK API.

    :returns: {"timeliness": <date>: [featuretype,...]}
    """
    r_meta = requests.get(url_api)
    if not r_meta.status_code == requests.codes.ok:
        r_meta.raise_for_status()
    meta = {"timeliness": {}}
    for layer in r_meta.json()["timeliness"]:
        if layer["datetimeTo"][-1] == "Z":
            dt = str(datetime.fromisoformat(layer["datetimeTo"][:-1]).date())
        else:
            dt = str(datetime.fromisoformat(layer["datetimeTo"]).date())
        if dt in meta["timeliness"]:
            meta["timeliness"][dt].append(layer["featuretype"])
        else:
            meta["timeliness"][dt] = [layer["featuretype"], ]
    return meta


def get_extract_download_link(url, featuretypes, data_format, geofilter) -> str:
    """Request an export and download link from the API."""
    logger = get_dagster_logger()
    request_json = {
        "featuretypes": featuretypes, "format": data_format,
    }
    if geofilter is not None:
        request_json["geofilter"] = geofilter
    r_post = requests.post(url, json=request_json)
    logger.info(f"Requesting extract: {r_post.url} with {request_json} ")
    if not r_post.status_code == requests.codes.accepted:
        logger.error(r_post.text)
        r_post.raise_for_status()
    else:
        _u = urlparse(url)
        pdok_server = f"{_u.scheme}://{_u.hostname}/"
        url_status = urljoin(pdok_server, r_post.json()['_links']['status']['href'])
        url_download = None

        if requests.get(url_status).status_code == requests.codes.ok:
            while (
                    r_status := requests.get(
                        url_status)).status_code == requests.codes.ok:
                sleep(15)
            if not r_status.status_code == requests.codes.created:
                logger.error(r_status.text)
                r_status.raise_for_status()
            url_download = urljoin(pdok_server,
                                   r_status.json()['_links']['download']['href'])
        elif requests.get(url_status).status_code == requests.codes.created:
            r_status = requests.get(url_status)
            url_download = urljoin(pdok_server,
                                   r_status.json()['_links']['download']['href'])
        else:
            _r = requests.get(url_status)
            logger.error(_r.text)
            _r.raise_for_status()
        logger.info(f"Extract URL: {url_download}")
        return url_download


def download_extract(dataset, url_api, featuretypes, data_format, geofilter,
                     download_dir):
    _m = get_metadata(f"{url_api}/dataset")
    metadata = {"timeliness": {}}
    for dt, ft in _m["timeliness"].items():
        metadata["timeliness"][dt] = list(set(featuretypes).intersection(ft))

    # TODO: materialize if there is a newer version only https://docs.dagster.io/concepts/assets/software-defined-assets#conditional-materialization
    url_download = get_extract_download_link(
        url=f"{url_api}/full/custom",
        featuretypes=featuretypes,
        data_format=data_format,
        geofilter=geofilter
    )

    dest_file = Path(download_dir) / f"{dataset}.zip"
    try:
        download_file(url=url_download, target_path=dest_file)
    except requests.HTTPError:
        raise RetryRequested(max_retries=2)

    return {
        "Extract Path": PathMetadataValue(dest_file),
        "Download URL": UrlMetadataValue(url_download),
        "Size [Mb]": FloatMetadataValue(
            round(dest_file.stat().st_size * 0.000001, ndigits=2)),
        "timeliness": metadata["timeliness"],
    }


def ogrinfo(context: OpExecutionContext, dataset: str, extract_path: Path,
            feature_types: list, xsd: str):
    """Runs ogrinfo on the zipped extract."""
    gdal = context.resources.gdal
    cmd = " ".join([
        "{exe}",
        "-so",
        "-al",
        '-oo XSD="{xsd}"',
        "-oo WRITE_GFS=NO",
        "/vsizip/{local_path}/{dataset}_{feature_type}.gml {feature_type}"
    ])

    info = {}
    for feature_type in feature_types:
        kwargs = {
            "xsd": xsd,
            "dataset": dataset,
            "feature_type": feature_type
        }
        return_code, output = gdal.execute("ogrinfo", command=cmd, kwargs=kwargs,
                                           local_path=extract_path)
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
        raise ValueError(f"Encountered a {layername} layer, "
                         f"but expected {feature_type}")

    re_feature_count = re.compile(r"(?<=Feature Count: )\d+")
    ft = feature_type.lower()
    layerinfo[f"Feature Count [{ft}]"] = int(re_feature_count.search(inf)[0])
    layerinfo[f"Extent [{ft}]"] = dict(
        (geom, wkt) for geom, wkt in parse_ogrinfo_extent(inf))
    schema = parse_ogrinfo_attributes(inf[inf.find("gml_id"):])
    layerinfo[f"Schema [{ft}]"] = TableSchemaMetadataValue(schema)
    return layername, layerinfo


def parse_ogrinfo_attributes(attributes_str: str) -> TableSchema:
    """Parses the attributes list of the ogrinfo stdout into a :py:class:`TableSchema`
    """
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
                other=[contstraints.strip(), ])
        ret.append(adict)
    return ret


def attributes_schema(attributes_list: List[dict]) -> TableSchema:
    cols = []
    for a in attributes_list:
        cols.append(TableColumn(name=a["name"], type=a["type"],
                                constraints=a["constraints"]))
    return TableSchema(columns=cols)


def parse_ogrinfo_extent(info):
    re_extent = re.compile(r"Extent")
    for line in info.split("\n"):
        extent = re_extent.match(line)
        if extent:
            l, box = line.split(": ")
            geom = l[extent.span()[1]:].strip().replace("(", "").replace(")", "")
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
        l = layername.lower()
        metadata.update(layerinfo)
        for dt, ft in metadata["timeliness"].items():
            if l in ft:
                metadata[f"Timeliness [{l}]"] = dt
    del metadata["timeliness"]


def ogr2postgres(context: OpExecutionContext, dataset: str, extract_path: Path,
                 feature_type: str, xsd: str,
                 new_table: PostgresTableIdentifier) -> dict:
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

    cmd = " ".join([
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
    ])

    kwargs = {
        "new_table": new_table,
        "feature_type": feature_type,
        "dsn": dsn,
        "xsd": xsd,
        "dataset": dataset,
    }
    return_code, output = gdal.execute("ogr2ogr", command=cmd, kwargs=kwargs,
                                       local_path=extract_path)
    if return_code == 0:
        return postgrestable_metadata(context, new_table)


@op(config_schema={"remove_file_store": Field(bool, default_value=False)},
    required_resource_keys={"container", "file_store"})
def clean_storage(context):
    """Remove docker containers and associated volumes, and the 'file_store'."""
    docker_client = docker.from_env()
    container = docker_client.containers.get(context.resources.container.id)
    container.remove(force=True, v=True)
    context.log.info(f"Removed container {context.resources.container.id}")
    if context.op_config["remove_file_store"]:
        context.resources.file_store.data_dir.unlink()
        context.log.info(
            f"Removed local directory {context.resources.file_store.data_dir}")


def drop_table(context, conn, new_table):
    """DROP TABLE IF EXISTS new_table CASCADE"""
    q = SQL("DROP TABLE IF EXISTS {tbl} CASCADE;").format(tbl=new_table.id)
    context.log.info(conn.print_query(q))
    conn.send_query(q)


def create_schema(context, conn, new_schema):
    """CREATE SCHEMA IF NOT EXISTS new_schema"""
    q = SQL("CREATE SCHEMA IF NOT EXISTS {sch};").format(sch=Identifier(new_schema))
    context.log.info(conn.print_query(q))
    conn.send_query(q)


def pdal_info(pdal, file_path: Path,
              with_all: bool = False) -> Tuple[int, dict]:
    """Run 'pdal info' on a point cloud file.

    Args:
        pdal (AppImage): The pdal AppImage executable.
        file_path: Path to the point cloud file.
        with_all: If true, run ``pdal info --all``, else run ``pdal info --metadata``.
            Defaults to ``False``.
    Returns:
        A tuple of (pdal's return code, parsed pdal info output)
    """
    cmd_list = ["{exe}", "info", ]
    cmd_list.append("--all") if with_all else cmd_list.append("--metadata")
    cmd_list.append("{local_path}")
    return_code, output = pdal.execute(
        "pdal", command=" ".join(cmd_list), local_path=file_path)

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
            "(" + ",".join([f"{pt[0]} {pt[1]}" for pt in inner_ring]) + ")")
    inner_str = ",".join(inner_strings)
    if len(inner_strings) > 0:
        return f"POLYGON({outer_str},{inner_str})"
    else:
        return f"POLYGON({outer_str})"


def bag3d_dir(root_dir: Path):
    return Path(root_dir) / "3DBAG"


def geoflow_crop_dir(root_dir):
    return bag3d_dir(root_dir) / "crop_reconstruct"


def bag3d_export_dir(root_dir):
    return bag3d_dir(root_dir) / "export"


def format_date(input_date: date, version: bool = True) -> str:
    """Formats a date for using it in versions, filenames, attributes etc.

    Args:
        input_date:
        version: If True, format the input_date as '9999.99.99', else as '9999-99-99'
    """
    if version:
        return input_date.strftime("%Y.%m.%d")
    else:
        return input_date.strftime("%Y-%m-%d")


def get_upstream_data_version(context: OpExecutionContext, asset_key: AssetKey) -> str:
    """Workaround for getting the upstream data version of an asset.
    Might change in future dagster.
    https://dagster.slack.com/archives/C01U954MEER/p1681931980941599?thread_ts=1681930694.932489&cid=C01U954MEER"""
    upstream_entry = context.get_step_execution_context().get_input_asset_record(
        asset_key).event_log_entry
    upstream_data_version = extract_data_version_from_entry(upstream_entry)
    return upstream_data_version.value
