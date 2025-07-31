"""Deploy 3D BAG to godzilla and podzilla servers and perform the final steps of the release"""

import tarfile
from pathlib import Path
import json

from dagster import AssetIn, Output, asset, AssetKey

from bag3d.common.utils.database import load_sql
from bag3d.common.types import PostgresTableIdentifier
from dagster import get_dagster_logger


logger = get_dagster_logger("deploy")


@asset(
    ins={"reconstruction_output_multitiles_nl": AssetIn(key_prefix="export")},
    deps=[
        AssetKey(("export", "geopackage_nl")),
        AssetKey(("export", "export_index")),
        AssetKey(("export", "metadata")),
        AssetKey(("export", "compressed_tiles")),
        AssetKey(("export", "compressed_tiles_validation")),
        AssetKey(("export", "reconstruction_output_3dtiles_lod12_nl")),
        AssetKey(("export", "reconstruction_output_3dtiles_lod13_nl")),
        AssetKey(("export", "reconstruction_output_3dtiles_lod22_nl")),
    ],
    required_resource_keys={"version"},
)
def compressed_export_nl(context, reconstruction_output_multitiles_nl):
    """A .tar.gz compressed full directory tree of the exports"""
    export_dir = reconstruction_output_multitiles_nl
    version = context.resources.version.version
    output_tarfile = export_dir.parent / f"export_{version}.tar.gz"
    with tarfile.open(output_tarfile, "w:gz") as tar:
        tar.add(export_dir, arcname=f"export_{version}")
    metadata_output = {
        "size [Gb]": output_tarfile.stat().st_size * 1e-9,
        "path": str(output_tarfile),
    }
    return Output(output_tarfile, metadata=metadata_output)


@asset(
    ins={"metadata": AssetIn(key_prefix="export")},
    required_resource_keys={"podzilla_server"},
)
def transfer_to_podzilla(
    context,
    compressed_export_nl: Path,
    metadata: Path,
):
    """Transfer the export_<version>.tar.gz archive to `podzilla` and decompress the files
    in the target directory.
    The target directory is set to the `BAG3D_PODZILLA_TARGET_DIR` environment variable.
    The version is extracted from the metadata file and used to create a subdirectory
    in the target directory.
    The files on `podzilla` will be used for the 3DBAG API.
    """
    data_dir: str = context.resources.podzilla_server.target_dir
    with metadata.open("r") as fo:
        metadata_json = json.load(fo)
        version = metadata_json["identificationInfo"]["citation"]["edition"]
        deploy_dir = f"{data_dir}/{version}"
        compressed_file = Path(data_dir) / compressed_export_nl.name

    try:
        with context.resources.podzilla_server.connect as c:
            # test connection
            result = c.run("echo connected", hide=True)
            assert result.ok, "Connection command failed"
            logger.debug("SSH connection successful")

            logger.debug(f"Transferring {compressed_export_nl} to {data_dir}")
            result = c.put(compressed_export_nl, remote=data_dir)
            logger.debug(f"Transferred: {result}")

            logger.debug(f"Creating deploy_dir {deploy_dir}")
            result = c.run(f"mkdir -p {deploy_dir}")
            assert result.ok, "Creating deploy_dir failed"

            logger.debug(f"Decompressing {compressed_file} to {deploy_dir}")
            result = c.run(
                f"tar --strip-components=1 -C {deploy_dir} -xzvf {compressed_file}"
            )
            assert result.ok, "Decompressing failed"

            logger.info(
                f"Deployment successful: Files transferred to {deploy_dir} on podzilla"
            )
    except Exception as e:
        logger.error(f"SSH connection failed: {e}")
        raise
    return deploy_dir


@asset(
    ins={"metadata": AssetIn(key_prefix="export")},
    required_resource_keys={"godzilla_server"},
)
def transfer_to_godzilla(
    context,
    compressed_export_nl: Path,
    metadata: Path,
):
    """Transfer the export_<version>.tar.gz archive to `godzilla` and decompress the files
    in the target directory.
    The target directory is set to the `BAG3D_GODZILLA_TARGET_DIR` environment variable.
    The version is extracted from the metadata file and used to create a subdirectory
    in the target directory.
    The files on `godzilla` will be made available for direct download and will be used by the webservices.
    """
    data_dir: str = context.resources.godzilla_server.target_dir
    with metadata.open("r") as fo:
        metadata_json = json.load(fo)
        version = metadata_json["identificationInfo"]["citation"]["edition"]
        deploy_dir = f"{data_dir}/{version}"
        compressed_file = Path(data_dir) / compressed_export_nl.name

    try:
        with context.resources.godzilla_server.connect as c:
            # test connection
            result = c.run("echo connected", hide=True)
            assert result.ok, "Connection command failed"
            logger.debug("SSH connection successful")

            logger.debug(f"Transferring {compressed_export_nl} to {data_dir}")
            result = c.put(compressed_export_nl, remote=data_dir)
            logger.debug(f"Transferred: {result}")

            logger.debug(f"Creating deploy_dir {deploy_dir}")
            result = c.run(f"mkdir -p {deploy_dir}")
            assert result.ok, "Creating deploy_dir failed"

            logger.debug(f"Decompressing {compressed_file} to {deploy_dir}")
            result = c.run(
                f"tar --strip-components=1 -C {deploy_dir} -xzvf {compressed_file}"
            )
            assert result.ok, "Decompressing failed"

            logger.info(
                f"Deployment successful: Files transferred to {deploy_dir} on godzilla"
            )
    except Exception as e:
        logger.error(f"SSH connection failed: {e}")
        raise
    return deploy_dir


@asset(
    deps={AssetKey(("transfer_to_godzilla"))},
    required_resource_keys={"db_connection", "godzilla_server"},
)
def webservice_godzilla(context, transfer_to_godzilla):
    """
    Load the layers for WFS, WMS to the database on Godzilla.
    The layers will be loaded into the schema `webservice_dev` and
    will not be published yet by the geoserver. The publication will
    be done in the `nl_release` job.
    """
    schema = "webservice_dev"
    sql = f"drop schema if exists {schema} cascade; create schema {schema};"
    with context.resources.godzilla_server.connect as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'"
        )

    deploy_dir = transfer_to_godzilla

    for layer in ["pand", "lod12_2d", "lod13_2d", "lod22_2d"]:
        cmd = " ".join(
            [
                "PG_USE_COPY=YES",
                "OGR_TRUNCATE=YES",
                "/opt/bin/ogr2ogr",
                "-gt",
                "65536",
                "-lco",
                "SPATIAL_INDEX=NONE",
                "-f",
                "PostgreSQL",
                f'PG:"dbname=baseregisters port=5432 host=localhost user=etl active_schema={schema}"',
                f"/vsizip/{deploy_dir}/3dbag_nl.gpkg.zip",
                layer,
                "-nln",
                layer + "_tmp",
            ]
        )
        with context.resources.godzilla_server.connect as c:
            context.log.debug(cmd)
            r = c.run(cmd)
            context.log.debug(r.stdout)

    pand_table = PostgresTableIdentifier(schema, "pand_tmp")
    lod12_2d_tmp = PostgresTableIdentifier(schema, "lod12_2d_tmp")
    lod13_2d_tmp = PostgresTableIdentifier(schema, "lod13_2d_tmp")
    lod22_2d_tmp = PostgresTableIdentifier(schema, "lod22_2d_tmp")
    lod12_2d = PostgresTableIdentifier(schema, "lod12_2d")
    lod13_2d = PostgresTableIdentifier(schema, "lod13_2d")
    lod22_2d = PostgresTableIdentifier(schema, "lod22_2d")

    # Create the LoD tables
    sql = load_sql(
        filename="webservice_lod.sql",
        query_params={
            "pand_table": pand_table,
            "lod12_2d_tmp": lod12_2d_tmp,
            "lod13_2d_tmp": lod13_2d_tmp,
            "lod22_2d_tmp": lod22_2d_tmp,
            "lod12_2d": lod12_2d,
            "lod13_2d": lod13_2d,
            "lod22_2d": lod22_2d,
        },
    )
    sql = context.resources.db_connection.connect.print_query(sql)
    with context.resources.godzilla_server.connect as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'"
        )

    # Create the intermediary export_index and validate_compressed_files tables so that they can be populated from the CSV files
    export_index = PostgresTableIdentifier(schema, "export_index")
    validate_compressed_files = PostgresTableIdentifier(
        schema, "validate_compressed_files"
    )
    sql = load_sql(
        filename="webservice_tiles_intermediary.sql",
        query_params={
            "export_index": export_index,
            "validate_compressed_files": validate_compressed_files,
        },
    )
    sql = context.resources.db_connection.connect.print_query(sql)
    with context.resources.godzilla_server.connect as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'"
        )

    # Load the CSV files into the intermediary tables
    with context.resources.godzilla_server.connect as c:
        filepath = f"{deploy_dir}/export_index.csv"
        copy_cmd = (
            "\copy "
            + str(export_index)
            + " FROM '"
            + filepath
            + "' DELIMITER ',' CSV HEADER "
        )
        context.log.debug(f"{copy_cmd}")
        c.run(
            rf'psql --dbname baseregisters --port 5432 --host localhost --user etl -c "{copy_cmd}" '
        )
        filepath = f"{deploy_dir}/validate_compressed_files.csv"
        copy_cmd = (
            "\copy "
            + str(validate_compressed_files)
            + " FROM '"
            + filepath
            + "' DELIMITER ',' CSV HEADER "
        )
        context.log.debug(f"{copy_cmd}")
        c.run(
            rf'psql --dbname baseregisters --port 5432 --host localhost --user etl -c "{copy_cmd}" '
        )

    # Create the public 'tiles' table
    tiles = PostgresTableIdentifier(schema, "tiles")
    sql = load_sql(
        filename="webservice_tiles.sql",
        query_params={
            "new_table": tiles,
            "export_index": export_index,
            "validate_compressed_files": validate_compressed_files,
        },
    )
    sql = context.resources.db_connection.connect.print_query(sql)
    with context.resources.godzilla_server.connect as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'"
        )

    grant_usage = f"GRANT USAGE ON SCHEMA {schema} TO bag_geoserver;"
    grant_select = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO bag_geoserver;"

    with context.resources.godzilla_server.connect as c:
        context.log.debug(grant_usage)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{grant_usage}'"
        )
        context.log.debug(grant_select)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{grant_select}'"
        )

    return (
        f"{schema}.lod12_2d",
        f"{schema}.lod13_2d",
        f"{schema}.lod22_2d",
        f"{schema}.tiles",
    )
