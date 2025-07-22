"""Deploy 3D BAG to godzilla"""

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
    ins={"metadata": AssetIn(key_prefix="export")}, required_resource_keys={"version"}
)
def downloadable_podzilla(
    context,
    compressed_export_nl: Path,
    metadata: Path,
):
    """Downloadable files hosted on podzilla, for the 3DBAG API.
    Transfer the export_<version>.tar.gz archive to `podzilla` and decompress the archive
    """
    data_dir: str = context.resources.podzilla_server.dir
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
    ins={"metadata": AssetIn(key_prefix="export")}, required_resource_keys={"version"}
)
def downloadable_godzilla(
    context,
    compressed_export_nl: Path,
    metadata: Path,
):
    """Downloadable files hosted on godzilla.
    - Transfer the export_<version>.tar.gz archive to `godzilla:/data/3DBAG`
    - Uncompress the archive and add the current version to the directory name
    - Symlink to the 'export' to the current version
    - Add the current version to the tar.gz archive
    """
    data_dir: str = context.resources.godzilla_server.dir
    public_dir: str = context.resources.godzilla_server.public_dir
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

            # symlink to latest version so the fileserver picks up the data
            version_nopoints = version.replace(".", "")

            logger.debug(f"Creating public_dir {public_dir}")
            result = c.run(f"mkdir -p {public_dir}")
            assert result.ok, "Creating public_dir failed"

            logger.debug(
                f"Creating symlink to {deploy_dir} as {public_dir}/{version_nopoints}"
            )
            result = c.run(f"ln -s {deploy_dir} {public_dir}/{version_nopoints}")
            assert result.ok, "Creating symlink failed"

            logger.debug(f"Removing compressed file {compressed_file}")
            result = c.run(f"rm {compressed_file}")
            assert result.ok, "Removing compressed file failed"

            logger.info(
                f"Deployment successful: Files transferred to {public_dir}/{version_nopoints} on godzilla"
            )
    except Exception as e:
        logger.error(f"SSH connection failed: {e}")
        raise
    return deploy_dir


@asset(required_resource_keys={"db_connection"})
def webservice_godzilla(context, downloadable_godzilla):
    """Load the layers for WFS, WMS that are served from godzilla"""
    schema = "webservice_dev"
    sql = f"drop schema if exists {schema} cascade; create schema {schema};"
    with context.resources.godzilla_server.connect as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'"
        )

    deploy_dir = downloadable_godzilla

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

    # extension = str(datetime.now().date())
    # alter_to_archive = f"ALTER SCHEMA {old_schema} RENAME TO bag3d_{extension};"
    # alter_to_old = f"ALTER SCHEMA {schema} RENAME TO {old_schema};"
    grant_usage = f"GRANT USAGE ON SCHEMA {schema} TO bag_geoserver;"
    grant_select = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO bag_geoserver;"

    with context.resources.godzilla_server.connect as c:
        # context.log.debug(alter_to_archive)
        # c.run(
        #     f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{alter_to_archive}'")
        # context.log.debug(alter_to_old)
        # c.run(
        #     f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{alter_to_old}'")
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
