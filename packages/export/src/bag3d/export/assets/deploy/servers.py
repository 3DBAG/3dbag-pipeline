"""Deploy 3D BAG to the publication server and perform the final steps of the release"""

import tarfile
from pathlib import Path

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.server_transfer import ServerTransferResource
from bag3d.common.resources.version import ReleaseVersionResource
from bag3d.common.types import PostgresTableIdentifier
from bag3d.common.utils.database import load_sql
from dagster import AssetIn, AssetKey, MetadataValue, Output, asset, get_dagster_logger

from bag3d.export.assets.export.metadata import read_export_edition

logger = get_dagster_logger("deploy")


@asset(
    ins={"metadata": AssetIn(key_prefix="export")},
    deps=[
        AssetKey(("export", "geopackage")),
        AssetKey(("export", "export_index")),
        AssetKey(("export", "compressed_tiles")),
        AssetKey(("export", "compressed_tiles_validation")),
        AssetKey(("export", "reconstruction_output_ifc")),
        AssetKey(("export", "reconstruction_output_gpkg")),
        AssetKey(("export", "reconstruction_output_3dtiles_lod12")),
        AssetKey(("export", "reconstruction_output_3dtiles_lod13")),
        AssetKey(("export", "reconstruction_output_3dtiles_lod22")),
        AssetKey(("export", "reconstruction_output_obj_lod12")),
        AssetKey(("export", "reconstruction_output_obj_lod13")),
        AssetKey(("export", "reconstruction_output_obj_lod22")),
    ],
)
def compressed_export(metadata: Path, version: ReleaseVersionResource) -> Output[Path]:
    """Create a compressed tar.gz archive containing the complete 3D BAG export.
    The archive will be named `export_<version>.tar.gz`.

    Args:
        metadata: Path to the 3DBAG metadata file
        version: Version resource

    Returns:
        Output: Path to the created export_{version}.tar.gz file with size metadata
    """
    export_dir = metadata.parent
    version_str = version.version
    output_tarfile = export_dir.parent / f"export_{version_str}.tar.gz"
    logger.info(
        "Archiving release %s from %s to %s", version_str, export_dir, output_tarfile
    )
    try:
        with tarfile.open(output_tarfile, "w:gz") as tar:
            tar.add(export_dir, arcname=f"export_{version_str}")
    except (OSError, tarfile.TarError) as exc:
        raise RuntimeError(
            f"Could not archive {export_dir} to {output_tarfile}. "
            f"Check that the export files are readable and the target has free space. {exc}"
        ) from exc
    metadata_output = {
        "size [Gb]": output_tarfile.stat().st_size * 1e-9,
        "artifact path": MetadataValue.path(output_tarfile),
        "source directory": MetadataValue.path(export_dir),
        "release": version_str,
    }
    return Output(output_tarfile, metadata=metadata_output)


def transfer_to_server(
    server: ServerTransferResource,
    compressed_export: Path,
    metadata: Path,
    target_dir: str,
) -> tuple[Path, Path]:
    """Transfer and extract export file to a remote server.

    Args:
        server: SSH connection resource for the target server
        compressed_export: Path to the compressed export file
        metadata: Path to metadata file containing version information
        target_dir: Base directory on remote server for deployment

    Returns:
        (Path to the deployment directory on the remote server, Path to the compressed export on the remote server)

    Raises:
        RuntimeError: If the connection, transfer or extraction fails.
    """

    version = read_export_edition(metadata)
    deploy_dir = Path(target_dir) / version
    compressed_file = Path(target_dir) / compressed_export.name

    operation = "open SSH connection"
    try:
        with server.connection as c:
            c.run("echo connected", hide=True, warn=False)

            operation = f"upload {compressed_export} to {compressed_file}"
            logger.info(
                "Uploading %s to %s:%s", compressed_export, server.host, compressed_file
            )
            c.put(compressed_export, remote=target_dir)

            operation = f"create deployment directory {deploy_dir}"
            c.run(f"mkdir -p {deploy_dir}", warn=False)

            operation = f"extract {compressed_file} into {deploy_dir}"
            logger.info(
                "Extracting %s into %s on %s", compressed_file, deploy_dir, server.host
            )
            c.run(
                f"tar --strip-components=1 -C {deploy_dir} -xzf {compressed_file}",
                warn=False,
            )

            operation = "close SSH connection after extraction"
    except Exception as exc:
        raise RuntimeError(
            f"Could not {operation} on {server.host} for release {version}. {exc}"
        ) from exc
    return deploy_dir, compressed_file


@asset(
    ins={"metadata": AssetIn(key_prefix="export")},
)
def transfer_to_publication(
    compressed_export: Path,
    metadata: Path,
    publication_server: ServerTransferResource,
) -> Output[tuple[Path, Path]]:
    """Transfer the 3D BAG export to the publication server for public downloads and webservices."""
    deployment = transfer_to_server(
        publication_server,
        compressed_export,
        metadata,
        publication_server.target_dir,
    )
    deploy_dir, compressed_file = deployment
    return Output(
        deployment,
        metadata={
            "host": publication_server.host,
            "deployment directory": MetadataValue.path(deploy_dir),
            "remote archive": MetadataValue.path(compressed_file),
        },
    )


@asset(
    deps={AssetKey(("deploy", "transfer_to_publication"))},
)
def webservice_publication(
    transfer_to_publication: tuple[Path, Path],
    computation_db: DatabaseResource,
    publication_server: ServerTransferResource,
    publication_db: DatabaseResource,
) -> Output[tuple[str, str, str, str]]:
    """
    Load the layers for WFS, WMS to the database on the publication server.
    The layers will be loaded into the schema `webservice_dev` and
    will not be published yet by the geoserver. The publication will
    be done in the `release` job.
    """
    schema = "webservice_dev"
    deploy_dir, _ = transfer_to_publication
    archive = deploy_dir / "3dbag_nl.gpkg.zip"
    database = f"{publication_db.host}:{publication_db.port}/{publication_db.dbname}"
    psql = (
        f"psql --dbname {publication_db.dbname} --port {publication_db.port} "
        f"--host {publication_db.host} --user {publication_db.user}"
    )

    def run_remote(command: str, operation: str) -> None:
        logger.info(
            "%s on %s for database %s", operation, publication_server.host, database
        )
        try:
            with publication_server.connection as connection:
                connection.run(command, warn=False)
        except Exception as exc:
            raise RuntimeError(
                f"{operation} failed on {publication_server.host} for database {database}. {exc}"
            ) from exc

    sql = f"drop schema if exists {schema} cascade; create schema {schema};"
    run_remote(f"{psql} -c '{sql}'", f"Recreate staging schema {schema}")

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
                f'PG:"dbname={publication_db.dbname} port={publication_db.port} host={publication_db.host} user={publication_db.user} active_schema={schema}"',
                f"/vsizip/{archive}",
                layer,
                "-nln",
                layer + "_tmp",
            ]
        )
        run_remote(cmd, f"Load {layer} from {archive} into {schema}.{layer}_tmp")

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
    sql = computation_db.connection.print_query(sql)
    run_remote(f"{psql} -c '{sql}'", f"Build LoD tables in {schema}")

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
    sql = computation_db.connection.print_query(sql)
    run_remote(
        f"{psql} -c '{sql}'", f"Create tile index and validation tables in {schema}"
    )

    # Load the CSV files into the intermediary tables
    for filename, table in (
        ("export_index.csv", export_index),
        ("validate_compressed_files.csv", validate_compressed_files),
    ):
        filepath = deploy_dir / filename
        copy_cmd = rf"\copy {table} FROM '{filepath}' DELIMITER ',' CSV HEADER"
        run_remote(f'{psql} -c "{copy_cmd}"', f"Load {filepath} into {table}")

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
    sql = computation_db.connection.print_query(sql)
    run_remote(f"{psql} -c '{sql}'", f"Build the webservice tiles table {tiles}")

    grant_usage = f"GRANT USAGE ON SCHEMA {schema} TO bag_geoserver;"
    grant_select = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO bag_geoserver;"

    run_remote(f"{psql} -c '{grant_usage}'", f"Grant bag_geoserver usage on {schema}")
    run_remote(
        f"{psql} -c '{grant_select}'",
        f"Grant bag_geoserver read access to tables in {schema}",
    )

    tables = (
        f"{schema}.lod12_2d",
        f"{schema}.lod13_2d",
        f"{schema}.lod22_2d",
        f"{schema}.tiles",
    )
    return Output(
        tables,
        metadata={
            "database": database,
            "schema": schema,
            "tables": MetadataValue.json(list(tables)),
            "source archive": MetadataValue.path(archive),
        },
    )
