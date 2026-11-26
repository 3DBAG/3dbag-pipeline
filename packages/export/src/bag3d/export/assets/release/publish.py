"""Perform the final steps for the 3D BAG release on the publication server"""

from datetime import UTC, datetime
from pathlib import Path

from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.server_transfer import ServerTransferResource
from dagster import AssetIn, AssetKey, MetadataValue, Output, asset, get_dagster_logger

from bag3d.export.assets.export.metadata import read_export_edition

logger = get_dagster_logger("release.publish")


@asset(
    ins={
        "metadata": AssetIn(key_prefix="export"),
        "transfer_to_publication": AssetIn(key_prefix="deploy"),
    },
)
def publish_data(
    transfer_to_publication: tuple[Path, Path],
    metadata: Path,
    publication_server: ServerTransferResource,
) -> Output[None]:
    """On the publication server, create symlink to the 'export' to the current version
    and add the current version to the tar.gz archive.
    """
    if publication_server.public_dir is None:
        raise ValueError(
            f"Set publication_server.public_dir for host {publication_server.host} "
            "before running release/publish_data."
        )
    public_dir = Path(publication_server.public_dir)
    deploy_dir, compressed_file = transfer_to_publication
    version = read_export_edition(metadata)
    version_link = public_dir / version.replace(".", "")
    latest_link = public_dir / "latest"
    host = publication_server.host
    logger.info(
        "Publishing release %s from %s to %s:%s", version, deploy_dir, host, public_dir
    )

    operation = "open SSH connection"
    try:
        with publication_server.connection as c:
            c.run("echo connected", hide=True, warn=False)
            operation = f"create public directory {public_dir}"
            c.run(f"mkdir -p {public_dir}", warn=False)
            operation = f"link {version_link} to {deploy_dir}"
            c.run(f"ln -s {deploy_dir} {version_link}", warn=False)
            operation = f"remove the previous {latest_link} link"
            c.run(f"rm -f {latest_link}", warn=False)
            operation = f"link {latest_link} to {version_link}"
            c.run(f"ln -s {version_link} {latest_link}", warn=False)
            operation = f"remove transferred archive {compressed_file}"
            c.run(f"rm {compressed_file}", warn=False)
            operation = "close SSH connection after publishing"
    except Exception as exc:
        raise RuntimeError(
            f"Could not {operation} on {host} while publishing release {version}. "
            f"Check {version_link} and {latest_link} before retrying. {exc}"
        ) from exc
    return Output(
        None,
        metadata={
            "release": version,
            "host": host,
            "deployment directory": MetadataValue.path(deploy_dir),
            "version link": MetadataValue.path(version_link),
            "latest link": MetadataValue.path(latest_link),
        },
    )


@asset(
    deps={AssetKey(("deploy", "webservice_publication"))},
)
def publish_webservices(
    publication_server: ServerTransferResource,
    publication_db: DatabaseResource,
) -> Output[None]:
    """Publish the webservices by promoting the dev schema to live on the publication server."""
    latest_schema = "webservice"
    dev_schema = "webservice_dev"

    archive_schema = f"bag3d_{datetime.now(tz=UTC).date()}"
    alter_latest_to_archive = (
        f"ALTER SCHEMA {latest_schema} RENAME TO {archive_schema};"
    )
    alter_dev_to_latest = f"ALTER SCHEMA {dev_schema} RENAME TO {latest_schema};"
    database = f"{publication_db.host}:{publication_db.port}/{publication_db.dbname}"
    psql = (
        f"psql --dbname {publication_db.dbname} --port {publication_db.port} "
        f"--host {publication_db.host} --user {publication_db.user}"
    )
    logger.info(
        "Publishing webservices on %s for database %s. Archive %s as %s and promote %s to %s.",
        publication_server.host,
        database,
        latest_schema,
        archive_schema,
        dev_schema,
        latest_schema,
    )

    operation = "open SSH connection"
    try:
        with publication_server.connection as c:
            operation = f"archive {latest_schema} as {archive_schema}"
            c.run(f"{psql} -c '{alter_latest_to_archive}'", warn=False)
            operation = f"promote {dev_schema} to {latest_schema}"
            c.run(f"{psql} -c '{alter_dev_to_latest}'", warn=False)
            operation = "close SSH connection after promoting the webservices"
    except Exception as exc:
        raise RuntimeError(
            f"Could not {operation} on {publication_server.host} for database {database}. "
            f"Check the state of schemas {latest_schema}, {dev_schema}, and "
            f"{archive_schema} before retrying. {exc}"
        ) from exc
    return Output(
        None,
        metadata={
            "database": database,
            "live schema": latest_schema,
            "archive schema": archive_schema,
        },
    )
