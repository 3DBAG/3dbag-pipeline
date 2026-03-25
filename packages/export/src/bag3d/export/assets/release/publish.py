"""Perform the final steps for the 3D BAG release on the publication server"""

from pathlib import Path
import json
from datetime import datetime

from dagster import AssetIn, asset, AssetKey

from bag3d.common.resources.server_transfer import ServerTransferResource
from bag3d.common.resources.database import DatabaseResource
from dagster import get_dagster_logger


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
) -> None:
    """On the publication server, create symlink to the 'export' to the current version
    and add the current version to the tar.gz archive.
    """
    if publication_server.public_dir is None:
        raise ValueError(
            "publication_server.public_dir must be configured for publish_data"
        )
    public_dir = publication_server.public_dir
    deploy_dir, compressed_file = transfer_to_publication
    with metadata.open("r") as fo:
        metadata_json = json.load(fo)
        version = metadata_json["identificationInfo"]["citation"]["edition"]

    try:
        with publication_server.connection as c:
            # test connection
            result = c.run("echo connected", hide=True)
            assert result.ok, "Connection command failed"
            logger.debug("SSH connection successful")

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

            logger.debug(
                f"Setting published version {version_nopoints} to latest version"
            )
            result = c.run(f"rm -f {public_dir}/latest")
            assert result.ok, "Removing public/latest symlink failed"
            result = c.run(f"ln -s {public_dir}/{version_nopoints} {public_dir}/latest")
            assert result.ok, "Setting latest version failed"

            logger.debug(f"Removing compressed file {compressed_file}")
            result = c.run(f"rm {compressed_file}")
            assert result.ok, "Removing compressed file failed"

            logger.info(
                f"Data Release successful: Link made to {public_dir}/{version_nopoints} on publication server"
            )
    except Exception as e:
        logger.error(f"Data release failed: {e}")
        raise


@asset(
    deps={AssetKey(("deploy", "webservice_publication"))},
)
def publish_webservices(
    publication_server: ServerTransferResource,
    publication_db: DatabaseResource,
) -> None:
    """Publish the webservices by promoting the dev schema to live on the publication server."""
    latest_schema = "webservice"
    dev_schema = "webservice_dev"

    extension = str(datetime.now().date())
    alter_latest_to_archive = (
        f"ALTER SCHEMA {latest_schema} RENAME TO bag3d_{extension};"
    )
    alter_dev_to_latest = f"ALTER SCHEMA {dev_schema} RENAME TO {latest_schema};"

    try:
        with publication_server.connection as c:
            logger.debug(alter_latest_to_archive)
            c.run(
                f"psql --dbname {publication_db.dbname} --port {publication_db.port} --host {publication_db.host} --user {publication_db.user} -c '{alter_latest_to_archive}'"
            )
            logger.debug(alter_dev_to_latest)
            c.run(
                f"psql --dbname {publication_db.dbname} --port {publication_db.port} --host {publication_db.host} --user {publication_db.user} -c '{alter_dev_to_latest}'"
            )
    except Exception as e:
        logger.error(f"Publishing Webservices on publication server failed: {e}")
        raise
