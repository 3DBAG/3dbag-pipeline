"""Deploy 3D BAG to godzilla and podzilla servers and perform the final steps of the release"""

from pathlib import Path
import json

from dagster import AssetIn, asset, AssetKey

from bag3d.common.resources.server_transfer import ServerTransferResource
from dagster import get_dagster_logger
from datetime import datetime


logger = get_dagster_logger("publish")


@asset(
    ins={
        "metadata": AssetIn(key_prefix="export"),
        "transfer_to_godzilla": AssetIn(key_prefix="deploy"),
    },
)
def publish_data(
    context,
    transfer_to_godzilla: tuple[Path, Path],
    metadata: Path,
    godzilla_server: ServerTransferResource,
):
    """On godzilla, create symlink to the 'export' to the current version
    and add the current version to the tar.gz archive.
    """
    public_dir: str = godzilla_server.public_dir
    deploy_dir, compressed_file = transfer_to_godzilla
    with metadata.open("r") as fo:
        metadata_json = json.load(fo)
        version = metadata_json["identificationInfo"]["citation"]["edition"]

    try:
        with godzilla_server.connection as c:
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
                f"Data Release successful: Link made to {public_dir}/{version_nopoints} on godzilla"
            )
    except Exception as e:
        logger.error(f"Data release failed: {e}")
        raise


@asset(
    deps={AssetKey(("deploy", "webservice_godzilla"))},
)
def publish_webservices(context, godzilla_server: ServerTransferResource):
    """ """
    latest_schema = "webservice"
    dev_schema = "webservice_dev"

    extension = str(datetime.now().date())
    alter_latest_to_archive = (
        f"ALTER SCHEMA {latest_schema} RENAME TO bag3d_{extension};"
    )
    alter_dev_to_latest = f"ALTER SCHEMA {dev_schema} RENAME TO {latest_schema};"

    try:
        with godzilla_server.connection as c:
            context.log.debug(alter_latest_to_archive)
            c.run(
                f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{alter_latest_to_archive}'"
            )
            context.log.debug(alter_dev_to_latest)
            c.run(
                f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{alter_dev_to_latest}'"
            )
    except Exception as e:
        logger.error(f"Publishing Webservices on Godzilla failed: {e}")
        raise
