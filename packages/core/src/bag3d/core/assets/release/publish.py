"""Deploy 3D BAG to godzilla and podzilla servers and perform the final steps of the release"""

from pathlib import Path
import json

from dagster import AssetIn, asset, AssetKey

from dagster import get_dagster_logger
from datetime import datetime


logger = get_dagster_logger("publish")


@asset(
    deps={AssetKey(("transfer_to_godzilla"))},
    ins={
        "metadata": AssetIn(key_prefix="export"),
        "compressed_export_nl": AssetIn(key_prefix="deploy"),
    },
    required_resource_keys={"godzilla_server"},
)
def publish_data(
    context,
    compressed_export_nl: Path,
    metadata: Path,
):
    """On godzilla, create symlink to the 'export' to the current version
    and add the current version to the tar.gz archive.
    """
    data_dir: str = context.resources.godzilla_server.target_dir
    public_dir: str = context.resources.godzilla_server.target_dir
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
                f"Data Release successful: Link made to {public_dir}/{version_nopoints} on godzilla"
            )
    except Exception as e:
        logger.error(f"Data release failed: {e}")
        raise


@asset(
    deps={AssetKey(("webservice_godzilla"))}, required_resource_keys={"godzilla_server"}
)
def publish_webservices(context):
    """ """
    latest_schema = "webservice"
    dev_schema = "webservice_dev"

    extension = str(datetime.now().date())
    alter_latest_to_archive = (
        f"ALTER SCHEMA {latest_schema} RENAME TO bag3d_{extension};"
    )
    alter_dev_to_latest = f"ALTER SCHEMA {dev_schema} RENAME TO {latest_schema};"

    try:
        with context.resources.godzilla_server.connect as c:
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
