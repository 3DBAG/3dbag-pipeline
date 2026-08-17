from bag3d.common.resources.executables import GDALResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.types import Path
from bag3d.common.utils.geodata import add_info, ogrinfo
from bag3d.common.utils.requests import download_extract
from dagster import AutomationCondition, Config, Output, asset, get_dagster_logger
from pydantic import Field

logger = get_dagster_logger("bgt.download")


class BgtDownloadConfig(Config):
    """Configuration for BGT download assets."""

    featuretypes: list = Field(
        default=["pand"], description="The feature types to download."
    )
    geofilter: str | None = Field(
        default=None, description="WKT of the polygonal extent"
    )


@asset(automation_condition=AutomationCondition.on_cron("0 6 9 * *"))
def extract_bgt(
    config: BgtDownloadConfig,
    file_store: FileStoreResource,
    gdal: GDALResource,
) -> Output[Path]:
    """The BGT extract downloaded from the PDOK API, containing the 'pand' layer."""
    metadata = download_extract(
        dataset="bgt",
        url_api="https://api.pdok.nl/lv/bgt/download/v1_0",
        featuretypes=config.featuretypes,
        data_format="gmllight",
        geofilter=config.geofilter,
        download_dir=file_store.path,
    )
    extract_path = Path(metadata["Extract Path"].value)
    logger.info(f"Downloaded {extract_path}")
    logger.info("Starting ogrinfo to extract metadata...")
    logger.info(config.featuretypes)
    metadata["XSD"] = (
        "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    )
    info = dict(
        ogrinfo(
            gdal_runner=gdal.runner,
            dataset="bgt",
            extract_path=extract_path,
            feature_types=config.featuretypes,
            xsd=metadata["XSD"],
        )
    )
    add_info(metadata, info)
    return Output(extract_path, metadata=metadata)
