from bag3d.common.resources.executables import GDALResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.types import Path
from bag3d.common.utils.geodata import add_info, ogrinfo
from bag3d.common.utils.requests import download_extract
from dagster import (
    AutomationCondition,
    Config,
    DataVersion,
    Output,
    asset,
    get_dagster_logger,
)
from pydantic import Field

logger = get_dagster_logger("top10nl.download")


class Top10nlDownloadConfig(Config):
    """Configuration for TOP10NL download assets."""

    featuretypes: list = Field(
        default=["gebouw"], description="The feature types to download."
    )
    geofilter: str | None = Field(
        default=None, description="WKT of the polygonal extent"
    )


@asset(automation_condition=AutomationCondition.on_cron("0 12 9 * *"))
def extract_top10nl(
    config: Top10nlDownloadConfig,
    file_store: FileStoreResource,
    gdal: GDALResource,
) -> Output[Path]:
    """The TOP10NL extract downloaded from the PDOK API, containing the Gebouw layer."""
    metadata = download_extract(
        dataset="top10nl",
        url_api="https://api.pdok.nl/brt/top10nl/download/v1_0",
        featuretypes=config.featuretypes,
        data_format="gml",
        geofilter=config.geofilter,
        download_dir=file_store.path,
    )
    extract_path = Path(metadata["Extract Path"].value)
    logger.info(f"Downloaded {extract_path}")
    metadata["XSD"] = (
        "https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd"
    )
    info = ogrinfo(
        gdal_runner=gdal.runner,
        dataset="top10nl",
        extract_path=extract_path,
        feature_types=config.featuretypes,
        xsd=metadata["XSD"],
    )
    add_info(metadata, info)
    return Output(
        extract_path,
        metadata=metadata,
        data_version=DataVersion(metadata["Timeliness [gebouw]"]),
    )
