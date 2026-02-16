from typing import Optional

from dagster import asset, Output, Config, get_dagster_logger
from pydantic import Field

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.executables import GDALResource
from bag3d.common.utils.requests import download_extract
from bag3d.common.utils.geodata import ogrinfo, add_info
from bag3d.common.types import Path

logger = get_dagster_logger("bgt.download")


class BgtDownloadConfig(Config):
    """Configuration for BGT download assets."""

    featuretypes: list = Field(
        default=["pand"], description="The feature types to download."
    )
    geofilter: Optional[str] = Field(
        default=None, description="WKT of the polygonal extent"
    )


@asset
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
        download_dir=file_store.file_store.data_dir,
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
