from typing import Optional

from dagster import asset, Output, Config
from pydantic import Field

from bag3d.common.utils.requests import download_extract
from bag3d.common.utils.geodata import ogrinfo, add_info
from bag3d.common.types import Path


class BgtDownloadConfig(Config):
    """Configuration for BGT download assets."""

    featuretypes: list = Field(
        default=["pand"], description="The feature types to download."
    )
    geofilter: Optional[str] = Field(
        default=None, description="WKT of the polygonal extent"
    )


@asset(
    required_resource_keys={"gdal", "file_store"},
)
def extract_bgt(context, config: BgtDownloadConfig) -> Output[Path]:
    """The BGT extract downloaded from the PDOK API, containing the 'pand' layer."""
    metadata = download_extract(
        dataset="bgt",
        url_api="https://api.pdok.nl/lv/bgt/download/v1_0",
        featuretypes=config.featuretypes,
        data_format="gmllight",
        geofilter=config.geofilter,
        download_dir=context.resources.file_store.file_store.data_dir,
    )
    extract_path = Path(metadata["Extract Path"].value)
    context.log.info(f"Downloaded {extract_path}")
    context.log.info("Starting ogrinfo to extract metadata...")
    context.log.info(config.featuretypes)
    metadata["XSD"] = (
        "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    )
    info = dict(
        ogrinfo(
            gdal_runner=context.resources.gdal.runner,
            dataset="bgt",
            extract_path=extract_path,
            feature_types=config.featuretypes,
            xsd=metadata["XSD"],
            context=context,
        )
    )
    add_info(metadata, info)
    return Output(extract_path, metadata=metadata)
