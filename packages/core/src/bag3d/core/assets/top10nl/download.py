from dagster import asset, Output, Field, DataVersion

from bag3d.common.utils.requests import download_extract
from bag3d.common.utils.geodata import ogrinfo, add_info
from bag3d.common.types import Path


@asset(
    config_schema={
        "featuretypes": Field(
            list,
            default_value=[
                "gebouw",
            ],
            description="The feature types to download.",
            is_required=False,
        ),
        "geofilter": Field(
            str, description="WKT of the polygonal extent", is_required=False
        ),
    },
    required_resource_keys={"gdal", "file_store"},
)
def extract_top10nl(context) -> Output[Path]:
    """The TOP10NL extract downloaded from the PDOK API, containing the Gebouw layer."""
    metadata = download_extract(
        dataset="top10nl",
        url_api="https://api.pdok.nl/brt/top10nl/download/v1_0",
        featuretypes=context.op_execution_context.op_config["featuretypes"],
        data_format="gml",
        geofilter=context.op_execution_context.op_config.get("geofilter"),
        download_dir=context.resources.file_store.file_store.data_dir,
    )
    extract_path = Path(metadata["Extract Path"].value)
    context.log.info(f"Downloaded {extract_path}")
    metadata["XSD"] = (
        "https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd"
    )
    info = ogrinfo(
        context,
        dataset="top10nl",
        extract_path=extract_path,
        feature_types=context.op_execution_context.op_config["featuretypes"],
        xsd=metadata["XSD"],
    )
    add_info(metadata, info)
    return Output(
        extract_path,
        metadata=metadata,
        data_version=DataVersion(metadata["Timeliness [gebouw]"]),
    )
