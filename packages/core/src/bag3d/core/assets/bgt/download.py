from dagster import (asset, Output, Field)

from bag3d.common.utils.requests import download_extract
from bag3d.common.utils.geodata import ogrinfo, add_info
from bag3d.common.custom_types import Path


@asset(
    config_schema={
        "featuretypes": Field(
            list,
            default_value=["pand", "wegdeel"],
            description="The feature types to download.",
            is_required=False
        ),
        "geofilter": Field(
            str,
            description="WKT of the polygonal extent",
            is_required=False
        ),
    },
    required_resource_keys={"gdal", "file_store"}
)
def extract_bgt(context) -> Output[Path]:
    """The TOP10NL extract downloaded from the PDOK API, containing the 'pand' and
    'wegdeel' layers."""
    metadata = download_extract(
        dataset="bgt",
        url_api="https://api.pdok.nl/lv/bgt/download/v1_0",
        featuretypes=context.op_config["featuretypes"],
        data_format="gmllight",
        geofilter=context.op_config["geofilter"],
        download_dir=context.resources.file_store.data_dir
    )
    extract_path = Path(metadata["Extract Path"].value)
    context.log.info(f"Downloaded {extract_path}")
    metadata["XSD"] = "http://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    info = dict(ogrinfo(context, dataset="bgt", extract_path=extract_path,
                        feature_types=context.op_config["featuretypes"],
                        xsd=metadata["XSD"]))
    add_info(metadata, info)
    return Output(extract_path, metadata=metadata)
