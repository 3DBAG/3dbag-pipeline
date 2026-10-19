"""Jobs for underpass detection."""

from dagster import AssetSelection, define_asset_job

job_underpasses = define_asset_job(
    name="underpasses",
    description="Detect 2D underpass polygons from current BAG and BGT footprints.",
    selection=AssetSelection.groups("underpasses"),
)
