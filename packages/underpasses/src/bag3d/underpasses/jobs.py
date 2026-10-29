"""Jobs for underpass detection."""

from dagster import AssetSelection, define_asset_job

job_underpasses = define_asset_job(
    name="underpasses",
    description="Detect 2D underpasses, classify their boundaries, and offset exterior edges.",
    selection=AssetSelection.groups("underpasses"),
)
