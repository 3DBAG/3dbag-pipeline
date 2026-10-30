"""Jobs for underpass detection."""

from dagster import AssetSelection, define_asset_job

job_underpasses = define_asset_job(
    name="underpasses",
    description="Detect underpasses and estimate ceilings from lidar and filled AHN for Roofer.",
    selection=AssetSelection.groups("underpasses"),
)
