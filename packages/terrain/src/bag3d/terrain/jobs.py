"""Manual jobs with explicit acquisition and offline processing selections."""

from dagster import AssetKey, AssetSelection, define_asset_job

DOWNLOAD_ASSETS = (
    "ahn_catalog",
    "ahn_download",
    "glo30_download",
    "proj_grids_download",
)
download_selection = AssetSelection.assets(
    *[AssetKey(["terrain", name]) for name in DOWNLOAD_ASSETS]
)
all_selection = AssetSelection.groups("terrain")

job_terrain_download = define_asset_job(
    "terrain_download", selection=download_selection
)
job_terrain_process = define_asset_job(
    "terrain_process", selection=all_selection - download_selection
)
job_terrain = define_asset_job("terrain", selection=all_selection)

all_jobs = [job_terrain_download, job_terrain_process, job_terrain]
