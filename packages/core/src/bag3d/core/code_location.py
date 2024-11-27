from dagster import Definitions

from bag3d.common.resources import resource_defs
from bag3d.core.asset_groups import (
    source_assets,
    input_assets,
    reconstruction_assets,
    export_assets,
    deploy_assets,
)
from bag3d.core.jobs import (
    job_source_input,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn_tiles_200m,
    job_nl_reconstruct,
    job_nl_export,
    job_nl_deploy,
)

all_assets = [
    *source_assets,
    *input_assets,
    *reconstruction_assets,
    *export_assets,
    *deploy_assets,
]

all_jobs = [
    job_source_input,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn_tiles_200m,
    job_nl_reconstruct,
    job_nl_export,
    job_nl_deploy,
]

defs = Definitions(resources=resource_defs, assets=all_assets, jobs=all_jobs)
