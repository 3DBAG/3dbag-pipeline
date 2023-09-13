from dagster import Definitions

from bag3d.common.resources import resource_defs
from bag3d.core.assets import (source_assets, input_assets,
                               reconstruction_assets, export_assets, deploy_assets,)
from bag3d.core.jobs import (job_source_input, job_clean_containers,
                             job_ahn3, job_ahn4, job_zuid_holland_reconstruct,
                             job_zuid_holland_export, job_zuid_holland_deploy,
                             job_zuid_holland_export_deploy,
                             job_nl_export_deploy,
                             job_nl_reconstruct, job_nl_export, job_nl_deploy,
                             job_nl_reconstruct_rerun )

all_assets = [
    *source_assets,
    *input_assets,
    *reconstruction_assets,
    *export_assets,
    *deploy_assets,
]

all_jobs = [
    job_clean_containers,
    job_source_input,
    job_ahn3,
    job_ahn4,
    job_zuid_holland_reconstruct,
    job_zuid_holland_export,
    job_zuid_holland_deploy,
    job_zuid_holland_export_deploy,
    job_nl_reconstruct,
    job_nl_export,
    job_nl_deploy,
    job_nl_reconstruct_rerun,
    job_nl_export_deploy,
]

defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=all_jobs
)
