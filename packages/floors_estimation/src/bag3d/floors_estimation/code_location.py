from dagster import Definitions, load_assets_from_modules

from bag3d.common.resources import resource_defs
from bag3d.floors_estimation.assets import floors_estimation
from bag3d.floors_estimation.jobs import job_floors_estimation

all_assets = load_assets_from_modules(
    modules=(floors_estimation,),
    key_prefix="floors_estimation",
    group_name="floors_estimation",
)

defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=[
        job_floors_estimation,
    ],
)
