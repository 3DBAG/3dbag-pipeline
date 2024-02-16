from bag3d.common.resources import resource_defs
from bag3d.floors_estimation.assets import floors_estimation
from bag3d.floors_estimation.jobs import job_floors_estimation
from dagster import Definitions, load_assets_from_modules

all_assets = load_assets_from_modules(
    modules=(floors_estimation,),
    key_prefix="floors_estimation",
    group_name="floors_estimation",
)

resources = resource_defs.update({"model_path": '/data2/floors-estimation/models/pipeline_model1_gbr_untuned.joblib'})

defs = Definitions(
    resources=resources,
    assets=all_assets,
    jobs=[
        job_floors_estimation,
    ],
)
