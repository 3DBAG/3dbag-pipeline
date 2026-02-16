import os

from bag3d.common.resources import resource_defs
from bag3d.floors_estimation.assets import floors_estimation
from bag3d.floors_estimation.jobs import job_floors_estimation
from bag3d.floors_estimation.resources import ModelStoreResource
from dagster import Definitions, load_assets_from_modules

all_assets = load_assets_from_modules(
    modules=(floors_estimation,),
    key_prefix="floors_estimation",
    group_name="floors_estimation",
)


model_store = ModelStoreResource(model_path=os.getenv("BAG3D_FLOORS_ESTIMATION_MODEL"))
resource_defs.update({"model_store": model_store})


defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=[
        job_floors_estimation,
    ],
)
