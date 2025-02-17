from bag3d.common.resources import resource_defs
from bag3d.floors_estimation.assets import floors_estimation
from bag3d.floors_estimation.jobs import job_floors_estimation
from dagster import Definitions, load_assets_from_modules
from dagster import resource
import os

all_assets = load_assets_from_modules(
    modules=(floors_estimation,),
    key_prefix="floors_estimation",
    group_name="floors_estimation",
)


@resource(config_schema={"model": str})
def model_store(context):
    """A resource for the floors' estimation model."""
    return context.resource_config["model"]


floors_model = model_store.configured(
    {"model": os.getenv("BAG3D_FLOORS_ESTIMATION_MODEL")}
)
resource_defs.update({"model_store": floors_model})


defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=[
        job_floors_estimation,
    ],
)
