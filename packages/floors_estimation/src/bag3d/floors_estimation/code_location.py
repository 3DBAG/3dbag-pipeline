import os

from bag3d.common.resources import DagsterDeployment, resource_defs
from dagster import Definitions, load_assets_from_modules

from bag3d.floors_estimation.assets import floors_estimation
from bag3d.floors_estimation.jobs import job_floors_estimation
from bag3d.floors_estimation.resources import ModelStoreResource

all_assets = load_assets_from_modules(
    modules=(floors_estimation,),
    key_prefix="floors_estimation",
    group_name="floors_estimation",
)

dagster_deployment = os.getenv("DAGSTER_DEPLOYMENT", "default")
if dagster_deployment.lower() == DagsterDeployment.DEFAULT:
    model_store = ModelStoreResource.configure_at_launch()
elif dagster_deployment.lower() in DagsterDeployment.env_configured_deployments():
    model_store = ModelStoreResource(
        model_path=os.environ["BAG3D_FLOORS_ESTIMATION_MODEL"]
    )
else:
    raise RuntimeError(
        f"Invalid DAGSTER_DEPLOYMENT {dagster_deployment}, cannot configure dagster environment"
    )
resource_defs.update({"model_store": model_store})


defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=[
        job_floors_estimation,
    ],
)
