import os

from bag3d.common.resources import resource_defs, DagsterDeployment
from bag3d.floors_estimation.assets import floors_estimation, training_data
from bag3d.floors_estimation.jobs import job_floors_estimation, job_import_training_data
from bag3d.floors_estimation.resources import ModelStoreResource, TrainingDataResource
from dagster import Definitions, load_assets_from_modules

all_assets = load_assets_from_modules(
    modules=(floors_estimation, training_data),
    key_prefix="floors_estimation",
    group_name="floors_estimation",
)

dagster_deployment = os.getenv("DAGSTER_DEPLOYMENT", "default")
if dagster_deployment.lower() == DagsterDeployment.DEFAULT:
    model_store = ModelStoreResource.configure_at_launch()
    training_data_resource = TrainingDataResource.configure_at_launch()
elif dagster_deployment.lower() in DagsterDeployment.env_configured_deployments():
    model_store = ModelStoreResource(
        model_path=os.environ["BAG3D_FLOORS_ESTIMATION_MODEL"]
    )
    training_data_resource = TrainingDataResource(
        data_dir=os.environ["BAG3D_TRAINING_DATA"]
    )
else:
    raise RuntimeError(
        f"Invalid DAGSTER_DEPLOYMENT {dagster_deployment}, cannot configure dagster environment"
    )
resource_defs.update(
    {"model_store": model_store, "training_data": training_data_resource}
)


defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=[
        job_floors_estimation,
        job_import_training_data,
    ],
)
