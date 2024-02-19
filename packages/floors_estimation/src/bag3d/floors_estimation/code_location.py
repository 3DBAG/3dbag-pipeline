from bag3d.common.resources import resource_defs
from bag3d.floors_estimation.assets import floors_estimation
from bag3d.floors_estimation.jobs import job_floors_estimation
from dagster import Definitions, load_assets_from_modules
from dagster import resource

all_assets = load_assets_from_modules(
    modules=(floors_estimation,),
    key_prefix="floors_estimation",
    group_name="floors_estimation",
)


@resource(config_schema={'model_dir': str})
def model_store(context):
    return context.resource_config['model_dir']


floors_model = model_store.configured({"model_dir": "/data2/floors-estimation/models/pipeline_model1_gbr_untuned.joblib"})
resource_defs.update({"model_path": floors_model})


defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=[
        job_floors_estimation,
    ],
)
