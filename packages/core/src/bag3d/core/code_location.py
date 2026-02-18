from os import getenv

from dagster import (
    Definitions,
    AutomationConditionSensorDefinition,
    AssetSelection,
    DefaultSensorStatus,
)

from bag3d.common.resources import resource_defs
from bag3d.core.asset_groups import (
    bgt_assets,
    source_assets,
    input_assets,
    reconstruction_assets,
    export_assets,
    deploy_assets,
    release_assets,
)
from bag3d.core.jobs import (
    job_bgt,
    job_source_input,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn_metadata_index,
    job_nl_reconstruct,
    job_nl_reconstruct_debug,
    job_nl_export,
    job_nl_export_after_floors,
    job_nl_deploy,
    job_nl_release,
)

all_assets = [
    *bgt_assets,
    *source_assets,
    *input_assets,
    *reconstruction_assets,
    *export_assets,
    *deploy_assets,
    *release_assets,
]

all_jobs = [
    job_bgt,
    job_source_input,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn_metadata_index,
    job_nl_reconstruct,
    job_nl_reconstruct_debug,
    job_nl_export,
    job_nl_export_after_floors,
    job_nl_deploy,
    job_nl_release,
]

sensor_status = (
    DefaultSensorStatus.RUNNING
    if getenv("DAGSTER_DEPLOYMENT") == "production"
    else DefaultSensorStatus.STOPPED
)
all_sensors = [
    AutomationConditionSensorDefinition(
        "automation_condition_sensor",
        target=AssetSelection.all(),
        default_status=sensor_status,
    )
]

defs = Definitions(
    resources=resource_defs, assets=all_assets, jobs=all_jobs, sensors=all_sensors
)
