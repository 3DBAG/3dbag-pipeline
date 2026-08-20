from os import getenv

from bag3d.common.resources import DagsterDeployment, resource_defs
from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
)

from bag3d.core.asset_groups import (
    bgt_assets,
    input_assets,
    integration_data_assets,
    reconstruction_assets,
    source_assets,
)
from bag3d.core.jobs import (
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn6,
    job_ahn_metadata_index,
    job_ahn_tile_index,
    job_bgt,
    job_cbs,
    job_integration_data,
    job_preprocessing,
    job_reconstruct,
    job_reconstruct_debug,
    job_source_input,
)
from bag3d.core.schedules import integration_data_schedule
from bag3d.core.sensors import ahn_checksum_sensor

all_assets = [
    *bgt_assets,
    *source_assets,
    *input_assets,
    *reconstruction_assets,
    *integration_data_assets,
]

all_jobs = [
    job_bgt,
    job_cbs,
    job_source_input,
    job_preprocessing,
    job_ahn_tile_index,
    job_ahn3,
    job_ahn4,
    job_ahn5,
    job_ahn6,
    job_ahn_metadata_index,
    job_reconstruct,
    job_reconstruct_debug,
    job_integration_data,
]

sensor_status = (
    DefaultSensorStatus.RUNNING
    if getenv("DAGSTER_DEPLOYMENT") == DagsterDeployment.PRODUCTION
    else DefaultSensorStatus.STOPPED
)
all_sensors = [
    AutomationConditionSensorDefinition(
        "automation_condition_sensor",
        target=AssetSelection.all(),
        default_status=sensor_status,
    ),
    ahn_checksum_sensor(sensor_status),
]

defs = Definitions(
    resources=resource_defs,
    assets=all_assets,
    jobs=all_jobs,
    schedules=[integration_data_schedule],
    sensors=all_sensors,
)
