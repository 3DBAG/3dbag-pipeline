import os
from dagster import repository, with_resources, Definitions

from bag3d_pipeline.assets import (source_assets, input_assets, sample_data_assets,
                                   reconstruction_assets, export_assets, deploy_assets)
from bag3d_pipeline.resources import RESOURCES_LOCAL, RESOURCES_PROD, RESOURCES_PYTEST
from bag3d_pipeline.jobs import (job_sample_data, sensor_sample_data,
                                 sensor_clean_containers, job_clean_containers,
                                 job_ahn3, job_ahn4, job_zuid_holland_reconstruct,
                                 job_zuid_holland_export, job_zuid_holland_deploy)
from bag3d_pipeline.simple_for_testing import (asset_testing, job_testing, test_table1,
                                               test_table2)

all_assets = [
    *source_assets,
    *input_assets,
    asset_testing,
    test_table1,
    test_table2,
    *sample_data_assets,
    *reconstruction_assets,
    *export_assets,
    *deploy_assets
]

all_jobs = [
    job_testing,
    job_sample_data,
    job_clean_containers,
    job_ahn3,
    job_ahn4,
    job_zuid_holland_reconstruct,
    job_zuid_holland_export,
    job_zuid_holland_deploy,
    sensor_sample_data,
    sensor_clean_containers
]

resource_defs_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
    "pytest": RESOURCES_PYTEST
}


@repository
def bag3d():
    """The complete 3D BAG pipeline, from source ingestion to quality control."""
    deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
    resource_defs = resource_defs_by_deployment_name[deployment_name]

    definitions = [
        with_resources(all_assets, resource_defs),
        *all_jobs
    ]

    # If we need to set up some alerting for production, we can add the sensor
    # definition like this:
    # if deployment_name == "prod":
    #     definitions.append(sensor_for_alerting)

    return definitions
