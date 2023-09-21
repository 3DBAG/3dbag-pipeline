import os
from datetime import datetime

from dagster import (define_asset_job, AssetSelection, run_status_sensor,
                     DagsterRunStatus, RunRequest, SkipReason, config_mapping, job,
                     StaticPartitionsDefinition)

from bag3d.common.resources.database import make_container_id, container
from bag3d.common.resources.files import make_temp_path, file_store
from bag3d.common.resources.wkt import ZUID_HOLLAND
from bag3d.common.utils.docker import clean_storage
from bag3d.common.utils.dagster import get_run_id
from bag3d.core.simple_for_testing import job_testing
from bag3d.core.assets.reconstruction.reconstruction import (
    PartitionDefinition3DBagReconstruction
)
from bag3d.core.assets.reconstruction import RECONSTRUCT_RERUN_INPUT_PARTITIONS
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA


@config_mapping(config_schema={
    "geofilter": str,
})
def config_3dbag_pipeline(val):
    """Configure the pipeline to produce the sample data in the docker image
    3dbag-sample-data. The docker image only contains the data, does not contain
    dagster."""
    return {
        "ops": {
            "extract_top10nl": {
                "config": {"geofilter": val["geofilter"]}},
            "stage_bag_woonplaats": {
                "config": {"geofilter": val["geofilter"]}},
            "stage_bag_verblijfsobject": {
                "config": {"geofilter": val["geofilter"]}},
            "stage_bag_pand": {
                "config": {"geofilter": val["geofilter"]}},
            "stage_bag_openbareruimte": {
                "config": {"geofilter": val["geofilter"]}},
            "stage_bag_nummeraanduiding": {
                "config": {"geofilter": val["geofilter"]}},
        }
    }


config_sample = config_3dbag_pipeline.resolve_from_validated_config(
    {
        "geofilter": ("Polygon ((136251.531 456118.126, 136620.128 456118.126, "
                      "136620.128 456522.218, 136251.531 456522.218, "
                      "136251.531 456118.126))"),
    }
)

sample_job_name = "sample_data_produce"
job_sample_data = define_asset_job(
    name=sample_job_name,
    description="Produce the sample data for developing the 3D BAG pipeline. The sample"
                " data is in the area of `Polygon ((136251.531 456118.126, 136620.128 "
                "456118.126, 136620.128 456522.218, 136251.531 456522.218, 136251.531 "
                "456118.126))`. The job assets are produced in a container of the "
                "`3dbag-sample-data` docker image.",
    selection=AssetSelection.groups("bag_top10nl") |
              AssetSelection.groups("input"),
    # config=config_sample,
)

assets_sample_data = AssetSelection.groups("sample")
job_sample_data_image = define_asset_job(
    name="sample_data_update_image",
    description="Update the 3dbag-sample-data docker image with new sample data.",
    selection=assets_sample_data
)

job_bgt = define_asset_job(
    name="bgt",
    description="Load the latest BGT Pand, Wegdeel layers.",

)

# WARNING!!! multi_assets don't have key_prefix, https://github.com/dagster-io/dagster/issues/9344
job_ahn3 = define_asset_job(
    name="ahn3",
    description="Make sure that the available AHN 3 LAZ files are present on disk, "
                "and their metadata is recorded.",
    selection=AssetSelection.keys(["ahn", "laz_files_ahn3"]) |
              AssetSelection.keys(["ahn", "metadata_ahn3"]) |
              AssetSelection.keys(["ahn", "lasindex_ahn3"])
)

job_ahn4 = define_asset_job(
    name="ahn4",
    description="Make sure that the available AHN 4 LAZ files are present on disk, "
                "and their metadata is recorded.",
    selection=AssetSelection.keys(["ahn", "md5_pdok_ahn4"]) |
              AssetSelection.keys(["ahn", "metadata_table_ahn4"]) |
              AssetSelection.keys(["ahn", "tile_index_ahn4_pdok"]) |
              AssetSelection.keys(["ahn", "laz_files_ahn4"]) |
              AssetSelection.keys(["ahn", "metadata_ahn4"]) |
              AssetSelection.keys(["ahn", "lasindex_ahn4"])
)

job_source_input = define_asset_job(
    name="source_input",
    description="Update the source data sets and prepare the input for the reconstruction.",
    selection=AssetSelection.groups("bag") |
              AssetSelection.groups("top10nl") |
              AssetSelection.groups("input"),
)

job_nl_reconstruct = define_asset_job(
    name="nl_reconstruct",
    description="Run the crop and reconstruct steps for the Netherlands.",
    selection=AssetSelection.keys(["reconstruction", "cropped_input_and_config_nl"]) |
              AssetSelection.keys(
                  ["reconstruction", "reconstructed_building_models_nl"]),
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles"
    ),
)

job_nl_export = define_asset_job(
    name="nl_export",
    description="Run the tyler export and 3D Tiles steps for the Netherlands.",
    selection=AssetSelection.keys(["export", "feature_evaluation"]) |
              AssetSelection.keys(["export", "export_index"]) |
              AssetSelection.keys(["export", "metadata"]) |
              AssetSelection.keys(["export", "geopackage_nl"]) |
              AssetSelection.keys(["export", "reconstruction_output_multitiles_nl"]),
)

job_nl_deploy = define_asset_job(
    name="nl_deploy",
    description="Deploy the Netherland data.",
    selection=AssetSelection.keys(["export", "compressed_tiles"]) |
              AssetSelection.keys(["export", "validate_compressed_files"]) |
              AssetSelection.keys(["deploy", "compressed_export_nl"]) |
              AssetSelection.keys(["deploy", "downloadable_godzilla"]) |
              AssetSelection.keys(["deploy", "webservice_godzilla"]),
)

# @run_status_sensor(
#     run_status=DagsterRunStatus.SUCCESS,
#     name=f"nl_export_deploy_sensor",
#     description=f"Run the nl_export_deploy job if the nl_reconstruct succeeded.",
#     monitored_jobs=[job_nl_reconstruct, ],
#     request_job=job_nl_export_deploy,
#     minimum_interval_seconds=300
# )
# def sensor_nl_export_deploy(context):
#     return RunRequest(run_key="nl-2")


job_zuid_holland_reconstruct = define_asset_job(
    name="zuid_holland_reconstruct",
    description="Run the crop and reconstruct steps for the province of Zuid-Holland.",
    selection=AssetSelection.keys(
        ["reconstruction", "cropped_input_and_config_zuid_holland"]) |
              AssetSelection.keys(
                  ["reconstruction", "reconstructed_building_models_zuid_holland"]),
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles", wkt=ZUID_HOLLAND
    ),
)

job_zuid_holland_export = define_asset_job(
    name="zuid_holland_export",
    description="Run the tyler export and 3D Tiles steps for the province of "
                "Zuid-Holland.",
    selection=AssetSelection.keys(["export", "feature_evaluation"]) |
              AssetSelection.keys(["export", "export_index"]) |
              AssetSelection.keys(["export", "metadata"]) |
              AssetSelection.keys(["export", "geopackage_nl"]) |
              AssetSelection.keys(
                  ["export", "reconstruction_output_multitiles_zuid_holland"]) |
              AssetSelection.keys(
                  ["export", "reconstruction_output_3dtiles_zuid_holland"]),
)

job_zuid_holland_deploy = define_asset_job(
    name="zuid_holland_deploy",
    description="Deploy the Zuid-Holland data.",
    selection=AssetSelection.keys(["deploy", "compressed_export_zuid_holland"]) |
              AssetSelection.keys(["deploy", "downloadable_godzilla"]) |
              AssetSelection.keys(["deploy", "webservice_godzilla"]),
)

job_zuid_holland_export_deploy = define_asset_job(
    name="zuid_holland_export_deploy",
    description="Run the tyler export and 3D Tiles and deploy steps for the province of "
                "Zuid-Holland.",
    selection=AssetSelection.keys(["export", "feature_evaluation"]) |
              AssetSelection.keys(["export", "export_index"]) |
              AssetSelection.keys(["export", "metadata"]) |
              AssetSelection.keys(["export", "geopackage_nl"]) |
              AssetSelection.keys(
                  ["export", "reconstruction_output_multitiles_zuid_holland"]) |
              AssetSelection.keys(["deploy", "compressed_export_zuid_holland"]) |
              AssetSelection.keys(["deploy", "downloadable_godzilla"]) |
              AssetSelection.keys(["deploy", "webservice_godzilla"]),
)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name=f"{sample_job_name}_on_success",
    description=f"Run when the {sample_job_name} job succeeds and update the "
                f"3dbag-sample-data docker image with the generated sample data."
                f"If DAGSTER_DEPLOYMENT='prod', it also pushes the new image to "
                f"Docker Hub.",
    monitored_jobs=[job_sample_data, job_testing, ],
    request_job=job_sample_data_image
)
def sensor_sample_data(context):
    # The container and the temp dir in the monitored job were created with the
    # Run Id of the monitored job. In order to find the container the dir path, we
    # need the Run Id of the monitored job.
    deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
    if deployment_name == "pytest":
        base_image = "busybox:latest"
    else:
        base_image = "balazsdukai/3dbag-sample-data"
    do_push = True if deployment_name == "prod" else False

    run_id = get_run_id(context, short=True)
    container_id = make_container_id(run_id)
    temp_path = make_temp_path(run_id)
    image_tag = f"{datetime.today().date().isoformat()}-{run_id}"
    if deployment_name in ["local", "pytest"]:
        image_tag += "-dev"

    if context.dagster_run.job_name != job_sample_data_image.name:
        run_config = {
            "ops": {
                "sample_data_image": {
                    "config": {"image_repository": base_image,
                               "image_tag": image_tag,
                               "image_data_dir": "/data_container",
                               "image_push": do_push}}
            },
            "resources": {
                "file_store": {"config": {"data_dir": temp_path}},
                "container": {"config": {"id": container_id}},
                "docker_hub": {}
            }
        }
        return RunRequest(run_key=None, run_config=run_config)
    else:
        return SkipReason("Don't report status of job_sample_data_image")


@job(
    name="clean_containers",
    description="Remove docker containers and associated volumes.",
    resource_defs={"file_store": file_store, "container": container}
)
def job_clean_containers():
    clean_storage()


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    name=f"clean_containers_on_failure",
    description=f"Remove docker containers on failed jobs.",
    monitored_jobs=[job_sample_data, job_testing, ],
    request_job=job_clean_containers
)
def sensor_clean_containers(context):
    # The container and the temp dir in the monitored job were created with the
    # Run Id of the monitored job. In order to find the container the dir path, we
    # need the Run Id of the monitored job.
    run_id = get_run_id(context, short=True)
    container_id = make_container_id(run_id)
    temp_path = make_temp_path(run_id)
    # Probably we should not ever remove the file directory. It is too dangerous,
    # because we can easily remove the prodouction file dir by accident.
    remove_files = False

    if context.dagster_run.job_name != job_clean_containers.name:
        run_config = {
            "ops": {
                "clean_storage": {
                    "config": {"remove_file_store": remove_files}}
            },
            "resources": {
                "file_store": {"config": {"data_dir": temp_path}},
                "container": {"config": {"id": container_id}},
            }
        }
        return RunRequest(run_key=None, run_config=run_config)
    else:
        return SkipReason("Don't report status of job_sample_data_image")
