from pytest import mark

from dagster import (DagsterInstance, build_run_status_sensor_context, with_resources)

from bag3d_pipeline.simple_for_testing import job_testing, asset_testing, simple_docker
from bag3d_pipeline.resources import file_store, container
from bag3d_pipeline.resources.database import make_container_id
from bag3d_pipeline.jobs import sensor_source_and_input_sample
from bag3d_pipeline.core import get_run_id


@mark.skip("Setup and teardown too complex.")
def test_asset_testing_success_sensor(temp_file_store, docker_client):
    # execute the job
    instance = DagsterInstance.ephemeral()
    conf_file_store = file_store.configured({"data_dir": str(temp_file_store)})
    conf_cont = container.configured({})
    conf_sd = simple_docker.configured({"image_id": "busybox:latest"})
    ass_with_res = with_resources([asset_testing, ], {
        "file_store": conf_file_store,
        "container": conf_cont,
        "simple_docker": conf_sd
    })
    job_test_resolved = job_testing.resolve(assets=ass_with_res, source_assets=[])
    result = job_test_resolved.execute_in_process(instance=instance, run_config={})

    # retrieve the DagsterRun
    dagster_run = result.dagster_run

    # retrieve a success event from the completed execution
    dagster_event = result.get_job_success_event()

    # create the context
    run_status_sensor_context = build_run_status_sensor_context(
        sensor_name="source_and_input_sample_success",
        dagster_instance=instance,
        dagster_run=dagster_run,
        dagster_event=dagster_event,
    )

    # run the sensor
    sensor_source_and_input_sample(run_status_sensor_context)

    # clean up the container
    run_id = get_run_id(run_status_sensor_context, short=True)
    cont = docker_client.containers.get(make_container_id(run_id))
    cont.remove(force=True, v=True)
