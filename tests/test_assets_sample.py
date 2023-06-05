from dagster import build_op_context

from bag3d_pipeline.assets.sample import sample_data
from bag3d_pipeline.resources import file_store, container, docker_hub


def test_sample_data_image(temp_file_store, docker_client):
    test_repo = "test/3dbag-sample-data"
    test_tag = "test_sample_data_image"
    image_data_dir = "/tmp"
    container_id = "test_sample_data_image"
    # set up container that will be updated
    cont = docker_client.containers.run(
            command="sleep 3560d",
            image="busybox:latest", name=container_id, detach=True,
            remove=False
        )
    cont.exec_run(cmd="mkdir /data_container")

    context = build_op_context(
        op_config={"image_repository": test_repo,
                   "image_tag": test_tag,
                   "image_data_dir": image_data_dir,
                   "image_push": False},
        resources={
            "file_store": file_store.configured({"data_dir": str(temp_file_store),}),
            "container": container.configured({"id": container_id}),
            "docker_hub": docker_hub.configured({})
        }
    )
    with (context.resources.file_store.data_dir / "file.txt").open("w") as fo:
        fo.write("test data")
    (context.resources.file_store.data_dir / "test").mkdir()
    with (context.resources.file_store.data_dir / "test" / "file2.txt").open("w") as fo:
        fo.write("test data 2")

    sample_data.sample_data_image(context)

    stdout = docker_client.containers.run(image=f"{test_repo}:{test_tag}",
                                          command=["ls", image_data_dir],
                                          name=container_id, remove=True)
    # Remove image in any case
    docker_client.images.remove(image=f"{test_repo}:{test_tag}", force=True)
    # the 'sample_data_image' asset already removes the container so it doesn't need
    # to be removed anymore

    assert "file.txt" in stdout.decode("utf-8")
