import tarfile

from dagster import (asset, Field, Output, UrlMetadataValue)
import docker


@asset(
    config_schema={
        "image_repository": str,
        "image_tag": str,
        "image_data_dir": Field(
            str, description="Directory in the docker image where the data will be "
                             "uploaded."),
        "image_push": Field(
            bool, default_value=False,
            description="Push the new image to registry (DockerHub) or no."
        )
    },
    required_resource_keys={"file_store", "container", "docker_hub"},
)
def sample_data_image(context):
    """Save a docker image with the sample data included, by copying the data files
    from the 'file_store.data_dir' into the docker container and committing the
    container.
    If `image_push` is True, it pushes the new image to DockerHub.
    """
    # We can only copy tar-archives into containers with the python SDK
    tar_name = "data.tar"

    def _exclude_archive(tarinfo):
        return None if tarinfo.name == tar_name else tarinfo

    with tarfile.open(context.resources.file_store.data_dir / tar_name, "w") as to:
        for f in context.resources.file_store.data_dir.iterdir():
            to.add(f, arcname=f.relative_to(context.resources.file_store.data_dir),
                   filter=_exclude_archive)

    docker_client = docker.from_env()
    container = docker_client.containers.get(
        context.resources.container.id)
    with (context.resources.file_store.data_dir / tar_name).open("rb") as to:
        container.put_archive(path=context.op_config["image_data_dir"], data=to.read())

    new_repo = context.op_config["image_repository"]
    new_tag = context.op_config["image_tag"]
    new_image_id = f'{new_repo}:{new_tag}'
    container.commit(repository=new_repo, tag=new_tag)
    new_image = docker_client.images.get(new_image_id)
    new_image.tag(repository=new_repo, tag="latest")

    # clean up
    container.remove(force=True, v=True)
    (context.resources.file_store.data_dir / tar_name).unlink()

    # push the image
    if context.op_config["image_push"]:
        auth_config = {
            "username": context.resources.docker_hub.username,
            "password": context.resources.docker_hub.password
        }
        # push the new_tag
        msg = []
        error = None
        for line in docker_client.images.push(repository=new_repo, tag=new_tag,
                                              stream=True, decode=True,
                                              auth_config=auth_config):
            if "errorDetail" in line:
                error = ", ".join(f"{k}: {v}" for k, v in line.items())
            msg.append(", ".join(f"{k}: {v}" for k, v in line.items()))
        context.log.debug("\n".join(msg))
        if error:
            context.log.error(f"Failed to push the image {new_image_id}: {error}")

        # push 'latest'
        msg = []
        error = None
        for line in docker_client.images.push(repository=new_repo, tag="latest",
                                              stream=True, decode=True,
                                              auth_config=auth_config):
            if "errorDetail" in line:
                error = ", ".join(f"{k}: {v}" for k, v in line.items())
            msg.append(", ".join(f"{k}: {v}" for k, v in line.items()))
        context.log.debug("\n".join(msg))
        if error:
            context.log.error(f"Failed to push the image {new_repo}:'latest': {error}")

    return Output(
        [new_image_id, f'{new_repo}:latest'],
        metadata={
            "repository": new_repo,
            "tags": ",".join([new_tag, "latest"]),
            "id": new_image.attrs["Id"].split(":")[1],
            "size [Mb]": round(new_image.attrs["Size"] / 1e6, 3),
            "url": UrlMetadataValue(
                "https://hub.docker.com/r/balazsdukai/3dbag-sample-data/tags")
        }
    )
