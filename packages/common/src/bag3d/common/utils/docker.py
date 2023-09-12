import docker
from dagster import (op, Field)


@op(config_schema={"remove_file_store": Field(bool, default_value=False)},
    required_resource_keys={"container", "file_store"})
def clean_storage(context):
    """Remove docker containers and associated volumes, and the 'file_store'."""
    docker_client = docker.from_env()
    container = docker_client.containers.get(context.resources.container.id)
    container.remove(force=True, v=True)
    context.log.info(f"Removed container {context.resources.container.id}")
    if context.op_config["remove_file_store"]:
        context.resources.file_store.data_dir.unlink()
        context.log.info(
            f"Removed local directory {context.resources.file_store.data_dir}")
