"""
The 'simple_for_testing' sub-package has very minimal things that
run quickly without big dependencies, and help in developing some new concept,
workflow, logic etc.
"""

import docker
from docker.errors import ImageNotFound
from dagster import (resource, asset, AssetSelection, AssetIn, define_asset_job, Output)

from bag3d_pipeline.core import load_sql, postgrestable_from_query
from bag3d_pipeline.custom_types import PostgresTableIdentifier


class SimpleDocker:
    def __init__(self, image_id, container_id):
        self.docker_client = docker.from_env()
        try:
            self.docker_image = self.docker_client.images.get(image_id)
        except ImageNotFound:
            self.docker_image = self.docker_client.images.pull(image_id)
        container = self.docker_client.containers.run(
            command="sleep 3560d",
            image=self.docker_image, name=container_id, detach=True,
            remove=False
        )
        container.exec_run(cmd="mkdir /data_container")


@resource(
    config_schema={
        "image_id": str,
    },
    required_resource_keys={"container"}
)
def simple_docker(context):
    return SimpleDocker(
        image_id=context.resource_config["image_id"],
        container_id=context.resources.container.id
    )


conf_simpl_dock = simple_docker.configured(
    {"image_id": "balazsdukai/3dbag-sample-data:latest"})


@asset(
    key_prefix="test",
    group_name="test",
    config_schema={
        "geofilter": str
    },
    required_resource_keys={"file_store", "simple_docker"}
)
def asset_testing(context):
    context.log.info(f"geofilter: {context.op_config['geofilter']}")
    with (context.resources.file_store.data_dir / "file.txt").open("w") as fo:
        fo.write("test data")
    (context.resources.file_store.data_dir / "test").mkdir()
    with (context.resources.file_store.data_dir / "test" / "file2.txt").open("w") as fo:
        fo.write("test data 2")
    context.log.info(f"written files to {context.resources.file_store.data_dir}")


job_testing = define_asset_job(
    name="job_testing",
    selection=AssetSelection.assets(asset_testing),
    config={
        "ops": {
            "test__asset_testing": {
                "config": {"geofilter": "some Polygon"}},
        },
    }
)


@asset(
    key_prefix="test_db",
    group_name="test_db",
    required_resource_keys={"db_connection"},
    op_tags={"kind": "sql"}
)
def test_table1(context):
    query = load_sql()
    tbl = PostgresTableIdentifier("public", "table1")
    metadata = postgrestable_from_query(context, query, tbl)
    return Output(tbl, metadata=metadata)


@asset(
    key_prefix="test_db",
    group_name="test_db",
    required_resource_keys={"db_connection"},
    op_tags={"kind": "sql"},
    ins={
        "test_table1": AssetIn(key_prefix="test_db")
    }
)
def test_table2(context, test_table1: PostgresTableIdentifier):
    query = load_sql(query_params={"tbl": test_table1})
    tbl = PostgresTableIdentifier("public", "table2")
    metadata = postgrestable_from_query(context, query, tbl)
    return Output(tbl, metadata=metadata)
