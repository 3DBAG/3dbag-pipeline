from dagster import build_op_context

from bag3d_pipeline.assets.input import intermediary
from bag3d_pipeline.custom_types import PostgresTableIdentifier


def test_bag_kas_warenhuis(resource_db_connection_docker, resource_container):
    """Does the complete asset work?"""
    context = build_op_context(
        resources={"container": resource_container,
                   "db_connection": resource_db_connection_docker}
    )
    bag_pandactueelbestaand = PostgresTableIdentifier("lvbag", "pandactueelbestaand")
    top10nl_gebouw = PostgresTableIdentifier("top10nl", "gebouw")
    res = intermediary.bag_kas_warenhuis(context, bag_pandactueelbestaand,
                                         top10nl_gebouw)
    assert res.value.exists()
    context.resources.file_store.rm(force=True)
