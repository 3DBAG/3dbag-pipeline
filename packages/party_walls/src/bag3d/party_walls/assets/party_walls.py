from dagster import asset

from bag3d.common.utils.dagster import PartitionDefinition3DBagDistribution


@asset(
    partitions_def=PartitionDefinition3DBagDistribution(),
)
def party_walls_nl(context):
    """Party walls calculation from the exported CityJSON tiles."""
