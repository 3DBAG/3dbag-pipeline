from dagster import asset, AssetIn, StaticPartitionsDefinition, get_dagster_logger

from bag3d_pipeline.assets.export.tile import get_tile_ids


class PartitionDefinition3DBagExport(StaticPartitionsDefinition):
    """Distribution tiles"""
    def __init__(self):
        logger = get_dagster_logger("PartitionDefinition3DBagExport")
        try:
            tile_ids = get_tile_ids()
        except:
            tile_ids = []
        super().__init__(partition_keys=sorted(list(tile_ids)))


@asset(
    partitions_def=PartitionDefinition3DBagExport(),
    ins={
        "reconstruction_output_multitiles_nl": AssetIn(key_prefix="export"),
    },
)
def party_walls_nl(context,
                   reconstruction_output_multitiles_nl,
                   ):
    """Party walls calculation from the exported CityJSON tiles."""
