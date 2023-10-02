from dagster import define_asset_job, AssetSelection

from bag3d.common.utils.dagster import PartitionDefinition3DBagDistribution

job_nl_party_walls = define_asset_job(
    name="nl_party_walls",
    description="""Compute the party walls from the reconstructed models and write the 
    results back to the CityJSONFeatures.
    It reads the uncompressed CityJSON tiles that were created with *tyler*, and 
    writes CityJSONFeature files. The new features need to be run through *tyler* again, 
    to generate the tiles for export.
    """,
    selection=AssetSelection.keys(["party_walls", "party_walls_nl"]) |
              AssetSelection.keys(
                  ["party_walls", "cityjsonfeatures_with_party_walls_nl"]),
    partitions_def=PartitionDefinition3DBagDistribution()
)
