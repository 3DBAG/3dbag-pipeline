from dagster import define_asset_job, AssetSelection

job_party_walls_index = define_asset_job(
    name="party_walls_index",
    description="Generate the indices that are required for running the party_walls job",
    selection=AssetSelection.assets(["party_walls", "features_file_index"]),
)

job_party_walls = define_asset_job(
    name="party_walls",
    description="""Compute the party walls from the reconstructed features using
    bag3d-surfaces shared_walls() per building, driven by the BAG adjacency index.
    Writes CityJSONFeature files to stages/party_walls/{tile_id}/.
    """,
    selection=AssetSelection.assets(["party_walls", "building_surfaces"]),
)
