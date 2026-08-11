from dagster import define_asset_job, AssetSelection

job_party_walls = define_asset_job(
    name="party_walls",
    description="""Compute the party walls from the reconstructed features using
    bag3d-surfaces shared_walls() per building, driven by the BAG adjacency index.
    Reads indexed CityJSONSeq data and writes one strict CityJSONSeq file per tile
    to stages/party_walls/{tile_id}/.
    """,
    selection=AssetSelection.assets(["party_walls", "building_surfaces"]),
)
