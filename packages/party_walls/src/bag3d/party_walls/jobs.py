from dagster import define_asset_job, AssetSelection

job_nl_party_walls = define_asset_job(
    name="nl_party_walls",
    description="Run the tyler export and 3D Tiles steps for the Netherlands.",
    selection=AssetSelection.keys(["party_walls", "party_walls_nl"]) |
              AssetSelection.keys(
                  ["party_walls", "cityjsonfeatures_with_party_walls_nl"])
)
