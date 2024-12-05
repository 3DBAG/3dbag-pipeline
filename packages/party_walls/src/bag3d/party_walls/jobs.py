from dagster import define_asset_job, AssetSelection

job_nl_party_walls_index = define_asset_job(
    name="nl_party_walls_index",
    description="Generate the indices that are required for running the party_walls job",
    selection=AssetSelection.assets(["party_walls", "distribution_tiles_files_index"])
    | AssetSelection.assets(["party_walls", "features_file_index"]),
)

job_nl_party_walls = define_asset_job(
    name="nl_party_walls",
    description="""Compute the party walls from the reconstructed models and write the 
    results back to the CityJSONFeatures.
    It reads the uncompressed CityJSON tiles that were created with *tyler*, and 
    writes CityJSONFeature files. The new features need to be run through *tyler* again, 
    to generate the tiles for export.
    """,
    selection=AssetSelection.assets(["party_walls", "party_walls_nl"])
    | AssetSelection.assets(["party_walls", "cityjsonfeatures_with_party_walls_nl"]),
)
