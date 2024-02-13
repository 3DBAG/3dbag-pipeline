from dagster import AssetSelection, define_asset_job

job_floors_estimation = define_asset_job(
    name="floors_estimation",
    description="""Estimate the number of floors per building and write the
    results back to the CityJSONFeatures.""",
    selection=AssetSelection.keys(["floors_estimation", "extract_external_features"])
    | AssetSelection.keys(["floors_estimation", "extract_3DBAG_features"])
    | AssetSelection.keys(["floors_estimation", "create_building_features_table"]),
)
