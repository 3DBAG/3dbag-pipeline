from dagster import AssetSelection, define_asset_job

job_floors_estimation = define_asset_job(
    name="floors_estimation",
    description="""Estimate the number of floors per building and write the
    results back to the CityJSONFeatures.""",
    selection=AssetSelection.keys(["floors_estimation", "features_file_index"])
    | AssetSelection.keys(["floors_estimation", "external_features"])
    | AssetSelection.keys(["floors_estimation", "bag3d_features"])
    | AssetSelection.keys(["floors_estimation", "all_features"])
    | AssetSelection.keys(["floors_estimation", "preprocessed_features"])
    | AssetSelection.keys(["floors_estimation", "inferenced_floors"])
    | AssetSelection.keys(["floors_estimation", "save_cjfiles"])
    ,
)
