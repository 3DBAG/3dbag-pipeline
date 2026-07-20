from dagster import AssetSelection, define_asset_job

job_floors_estimation = define_asset_job(
    name="floors_estimation",
    description="""Estimate the number of floors per building and write the
    results back to the CityJSONFeatures.""",
    selection=AssetSelection.assets(["floors_estimation", "features_file_index"])
    | AssetSelection.assets(["floors_estimation", "external_features"])
    | AssetSelection.assets(["floors_estimation", "bag3d_features"])
    | AssetSelection.assets(["floors_estimation", "all_features"])
    | AssetSelection.assets(["floors_estimation", "preprocessed_features"])
    | AssetSelection.assets(["floors_estimation", "inferenced_floors"])
    | AssetSelection.assets(["floors_estimation", "save_cjfiles"]),
)

job_import_training_data = define_asset_job(
    name="import_training_data",
    description="Import floor count training datasets from Den Haag, Rotterdam, "
    "Amsterdam and Rijssen-Holten into the floors_estimation schema.",
    selection=AssetSelection.assets(
        [
            "floors_estimation",
            "import_training_denhaag_woonhuizen",
        ]
    )
    | AssetSelection.assets(
        ["floors_estimation", "import_training_denhaag_galerijflats"]
    )
    | AssetSelection.assets(["floors_estimation", "import_training_rotterdam"])
    | AssetSelection.assets(["floors_estimation", "import_training_amsterdam"])
    | AssetSelection.assets(["floors_estimation", "import_training_rijssen_holten"]),
)
