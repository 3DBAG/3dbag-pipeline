from dagster import define_asset_job, AssetSelection


job_export = define_asset_job(
    name="export",
    description="Run the tyler export and 3D Tiles steps for the Netherlands. To be run after the floors_estimation package's jobs.",
    selection=AssetSelection.assets(["export", "feature_evaluation"])
    | AssetSelection.assets(["export", "export_index"])
    | AssetSelection.assets(["export", "metadata"])
    | AssetSelection.assets(["export", "reconstruction_output_cityjson"])
    | AssetSelection.assets(["export", "reconstruction_output_gpkg"])
    | AssetSelection.assets(["export", "reconstruction_output_obj"])
    | AssetSelection.assets(["export", "geopackage"])
    | AssetSelection.assets(["export", "compressed_tiles"])
    | AssetSelection.assets(["export", "compressed_tiles_validation"])
    | AssetSelection.assets(["export", "reconstruction_output_3dtiles_lod12"])
    | AssetSelection.assets(["export", "reconstruction_output_3dtiles_lod13"])
    | AssetSelection.assets(["export", "reconstruction_output_3dtiles_lod22"]),
)

job_deploy = define_asset_job(
    name="deploy",
    description="Deploy the Netherland data.",
    selection=AssetSelection.assets(["deploy", "compressed_export"])
    | AssetSelection.assets(["deploy", "transfer_to_publication"])
    | AssetSelection.assets(["deploy", "webservice_publication"]),
)


job_release = define_asset_job(
    name="release",
    description="Perform the final steps for the 3DBAG release.",
    selection=AssetSelection.assets(["release", "publish_data"])
    | AssetSelection.assets(["release", "publish_webservices"]),
)
