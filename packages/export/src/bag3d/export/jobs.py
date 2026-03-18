from dagster import define_asset_job, AssetSelection


job_nl_export = define_asset_job(
    name="nl_export",
    description="Run the tyler export and 3D Tiles steps for the Netherlands. To be run after the floors_estimation package's jobs.",
    selection=AssetSelection.assets(["export", "feature_evaluation"])
    | AssetSelection.assets(["export", "export_index"])
    | AssetSelection.assets(["export", "metadata"])
    | AssetSelection.assets(["export", "reconstruction_output_multitiles_nl"])
    | AssetSelection.assets(["export", "geopackage_nl"])
    | AssetSelection.assets(["export", "compressed_tiles"])
    | AssetSelection.assets(["export", "compressed_tiles_validation"])
    | AssetSelection.assets(["export", "reconstruction_output_3dtiles_lod12_nl"])
    | AssetSelection.assets(["export", "reconstruction_output_3dtiles_lod13_nl"])
    | AssetSelection.assets(["export", "reconstruction_output_3dtiles_lod22_nl"]),
)

job_nl_deploy = define_asset_job(
    name="nl_deploy",
    description="Deploy the Netherland data.",
    selection=AssetSelection.assets(["deploy", "compressed_export_nl"])
    | AssetSelection.assets(["deploy", "transfer_to_publication"])
    | AssetSelection.assets(["deploy", "webservice_publication"]),
)


job_nl_release = define_asset_job(
    name="nl_release",
    description="Perform the final steps for the 3DBAG release.",
    selection=AssetSelection.assets(["release", "publish_data"])
    | AssetSelection.assets(["release", "publish_webservices"]),
)
