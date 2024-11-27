from dagster import define_asset_job, AssetSelection


job_ahn_tile_index = define_asset_job(
    name="ahn_tile_index",
    description="Get the tile index (bladwijzer), md5 hashes of the LAZ files and "
    "create the tables for storing the metadata for AHN 3, 4 and 5, so that "
    "the AHN jobs can be run.",
    selection=AssetSelection.assets(["ahn", "tile_index_ahn"])
    | AssetSelection.assets(["ahn", "md5_ahn3"])
    | AssetSelection.assets(["ahn", "md5_ahn4"])
    | AssetSelection.assets(["ahn", "sha256_ahn5"])
    | AssetSelection.assets(["ahn", "metadata_table_ahn3"])
    | AssetSelection.assets(["ahn", "metadata_table_ahn4"])
    | AssetSelection.assets(["ahn", "metadata_table_ahn5"]),
)

# WARNING!!! multi_assets don't have key_prefix, https://github.com/dagster-io/dagster/issues/9344
job_ahn3 = define_asset_job(
    name="ahn3",
    description="Make sure that the available AHN 3 LAZ files are present on disk, "
    "and their metadata is recorded.",
    selection=AssetSelection.assets(["ahn", "laz_files_ahn3"])
    | AssetSelection.assets(["ahn", "metadata_ahn3"])
    | AssetSelection.assets(["ahn", "lasindex_ahn3"]),
)

job_ahn4 = define_asset_job(
    name="ahn4",
    description="Make sure that the available AHN 4 LAZ files are present on disk, "
    "and their metadata is recorded.",
    selection=AssetSelection.assets(["ahn", "laz_files_ahn4"])
    | AssetSelection.assets(["ahn", "metadata_ahn4"])
    | AssetSelection.assets(["ahn", "lasindex_ahn4"]),
)

job_ahn5 = define_asset_job(
    name="ahn5",
    description="Make sure that the available AHN 5 LAZ files are present on disk, "
    "and their metadata is recorded.",
    selection=AssetSelection.assets(["ahn", "laz_files_ahn5"])
    | AssetSelection.assets(["ahn", "metadata_ahn5"])
    | AssetSelection.assets(["ahn", "lasindex_ahn5"]),
)

job_ahn_tiles_200m = define_asset_job(
    name="ahn_tiles_200m",
    description="Tile the AHN LAZ files into 200m tiles.",
    selection=AssetSelection.assets(["ahn", "regular_grid_200m"])
    | AssetSelection.assets(["ahn", "laz_tiles_ahn3_200m"])
    | AssetSelection.assets(["ahn", "laz_tiles_ahn4_200m"])
    | AssetSelection.assets(["ahn", "laz_tiles_ahn5_200m"]),
)

job_source_input = define_asset_job(
    name="source_input",
    description="Update the source data sets and prepare the input for the reconstruction.",
    selection=AssetSelection.assets(["bag", "extract_bag"])
    | AssetSelection.assets(["bag", "stage_bag_pand"])
    | AssetSelection.assets(["bag", "bag_pandactueelbestaand"])
    | AssetSelection.groups("top10nl")
    | AssetSelection.groups("input"),
)

job_nl_reconstruct = define_asset_job(
    name="nl_reconstruct",
    description="Run the crop and reconstruct steps for the Netherlands.",
    selection=AssetSelection.assets(
        ["reconstruction", "reconstructed_building_models_nl"]
    ),
)

job_nl_export = define_asset_job(
    name="nl_export",
    description="Run the tyler export and 3D Tiles steps for the Netherlands.",
    selection=AssetSelection.assets(["export", "feature_evaluation"])
    | AssetSelection.assets(["export", "export_index"])
    | AssetSelection.assets(["export", "metadata"])
    | AssetSelection.assets(["export", "geopackage_nl"])
    | AssetSelection.assets(["export", "reconstruction_output_multitiles_nl"]),
)

job_nl_deploy = define_asset_job(
    name="nl_deploy",
    description="Deploy the Netherland data.",
    selection=AssetSelection.assets(["export", "compressed_tiles"])
    | AssetSelection.assets(["export", "compressed_tiles_validation"])
    | AssetSelection.assets(["deploy", "compressed_export_nl"])
    | AssetSelection.assets(["deploy", "downloadable_godzilla"])
    | AssetSelection.assets(["deploy", "webservice_godzilla"]),
)
