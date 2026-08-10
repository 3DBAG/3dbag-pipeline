from dagster import define_asset_job, AssetSelection


job_bgt = define_asset_job(
    name="bgt",
    description="Load the latest BGT Pand layer.",
    selection=AssetSelection.groups("bgt"),
)

job_cbs = define_asset_job(
    name="cbs",
    description="Download and load CBS data (key figures, buurtkaart, address mapping).",
    selection=AssetSelection.groups("cbs"),
)

job_ahn_tile_index = define_asset_job(
    name="ahn_tile_index",
    description="Get the tile index (bladwijzer), md5 hashes of the LAZ files and "
    "create the tables for storing the metadata for AHN 3, 4 and 5, so that "
    "the AHN jobs can be run.",
    selection=AssetSelection.assets(["ahn", "tile_index_ahn"])
    | AssetSelection.assets(["ahn", "tile_index_ahn6"])
    | AssetSelection.assets(["ahn", "md5_ahn3"])
    | AssetSelection.assets(["ahn", "md5_ahn4"])
    | AssetSelection.assets(["ahn", "sha256_ahn5"])
    | AssetSelection.assets(["ahn", "sha256_ahn6"])
    | AssetSelection.assets(["ahn", "metadata_table_ahn3"])
    | AssetSelection.assets(["ahn", "metadata_table_ahn4"])
    | AssetSelection.assets(["ahn", "metadata_table_ahn5"])
    | AssetSelection.assets(["ahn", "metadata_table_ahn6"]),
)

# WARNING!!! multi_assets don't have key_prefix, https://github.com/dagster-io/dagster/issues/9344
job_ahn3 = define_asset_job(
    name="ahn3",
    description="Make sure that the available AHN3 LAZ files are present on disk, "
    "and their metadata is recorded.",
    selection=AssetSelection.assets(["ahn", "laz_files_ahn3"])
    | AssetSelection.assets(["ahn", "metadata_ahn3"])
    | AssetSelection.assets(["ahn", "lasindex_ahn3"]),
)

job_ahn4 = define_asset_job(
    name="ahn4",
    description="Make sure that the available AHN4 LAZ files are present on disk, "
    "and their metadata is recorded.",
    selection=AssetSelection.assets(["ahn", "laz_files_ahn4"])
    | AssetSelection.assets(["ahn", "metadata_ahn4"])
    | AssetSelection.assets(["ahn", "lasindex_ahn4"]),
)

job_ahn5 = define_asset_job(
    name="ahn5",
    description="Make sure that the available AHN5 LAZ files are present on disk, "
    "and their metadata is recorded.",
    selection=AssetSelection.assets(["ahn", "laz_files_ahn5"])
    | AssetSelection.assets(["ahn", "metadata_ahn5"])
    | AssetSelection.assets(["ahn", "lasindex_ahn5"]),
)


job_ahn6 = define_asset_job(
    name="ahn6",
    description="Make sure that the available AHN6 COPC.LAZ files in 1x1 km grid "
    "tiles are present on disk, and their metadata is recorded.",
    selection=AssetSelection.assets(["ahn", "laz_files_ahn6"])
    | AssetSelection.assets(["ahn", "metadata_ahn6"]),
)


job_ahn_metadata_index = define_asset_job(
    name="ahn_metadata_index",
    description="Creates indices on the AHN metadata tables",
    selection=AssetSelection.assets(["ahn", "metadata_ahn3_index"])
    | AssetSelection.assets(["ahn", "metadata_ahn4_index"])
    | AssetSelection.assets(["ahn", "metadata_ahn5_index"])
    | AssetSelection.assets(["ahn", "metadata_ahn6_index"]),
)


job_source_input = define_asset_job(
    name="source_input",
    description="Update the source data sets and prepare the input for the reconstruction.",
    selection=AssetSelection.assets(["bag", "extract_bag"])
    | AssetSelection.assets(["bag", "stage_bag_pand"])
    | AssetSelection.assets(["bag", "bag_pandactueelbestaand"])
    | AssetSelection.assets(["bag", "stage_bag_verblijfsobject"])
    | AssetSelection.assets(["bag", "bag_verblijfsobjectactueelbestaand"])
    | AssetSelection.groups("cbs")
    | AssetSelection.groups("top10nl")
    | AssetSelection.groups("input"),
)

job_reconstruct = define_asset_job(
    name="reconstruct",
    description="Run the crop and reconstruct steps for the Netherlands.",
    selection=AssetSelection.assets(
        ["reconstruction", "reconstructed_building_models"]
    ),
)

job_reconstruct_debug = define_asset_job(
    name="reconstruct_debug",
    description="Run the crop and reconstruct steps for the Netherlands with debug info.",
    selection=AssetSelection.assets(
        ["reconstruction", "reconstructed_building_models"]
    ),
)


job_integration_data = define_asset_job(
    name="integration_data",
    description="Refresh the AOI-specific native-format integration-data fixtures.",
    selection=AssetSelection.groups("integration_data")
    | AssetSelection.assets(["bag", "extract_bag"])
    | AssetSelection.assets(["bgt", "extract_bgt"])
    | AssetSelection.assets(["top10nl", "extract_top10nl"])
    | AssetSelection.assets(["cbs", "extract_cbs_key_figures"])
    | AssetSelection.assets(["cbs", "extract_cbs_buurtkaart"])
    | AssetSelection.assets(["ahn", "tile_index_ahn"])
    | AssetSelection.assets(["ahn", "tile_index_ahn6"])
    | AssetSelection.assets(["ahn", "md5_ahn3"])
    | AssetSelection.assets(["ahn", "md5_ahn4"])
    | AssetSelection.assets(["ahn", "sha256_ahn5"])
    | AssetSelection.assets(["ahn", "sha256_ahn6"]),
)
