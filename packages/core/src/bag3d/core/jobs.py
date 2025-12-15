from os import getenv
from dagster import define_asset_job, AssetSelection, multiprocess_executor


job_bgt = define_asset_job(
    name="bgt",
    description="Load the latest BGT Pand, Wegdeel layers.",
)

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


job_ahn_metadata_index = define_asset_job(
    name="ahn_metadata_index",
    description="Creates indices on the AHN metadata tables",
    selection=AssetSelection.assets(["ahn", "metadata_ahn3_index"])
    | AssetSelection.assets(["ahn", "metadata_ahn4_index"])
    | AssetSelection.assets(["ahn", "metadata_ahn5_index"]),
)


job_source_input = define_asset_job(
    name="source_input",
    description="Update the source data sets and prepare the input for the reconstruction.",
    selection=AssetSelection.assets(["bag", "extract_bag"])
    | AssetSelection.assets(["bag", "stage_bag_pand"])
    | AssetSelection.assets(["bag", "bag_pandactueelbestaand"])
    | AssetSelection.assets(["bag", "stage_bag_verblijfsobject"])
    | AssetSelection.assets(["bag", "bag_verblijfsobjectactueelbestaand"])
    | AssetSelection.groups("top10nl")
    | AssetSelection.groups("input"),
)

job_nl_reconstruct = define_asset_job(
    name="nl_reconstruct",
    description="Run the crop and reconstruct steps for the Netherlands.",
    executor_def=multiprocess_executor.configured(
        {"max_concurrent": int(getenv("BAG3D_CONCURRENCY_JOB_NL_RECONSTRUCT", 1))}
    ),
    selection=AssetSelection.assets(
        ["reconstruction", "reconstructed_building_models_nl"]
    ),
    config={
        "ops": {
            "reconstructed_building_models_nl": {
                "config": {
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_TOOL_ROOFER", 1))
                }
            }
        }
    },
)

job_nl_reconstruct_debug = define_asset_job(
    name="nl_reconstruct_debug",
    description="Run the crop and reconstruct steps for the Netherlands with debug info.",
    selection=AssetSelection.assets(
        ["reconstruction", "reconstructed_building_models_nl"]
    ),
    config={
        "ops": {
            "reconstructed_building_models_nl": {
                "config": {
                    "drop_views": False,
                    "loglevel": "debug",
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_TOOL_ROOFER", 1)),
                }
            }
        }
    },
)

job_nl_export = define_asset_job(
    name="nl_export",
    description="Run the tyler export and 3D Tiles steps for the Netherlands.",
    selection=AssetSelection.assets(["export", "feature_evaluation"])
    | AssetSelection.assets(["export", "export_index"])
    | AssetSelection.assets(["export", "metadata"])
    | AssetSelection.assets(["export", "reconstruction_output_multitiles_nl"]),
    config={
        "ops": {
            "reconstruction_output_multitiles_nl": {
                "config": {
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_TOOL_TYLER", 1))
                }
            }
        }
    },
)

job_nl_export_after_floors = define_asset_job(
    name="nl_export_after_floors",
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
    config={
        "ops": {
            "reconstruction_output_multitiles_nl": {
                "config": {
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_TOOL_TYLER", 1))
                }
            },
            "reconstruction_output_3dtiles_lod12_nl": {
                "config": {
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_TOOL_TYLER", 1))
                }
            },
            "reconstruction_output_3dtiles_lod13_nl": {
                "config": {
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_TOOL_TYLER", 1))
                }
            },
            "reconstruction_output_3dtiles_lod22_nl": {
                "config": {
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_TOOL_TYLER", 1))
                }
            },
            "compressed_tiles": {
                "config": {
                    "concurrency": int(getenv("BAG3D_CONCURRENCY_JOB_ARCHIVE", 1))
                }
            },
        }
    },
)

job_nl_deploy = define_asset_job(
    name="nl_deploy",
    description="Deploy the Netherland data.",
    selection=AssetSelection.assets(["deploy", "compressed_export_nl"])
    | AssetSelection.assets(["deploy", "transfer_to_godzilla"])
    | AssetSelection.assets(["deploy", "transfer_to_podzilla"])
    | AssetSelection.assets(["deploy", "webservice_godzilla"]),
)


job_nl_release = define_asset_job(
    name="nl_release",
    description="Perform the final steps for the 3DBAG release.",
    selection=AssetSelection.assets(["release", "publish_data"])
    | AssetSelection.assets(["release", "publish_webservices"]),
)
