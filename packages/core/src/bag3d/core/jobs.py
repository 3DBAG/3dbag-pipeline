import os
from datetime import datetime

from dagster import (define_asset_job, AssetSelection)

from bag3d.common.resources.wkt import ZUID_HOLLAND

from bag3d.core.assets.reconstruction.reconstruction import (
    PartitionDefinition3DBagReconstruction
)
from bag3d.core.assets.reconstruction import RECONSTRUCT_RERUN_INPUT_PARTITIONS
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA


assets_sample_data = AssetSelection.groups("sample")

job_sample_data_image = define_asset_job(
    name="sample_data_update_image",
    description="Update the 3dbag-sample-data docker image with new sample data.",
    selection=assets_sample_data
)

job_bgt = define_asset_job(
    name="bgt",
    description="Load the latest BGT Pand, Wegdeel layers.",

)

# WARNING!!! multi_assets don't have key_prefix, https://github.com/dagster-io/dagster/issues/9344
job_ahn3 = define_asset_job(
    name="ahn3",
    description="Make sure that the available AHN 3 LAZ files are present on disk, "
                "and their metadata is recorded.",
    selection=AssetSelection.keys(["ahn", "laz_files_ahn3"]) |
              AssetSelection.keys(["ahn", "metadata_ahn3"]) |
              AssetSelection.keys(["ahn", "lasindex_ahn3"])
)

job_ahn4 = define_asset_job(
    name="ahn4",
    description="Make sure that the available AHN 4 LAZ files are present on disk, "
                "and their metadata is recorded.",
    selection=AssetSelection.keys(["ahn", "md5_pdok_ahn4"]) |
              AssetSelection.keys(["ahn", "metadata_table_ahn4"]) |
              AssetSelection.keys(["ahn", "tile_index_ahn4_pdok"]) |
              AssetSelection.keys(["ahn", "laz_files_ahn4"]) |
              AssetSelection.keys(["ahn", "metadata_ahn4"]) |
              AssetSelection.keys(["ahn", "lasindex_ahn4"])
)

job_source_input = define_asset_job(
    name="source_input",
    description="Update the source data sets and prepare the input for the reconstruction.",
    selection=AssetSelection.groups("bag") |
              AssetSelection.groups("top10nl") |
              AssetSelection.groups("input"),
)

job_nl_reconstruct = define_asset_job(
    name="nl_reconstruct",
    description="Run the crop and reconstruct steps for the Netherlands.",
    selection=AssetSelection.keys(["reconstruction", "cropped_input_and_config_nl"]) |
              AssetSelection.keys(
                  ["reconstruction", "reconstructed_building_models_nl"]),
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles"
    ),
)

job_nl_export = define_asset_job(
    name="nl_export",
    description="Run the tyler export and 3D Tiles steps for the Netherlands.",
    selection=AssetSelection.keys(["export", "feature_evaluation"]) |
              AssetSelection.keys(["export", "export_index"]) |
              AssetSelection.keys(["export", "metadata"]) |
              AssetSelection.keys(["export", "geopackage_nl"]) |
              AssetSelection.keys(["export", "reconstruction_output_multitiles_nl"]),
)

job_nl_deploy = define_asset_job(
    name="nl_deploy",
    description="Deploy the Netherland data.",
    selection=AssetSelection.keys(["export", "compressed_tiles"]) |
              AssetSelection.keys(["export", "compressed_tiles_validation"]) |
              AssetSelection.keys(["deploy", "compressed_export_nl"]) |
              AssetSelection.keys(["deploy", "downloadable_godzilla"]) |
              AssetSelection.keys(["deploy", "webservice_godzilla"]),
)


job_zuid_holland_reconstruct = define_asset_job(
    name="zuid_holland_reconstruct",
    description="Run the crop and reconstruct steps for the province of Zuid-Holland.",
    selection=AssetSelection.keys(
        ["reconstruction", "cropped_input_and_config_zuid_holland"]) |
              AssetSelection.keys(
                  ["reconstruction", "reconstructed_building_models_zuid_holland"]),
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles", wkt=ZUID_HOLLAND
    ),
)

job_zuid_holland_export = define_asset_job(
    name="zuid_holland_export",
    description="Run the tyler export and 3D Tiles steps for the province of "
                "Zuid-Holland.",
    selection=AssetSelection.keys(["export", "feature_evaluation"]) |
              AssetSelection.keys(["export", "export_index"]) |
              AssetSelection.keys(["export", "metadata"]) |
              AssetSelection.keys(["export", "geopackage_nl"]) |
              AssetSelection.keys(
                  ["export", "reconstruction_output_multitiles_zuid_holland"]) |
              AssetSelection.keys(
                  ["export", "reconstruction_output_3dtiles_zuid_holland"]),
)

job_zuid_holland_deploy = define_asset_job(
    name="zuid_holland_deploy",
    description="Deploy the Zuid-Holland data.",
    selection=AssetSelection.keys(["deploy", "compressed_export_zuid_holland"]) |
              AssetSelection.keys(["deploy", "downloadable_godzilla"]) |
              AssetSelection.keys(["deploy", "webservice_godzilla"]),
)

job_zuid_holland_export_deploy = define_asset_job(
    name="zuid_holland_export_deploy",
    description="Run the tyler export and 3D Tiles and deploy steps for the province of "
                "Zuid-Holland.",
    selection=AssetSelection.keys(["export", "feature_evaluation"]) |
              AssetSelection.keys(["export", "export_index"]) |
              AssetSelection.keys(["export", "metadata"]) |
              AssetSelection.keys(["export", "geopackage_nl"]) |
              AssetSelection.keys(
                  ["export", "reconstruction_output_multitiles_zuid_holland"]) |
              AssetSelection.keys(["deploy", "compressed_export_zuid_holland"]) |
              AssetSelection.keys(["deploy", "downloadable_godzilla"]) |
              AssetSelection.keys(["deploy", "webservice_godzilla"]),
)

