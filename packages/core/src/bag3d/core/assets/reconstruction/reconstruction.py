from hashlib import sha1
from datetime import date
import time

from dagster import (asset, StaticPartitionsDefinition, AssetIn, Output, Failure,
                     get_dagster_logger, AssetKey, DataVersion)
from psycopg.sql import SQL
from pgutils import PostgresTableIdentifier

from bag3d_pipeline.assets.ahn.core import (ahn_dir)
from bag3d_pipeline.core import geoflow_crop_dir, format_date, get_upstream_data_version
from bag3d_pipeline.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from bag3d_pipeline.assets.input.tile import get_tile_ids
from bag3d_pipeline.resources.wkt import ZUID_HOLLAND
from bag3d_pipeline.resources.temp_until_configurableresource import geoflow_version, \
    roofer_version
# debug
from bag3d_pipeline.assets.reconstruction import RECONSTRUCT_RERUN_INPUT_PARTITIONS


def generate_3dbag_version_date(context):
    """Generate a version from today's date and current timestamp hash."""
    sha1().update(str(time.time()).encode("utf-8"))
    hs = sha1().hexdigest()
    dt = date.today().strftime("%Y%m%d")
    template = "v_{date}_{hash}"
    version = template.format(date=dt, hash=hs)
    context.log.info(f"Generated version: {version}")
    return version


@asset
def reconstruction_date():
    """Generates a version from today's date, so that each partition in the
    reconstruction assets get the same version, even if they are executed over multiple
    days."""
    template = "v{date}"
    return template.format(date=format_date(date.today()))


class PartitionDefinition3DBagReconstruction(StaticPartitionsDefinition):
    def __init__(self, schema: str, table_tiles: str, wkt: str = None):
        logger = get_dagster_logger("PartitionDefinition3DBagReconstruction")
        tile_ids = get_tile_ids(schema, table_tiles, logger, wkt)
        super().__init__(partition_keys=sorted(list(tile_ids)))


@asset(
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles"
    ),
    ins={
        "regular_grid_200m": AssetIn(key_prefix="ahn"),
        "tiles": AssetIn(key_prefix="input"),
        "index": AssetIn(key_prefix="input"),
        "reconstruction_input": AssetIn(key_prefix="input"),
    },
    required_resource_keys={"db_connection", "roofer", "file_store",
                            "file_store_fastssd"},
    code_version=roofer_version()
)
def cropped_input_and_config_nl(context, regular_grid_200m, tiles, index,
                             reconstruction_input):
    """Runs roofer for cropping the input data per feature and selects the best point
    cloud for the reconstruction per feature.

    1. Crop the point clouds with the BAG footprints
    2. Select the best point cloud for the footprint and write the point cloud file
    3. Write the geoflow (.toml) reconstruction configuration file for the footprint
    """

    return cropped_input_and_config_func(context, index, reconstruction_input,
                                         regular_grid_200m, tiles)


@asset(
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles", wkt=ZUID_HOLLAND
    ),
    ins={
        "regular_grid_200m": AssetIn(key_prefix="ahn"),
        "tiles": AssetIn(key_prefix="input"),
        "index": AssetIn(key_prefix="input"),
        "reconstruction_input": AssetIn(key_prefix="input"),
    },
    required_resource_keys={"db_connection", "roofer", "file_store",
                            "file_store_fastssd"},
    code_version=roofer_version()
)
def cropped_input_and_config_zuid_holland(context, regular_grid_200m, tiles, index,
                                          reconstruction_input):
    """Runs roofer for cropping the input data per feature and selects the best point
    cloud for the reconstruction per feature.

    1. Crop the point clouds with the BAG footprints
    2. Select the best point cloud for the footprint and write the point cloud file
    3. Write the geoflow (.toml) reconstruction configuration file for the footprint
    """

    return cropped_input_and_config_func(context, index, reconstruction_input,
                                         regular_grid_200m, tiles)


@asset(
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles"
    ),
    required_resource_keys={"geoflow", "file_store", "file_store_fastssd"},
    code_version=geoflow_version()
)
def reconstructed_building_models_nl(context, cropped_input_and_config_nl):
    """Generate the 3D building models by running the reconstruction sequentially
    within one partition. Runs geof."""
    return reconstruct_building_models_func(context, cropped_input_and_config_nl)


@asset(
    partitions_def=StaticPartitionsDefinition(
        partition_keys=RECONSTRUCT_RERUN_INPUT_PARTITIONS),
    ins={
        "regular_grid_200m": AssetIn(key_prefix="ahn"),
        "tiles": AssetIn(key_prefix="input"),
        "index": AssetIn(key_prefix="input"),
        "reconstruction_input": AssetIn(key_prefix="input"),
    },
    required_resource_keys={"db_connection", "roofer", "file_store",
                            "file_store_fastssd"},
    code_version=roofer_version()
)
def cropped_input_and_config_nl_rerun(context, regular_grid_200m, tiles, index,
                                          reconstruction_input):
    """Rerun the reconstruction with just a specific set of partitions.
    """
    return cropped_input_and_config_func(context, index, reconstruction_input,
                                         regular_grid_200m, tiles)


@asset(
    partitions_def=StaticPartitionsDefinition(
        partition_keys=RECONSTRUCT_RERUN_INPUT_PARTITIONS),
    required_resource_keys={"geoflow", "file_store", "file_store_fastssd"},
    code_version=geoflow_version()
)
def reconstructed_building_models_nl_rerun(context, cropped_input_and_config_nl_rerun):
    """Rerun the reconstruction with just a specific set of partitions.
    """
    return reconstruct_building_models_func(context, cropped_input_and_config_nl_rerun)



@asset(
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles", wkt=ZUID_HOLLAND
    ),
    required_resource_keys={"geoflow", "file_store", "file_store_fastssd"},
    code_version=geoflow_version()
)
def reconstructed_building_models_zuid_holland(context,
                                               cropped_input_and_config_zuid_holland):
    """Generate the 3D building models by running the reconstruction sequentially
    within one partition. Runs geof."""
    return reconstruct_building_models_func(context,
                                            cropped_input_and_config_zuid_holland)


def cropped_input_and_config_func(context, index, reconstruction_input,
                                  regular_grid_200m, tiles):
    toml_template = """
    [input.footprint]
    path = '{footprint_file}'
    bid = "identificatie"

    [[input.pointclouds]]
    name = "AHN3"
    quality = 1
    path = '{ahn3_files}'

    [[input.pointclouds]]
    name = "AHN4"
    quality = 0
    path = '{ahn4_files}'

    [parameters]
    cellsize = 0.5

    [output]
    path = '{output_path}'

    # {{bid}} will be replaced by building identifier
    # {{pc_name}} will be replaced by input pointcloud name
    # {{path}} will be replaced by path
    building_toml_file = '{{path}}/objects/{{bid}}/config_{{pc_name}}.toml'
    building_las_file = '{{path}}/objects/{{bid}}/crop/{{bid}}_{{pc_name}}.las'
    building_raster_file = '{{path}}/objects/{{bid}}/crop/{{bid}}_{{pc_name}}.tif'
    building_gpkg_file = '{{path}}/objects/{{bid}}/crop/{{bid}}.gpkg'
    building_jsonl_file = '{{path}}/objects/{{bid}}/reconstruct/{{bid}}.city.jsonl'

    metadata_json_file = '{{path}}/metadata.json'
    jsonl_list_file = '{{path}}/features.txt'
    index_file = '{{path}}/index.gpkg'

    # these get passed through to the geoflow config files that are generated for each building
    [output.reconstruction_parameters]
    GF_PROCESS_CRS="EPSG:7415"
    OUTPUT_CRS="EPSG:7415"
    CITYJSON_TRANSLATE_X=171800.0
    CITYJSON_TRANSLATE_Y=472700.0
    CITYJSON_TRANSLATE_Z=0.0
    CITYJSON_SCALE_X=0.001
    CITYJSON_SCALE_Y=0.001
    CITYJSON_SCALE_Z=0.001
    """
    tile_id = context.asset_partition_key_for_output()
    query_laz_tiles = SQL("""    
    SELECT DISTINCT g.id
    FROM {tile_index} AS i
             JOIN {reconstruction_input} USING (fid)
             JOIN {tiles_ahn} g ON st_intersects(geometrie, g.geom)
    WHERE i.tile_id = {tile_id};
    """)
    res = context.resources.db_connection.get_query(
        query_laz_tiles, query_params={
            "tiles_ahn": regular_grid_200m,
            "reconstruction_input": reconstruction_input,
            "tile_index": index,
            "tile_id": tile_id
        }
    )
    out_dir_ahn3 = ahn_dir(context.resources.file_store.data_dir,
                           ahn_version=3).joinpath(f"tiles_200m")
    laz_files_ahn3 = [
        str(out_dir_ahn3 / f't_{tile_id_ahn[0]}.laz')
        for tile_id_ahn in res
    ]
    # TODO: probably should take the tiles_200m directory from the asset output
    out_dir_ahn4 = ahn_dir(context.resources.file_store.data_dir,
                           ahn_version=4).joinpath(f"tiles_200m")
    # TODO: same with the laz filename pattern
    laz_files_ahn4 = [
        str(out_dir_ahn4 / f't_{tile_id_ahn[0]}.laz')
        for tile_id_ahn in res
    ]
    # Would be neater if we could use -sql in the OGR connection to do this query,
    # instead of creating a view.
    tile_view = PostgresTableIdentifier(tiles.schema, f"t_{tile_id}")
    query_tile_view = SQL("""
    CREATE OR REPLACE VIEW {tile_view} AS
    SELECT i.*
    FROM {reconstruction_input} i JOIN {tile_index} ti
            USING (fid)
    WHERE ti.tile_id = {tile_id}
    """)
    context.resources.db_connection.send_query(
        query_tile_view, query_params={
            "tile_view": tile_view,
            "reconstruction_input": reconstruction_input,
            "tile_index": index,
            "tile_id": tile_id
        }
    )
    output_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.data_dir).joinpath(
        tile_id)
    output_dir.mkdir(exist_ok=True, parents=True)
    output_toml = toml_template.format(
        tile_id=tile_id,
        footprint_file=f"PG:{context.resources.db_connection.dsn} tables={tile_view}",
        ahn3_files=" ".join(laz_files_ahn3),
        ahn4_files=" ".join(laz_files_ahn4),
        output_path=output_dir)
    path_toml = output_dir / f"crop.toml"
    with path_toml.open("w") as of:
        of.write(output_toml)
    context.resources.roofer.execute("crop", "{exe} -c {local_path}",
                                     local_path=path_toml)
    context.resources.db_connection.send_query(
        SQL("drop view {tile_view}"), query_params={"tile_view": tile_view}
    )
    # TODO: what are the conditions for partition failure?
    objects_dir = output_dir.joinpath("objects")
    if objects_dir.exists():
        feature_count = sum(1 for f in objects_dir.iterdir() if f.is_dir())
    else:
        feature_count = 0
    return Output(output_dir, metadata={
        "feature_count": feature_count,
        "path": str(output_dir)
    })


def reconstruct_building_models_func(context, cropped_input_and_config):
    context.log.info(f"geoflow.kwargs: {context.resources.geoflow.kwargs}")
    flowchart = context.resources.geoflow.kwargs["flowcharts"]["reconstruct"]
    cmd_template = "{{exe}} {{local_path}} --config {config_path}"
    # TODO: what are the conditions for partition failure?
    objects_dir = cropped_input_and_config.joinpath("objects")
    if not objects_dir.exists():
        # In this case it would make more sense to raise Failure, but then a downstream,
        # un-partitioned asset will never execute, because the downstream un-partitioned
        # asset will expect that *all* upstream partitions succeeded.
        # Maybe we could do something with a custom partition loader here...
        context.log.error(f"input features don't exists for {cropped_input_and_config}")
        return Output(cropped_input_and_config, metadata={
            "failed_nr": 0,
            "failed_id": [],
            "success_nr": 0
        })
    failed = []
    cnt = 0
    for feature in objects_dir.iterdir():
        if feature.is_dir():
            config_path = feature.joinpath("config_.toml")
            cmd = cmd_template.format(config_path=config_path)
            try:
                return_code, output = context.resources.geoflow.execute("geof", cmd,
                                                                        local_path=flowchart,
                                                                        silent=True)
                if return_code == 0:
                    cnt += 1
                else:
                    context.log.error(output)
                    raise Failure
            except Failure:
                failed.append(feature)
    if cnt == 0:
        raise Failure(
            f"all features failed the reconstruction in {cropped_input_and_config}")
    else:
        return Output(cropped_input_and_config, metadata={
            "failed_nr": len(failed),
            "failed_id": [f.name for f in failed],
            "success_nr": cnt
        })
