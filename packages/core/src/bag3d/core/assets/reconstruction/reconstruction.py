import time
from datetime import date
from hashlib import sha1
from pathlib import Path

from dagster import (
    asset,
    StaticPartitionsDefinition,
    AssetIn,
    Failure,
    get_dagster_logger,
    Field,
)
from pgutils import PostgresTableIdentifier
from psycopg.sql import SQL

from bag3d.common.resources import resource_defs
from bag3d.common.utils.dagster import format_date
from bag3d.common.utils.files import geoflow_crop_dir
from bag3d.core.assets.ahn.core import ahn_dir
from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from bag3d.core.assets.input.tile import get_tile_ids


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
    days.
    """
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
    required_resource_keys={
        "db_connection",
        "roofer",
        "file_store",
        "file_store_fastssd",
    },
    code_version=resource_defs["roofer"].app.version("roofer"),
    config_schema={
        "drop_views": Field(
            bool,
            description="Drop the tile view after reconstruction",
            is_required=False,
            default_value=True,
        ),
        "loglevel": Field(
            str,
            description="Roofer --loglevel.",
            is_required=False,
            default_value="info",
        ),
        "dir_tiles_200m_ahn3": Field(
            str,
            description="Directory of the 200m tiles of AHN3. Used if the tiles are stored in a non-standard location.",
            is_required=False,
        ),
        "dir_tiles_200m_ahn4": Field(
            str,
            description="Directory of the 200m tiles of AHN4. Used if the tiles are stored in a non-standard location.",
            is_required=False,
        ),
        "dir_tiles_200m_ahn5": Field(
            str,
            description="Directory of the 200m tiles of AHN5. Used if the tiles are stored in a non-standard location.",
            is_required=False,
        ),
    },
)
def reconstructed_building_models_nl(
    context, regular_grid_200m, tiles, index, reconstruction_input
):
    """Generate the 3D building models by running the reconstruction sequentially
    within one partition.
    Runs roofer."""

    roofer_toml, output_dir, tile_view = create_roofer_config(
        context,
        index,
        reconstruction_input,
        regular_grid_200m,
        tiles,
        dir_tiles_200m_ahn3=context.op_config.get("dir_tiles_200m_ahn3"),
        dir_tiles_200m_ahn4=context.op_config.get("dir_tiles_200m_ahn4"),
        dir_tiles_200m_ahn5=context.op_config.get("dir_tiles_200m_ahn5"),
    )

    context.log.info(f"{roofer_toml=}")
    context.log.info(f"{tile_view=}")

    try:
        return_code, output = context.resources.roofer.app.execute(
            exe_name="roofer",
            command=f"{{exe}} --config {{local_path}} {output_dir} --loglevel {context.op_config['loglevel']}",
            local_path=roofer_toml,
            silent=False,
        )
        context.log.debug(f"{return_code=} {output=}")
        if return_code != 0 or "error" in output.lower():
            context.log.error(output)
            raise Failure
    finally:
        if context.op_config["drop_views"]:
            context.resources.db_connection.connect.send_query(
                SQL("DROP VIEW {tile_view}"), query_params={"tile_view": tile_view}
            )


def create_roofer_config(
    context,
    index,
    reconstruction_input,
    regular_grid_200m,
    tiles,
    dir_tiles_200m_ahn3=None,
    dir_tiles_200m_ahn4=None,
    dir_tiles_200m_ahn5=None,
):
    toml_template = """
    polygon-source = "{footprint_file}"
    id-attribute = "identificatie"
    force-lod11-attribute = "kas_warenhuis"
    
    split-cjseq = true
    omit-metadata = true
    cj-translate = [171800.0,472700.0,0.0]
    output-directory = "{output_path}"
    
    [[pointclouds]]
    name = "AHN3"
    quality = 1
    source = {ahn3_files}
    
    [[pointclouds]]
    name = "AHN4"
    quality = 0
    source = {ahn4_files}

    [output-attributes]
    status = "b3_status"
    reconstruction_time = "b3_t_run"
    val3dity_lod12 = "b3_val3dity_lod12"
    val3dity_lod13 = "b3_val3dity_lod13"
    val3dity_lod22 = "b3_val3dity_lod22"
    is_glass_roof = "b3_is_glas_dak"
    nodata_frac = "b3_nodata_fractie"
    nodata_r = "b3_nodata_radius"
    pt_density = "b3_puntdichtheid"
    is_mutated = "b3_mutatie"
    pc_select = "b3_pw_selectie_reden"
    pc_source = "b3_pw_bron"
    pc_year = "b3_pw_datum"
    force_lod11 = "b3_reconstructie_onvolledig"
    roof_type = "b3_dak_type"
    h_roof_50p = "b3_h_dak_50p"
    h_roof_70p = "b3_h_dak_70p"
    h_roof_min = "b3_h_dak_min"
    h_roof_max = "b3_h_dak_max"
    roof_n_planes = "b3_n_vlakken"
    rmse_lod12 = "b3_rmse_lod12"
    rmse_lod13 = "b3_rmse_lod13"
    rmse_lod22 = "b3_rmse_lod22"
    h_ground = "b3_h_maaiveld"
    slope = "b3_hellingshoek"
    azimuth = "b3_azimut"
    """
    tile_id = context.partition_key
    query_laz_tiles = SQL("""    
    SELECT DISTINCT g.id
    FROM {tile_index} AS i
             JOIN {reconstruction_input} USING (fid)
             JOIN {tiles_ahn} g ON st_intersects(geometrie, g.geom)
    WHERE i.tile_id = {tile_id};
    """)
    res = context.resources.db_connection.connect.get_query(
        query_laz_tiles,
        query_params={
            "tiles_ahn": regular_grid_200m,
            "reconstruction_input": reconstruction_input,
            "tile_index": index,
            "tile_id": tile_id,
        },
    )
    if dir_tiles_200m_ahn3 is not None:
        out_dir_ahn3 = Path(dir_tiles_200m_ahn3)
    else:
        out_dir_ahn3 = ahn_dir(
            context.resources.file_store.file_store.data_dir, ahn_version=3
        ).joinpath("tiles_200m")
    laz_files_ahn3 = [
        str(out_dir_ahn3 / f"t_{tile_id_ahn[0]}.laz") for tile_id_ahn in res
    ]
    # TODO: probably should take the tiles_200m directory from the asset output
    if dir_tiles_200m_ahn4 is not None:
        out_dir_ahn4 = Path(dir_tiles_200m_ahn4)
    else:
        out_dir_ahn4 = ahn_dir(
            context.resources.file_store.file_store.data_dir, ahn_version=4
        ).joinpath("tiles_200m")
    # TODO: same with the laz filename pattern
    laz_files_ahn4 = [
        str(out_dir_ahn4 / f"t_{tile_id_ahn[0]}.laz") for tile_id_ahn in res
    ]
    out_dir_ahn5 = ahn_dir(
        context.resources.file_store.file_store.data_dir, ahn_version=5
    ).joinpath("tiles_200m")
    laz_files_ahn5 = [
        str(out_dir_ahn5 / f"t_{tile_id_ahn[0]}.laz") for tile_id_ahn in res
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
    context.resources.db_connection.connect.send_query(
        query_tile_view,
        query_params={
            "tile_view": tile_view,
            "reconstruction_input": reconstruction_input,
            "tile_index": index,
            "tile_id": tile_id,
        },
    )
    output_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.file_store.data_dir
    ).joinpath(tile_id)
    output_dir.mkdir(exist_ok=True, parents=True)
    output_toml = toml_template.format(
        footprint_file=f"PG:{context.resources.db_connection.connect.dsn} tables={tile_view}",
        ahn3_files=laz_files_ahn3,
        ahn4_files=laz_files_ahn4,
        ahn5_files=laz_files_ahn5,
        output_path=output_dir,
    )
    path_toml = output_dir / "roofer.toml"
    with path_toml.open("w") as of:
        of.write(output_toml)

    return path_toml, output_dir, tile_view
