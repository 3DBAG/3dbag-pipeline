import time
from copy import deepcopy
from datetime import date
from hashlib import sha1
from os import getenv

from bag3d.common.resources import NlTransform, tool_versions
from bag3d.common.resources.database import DatabaseResource
from bag3d.common.resources.executables import RooferResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.utils.dagster import format_date
from dagster import (
    AssetExecutionContext,
    AssetIn,
    Config,
    Failure,
    StaticPartitionsDefinition,
    asset,
    get_dagster_logger,
)
from pgutils import PostgresTableIdentifier
from psycopg.sql import SQL
from pydantic import Field

from bag3d.core.assets.input import RECONSTRUCTION_INPUT_SCHEMA
from bag3d.core.assets.input.tile import get_tile_ids

logger = get_dagster_logger("reconstruction.reconstruction")


class RooferConfig(Config):
    """Configuration for roofer reconstruction asset."""

    drop_views: bool = Field(
        default=True, description="Drop the tile view after reconstruction"
    )
    loglevel: str = Field(default="info", description="Roofer --loglevel.")
    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_ROOFER", "10")),
        description="Roofer --jobs",
    )


def generate_3dbag_version_date():
    """Generate a version from today's date and current timestamp hash."""
    sha1().update(str(time.time()).encode("utf-8"))
    hs = sha1().hexdigest()
    dt = date.today().strftime("%Y%m%d")
    template = "v_{date}_{hash}"
    version = template.format(date=dt, hash=hs)
    logger.info(f"Generated version: {version}")
    return version


@asset
def reconstruction_date() -> str:
    """Generates a version from today's date, so that each partition in the
    reconstruction assets get the same version, even if they are executed over multiple
    days.
    """
    template = "v{date}"
    return template.format(date=format_date(date.today()))


class PartitionDefinition3DBagReconstruction(StaticPartitionsDefinition):
    def __init__(self, schema: str, table_tiles: str, wkt: str | None = None):
        logger = get_dagster_logger("PartitionDefinition3DBagReconstruction")
        tile_ids = get_tile_ids(schema, table_tiles, logger, wkt)
        super().__init__(partition_keys=sorted(tile_ids))


@asset(
    partitions_def=PartitionDefinition3DBagReconstruction(
        schema=RECONSTRUCTION_INPUT_SCHEMA, table_tiles="tiles"
    ),
    ins={
        "metadata_ahn3_index": AssetIn(key_prefix="ahn"),
        "metadata_ahn4_index": AssetIn(key_prefix="ahn"),
        "metadata_ahn5_index": AssetIn(key_prefix="ahn"),
        "metadata_ahn6_index": AssetIn(key_prefix="ahn"),
        "tiles": AssetIn(key_prefix="input"),
        "index": AssetIn(key_prefix="input"),
        "reconstruction_input": AssetIn(key_prefix="input"),
    },
    code_version=tool_versions.get_version("roofer"),
    pool="roofer",
)
def reconstructed_building_models(
    context: AssetExecutionContext,
    config: RooferConfig,
    computation_db: DatabaseResource,
    roofer: RooferResource,
    file_store: FileStoreResource,
    nl_transform: NlTransform,
    tiles,
    index,
    reconstruction_input,
    metadata_ahn3_index,
    metadata_ahn4_index,
    metadata_ahn5_index,
    metadata_ahn6_index,
) -> None:
    """Generate the 3D building models by running the reconstruction sequentially
    within one partition.
    Runs roofer."""

    roofer_toml, output_dir, tile_view = create_roofer_config(
        context,
        computation_db=computation_db,
        file_store=file_store,
        nl_transform=nl_transform,
        reconstruction_input=reconstruction_input,
        index=index,
        tiles=tiles,
        metadata_ahn3=metadata_ahn3_index,
        metadata_ahn4=metadata_ahn4_index,
        metadata_ahn5=metadata_ahn5_index,
        metadata_ahn6=metadata_ahn6_index,
    )

    logger.info(f"{roofer_toml=}")
    logger.info(f"{tile_view=}")

    try:
        result = roofer.runner.run(
            f"{{exe}} --config {{local_path}} {output_dir} -j {config.concurrency} --loglevel {config.loglevel} --skip-pc-check",
            exe_name="roofer",
            local_path=roofer_toml,
            logger=logger,
        )
        logger.debug(f"{result.returncode=}")
        if not result.success or "error" in result.stdout.lower():
            logger.error(result.stdout)
            raise Failure()
    finally:
        if config.drop_views:
            computation_db.connection.send_query(
                SQL("DROP VIEW {tile_view}"), query_params={"tile_view": tile_view}
            )


def create_roofer_config(
    context: AssetExecutionContext,
    computation_db: DatabaseResource,
    file_store: FileStoreResource,
    nl_transform: NlTransform,
    reconstruction_input,
    index,
    tiles,
    metadata_ahn3,
    metadata_ahn4,
    metadata_ahn5,
    metadata_ahn6,
):
    toml_template = """
    polygon-source = "{footprint_file}"
    id-attribute = "identificatie"
    force-lod11-attribute = "b3_kas_warenhuis"
    yoc-attribute = "oorspronkelijkbouwjaar"
    lod11-fallback-area = 30000

    split-cjseq = false
    omit-metadata = false
    output-directory = "{output_path}"

    lod12 = true
    lod13 = true
    lod22 = true

    [[pointclouds]]
    name = "ahn3"
    quality = 3
    source = {ahn3_files}

    [[pointclouds]]
    name = "ahn4"
    quality = 2
    source = {ahn4_files}

    [[pointclouds]]
    name = "ahn5"
    quality = 1
    source = {ahn5_files}

    [[pointclouds]]
    name = "ahn6"
    quality = 0
    source = {ahn6_files}

    [output-attributes]
    success = ""
    force_lod11 = ""
    h_pc_98p = ""
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
    roof_type = "b3_dak_type"
    h_roof_50p = "b3_h_dak_50p"
    h_roof_70p = "b3_h_dak_70p"
    h_roof_min = "b3_h_dak_min"
    h_roof_max = "b3_h_dak_max"
    roof_n_planes = "b3_n_vlakken"
    rmse_lod12 = "b3_rmse_lod12"
    rmse_lod13 = "b3_rmse_lod13"
    rmse_lod22 = "b3_rmse_lod22"
    volume_lod12 = "b3_volume_lod12"
    volume_lod13 = "b3_volume_lod13"
    volume_lod22 = "b3_volume_lod22"
    h_ground = "b3_h_maaiveld"
    slope = "b3_hellingshoek"
    azimuth = "b3_azimut"
    extrusion_mode = "b3_extrusie"
    pointcloud_unusable = "b3_pw_onvoldoende"
    h_roof_ridge = "b3_h_nok"
    roof_n_ridgelines = "b3_n_nok"
    """
    tile_id = context.partition_key
    query_laz_tiles = SQL("""
    SELECT DISTINCT ON (m.tile_id) m.tile_id, m.pdal_info ->> 'filename' AS filename
    FROM {metadata_ahn} m
             JOIN {reconstruction_input} r
                  ON st_intersects(r.geometrie, m.boundary)
             JOIN {tile_index} AS i USING (fid)
    WHERE i.tile_id = {tile_id}
      AND NULLIF(m.pdal_info ->> 'filename', '') IS NOT NULL
      AND m.hash IS NOT NULL
    ORDER BY m.tile_id, m.insert_time DESC;
    """)
    query_params = {
        "metadata_ahn": None,
        "reconstruction_input": reconstruction_input,
        "tile_index": index,
        "tile_id": tile_id,
    }
    query_params_ahn3 = deepcopy(query_params)
    query_params_ahn3["metadata_ahn"] = metadata_ahn3
    query_params_ahn4 = deepcopy(query_params)
    query_params_ahn4["metadata_ahn"] = metadata_ahn4
    query_params_ahn5 = deepcopy(query_params)
    query_params_ahn5["metadata_ahn"] = metadata_ahn5
    query_params_ahn6 = deepcopy(query_params)
    query_params_ahn6["metadata_ahn"] = metadata_ahn6
    laz_files_ahn3 = [
        r["filename"]  # type: ignore[index]
        for r in computation_db.connection.get_dict(
            query_laz_tiles,
            query_params=query_params_ahn3,
        )
    ]
    laz_files_ahn4 = [
        r["filename"]  # type: ignore[index]
        for r in computation_db.connection.get_dict(
            query_laz_tiles, query_params=query_params_ahn4
        )
    ]
    laz_files_ahn5 = [
        r["filename"]  # type: ignore[index]
        for r in computation_db.connection.get_dict(
            query_laz_tiles,
            query_params=query_params_ahn5,
        )
    ]
    laz_files_ahn6 = [
        r["filename"]  # type: ignore[index]
        for r in computation_db.connection.get_dict(
            query_laz_tiles,
            query_params=query_params_ahn6,
        )
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
    computation_db.connection.send_query(
        query_tile_view,
        query_params={
            "tile_view": tile_view,
            "reconstruction_input": reconstruction_input,
            "tile_index": index,
            "tile_id": tile_id,
        },
    )
    output_dir = file_store.stage_dir("reconstruction").joinpath(tile_id)
    output_dir.mkdir(exist_ok=True, parents=True)
    output_toml = toml_template.format(
        footprint_file=f"PG:{computation_db.connection.dsn} tables={tile_view}",
        ahn3_files=laz_files_ahn3,
        ahn4_files=laz_files_ahn4,
        ahn5_files=laz_files_ahn5,
        ahn6_files=laz_files_ahn6,
        output_path=output_dir,
        nl_transform_scale=nl_transform.scale,
        nl_transform_translate=nl_transform.translate,
    )
    path_toml = output_dir / "roofer.toml"
    with path_toml.open("w") as of:
        of.write(output_toml)

    return path_toml, output_dir, tile_view
