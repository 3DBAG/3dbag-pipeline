from datetime import date

from dagster import AssetKey, asset

from bag3d_pipeline.core import (geoflow_crop_dir, bag3d_export_dir, format_date)
from bag3d_pipeline.resources.temp_until_configurableresource import tyler_version


def reconstruction_output_tiles_func(context, format: str):
    """Run tyler on the reconstruction output directory.
    Format is either 'multi' or '3dtiles'. See tyler docs for details.
    TODO: Generalize the paths that are currently hardcoded for gilfoyle.
    """
    reconstructed_root_dir = geoflow_crop_dir(context.resources.file_store_fastssd.data_dir)
    output_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    context.log.debug(f"{reconstructed_root_dir=}")
    # on gilfoyle
    metadata_file = "/home/bdukai/software/tyler/resources/geof/metadata.json"
    cmd = [
        "RUST_LOG=info",
        "TYLER_RESOURCES_DIR=/home/bdukai/software/tyler/resources",
        "{exe}",
        "--metadata", str(metadata_file),
        "--features", str(reconstructed_root_dir),
        "--output", str(output_dir),
        "--format", format.lower(),
        "--exe-geof", str(context.resources.geoflow.exes["geof"]),
        "--object-type", "Building",
        "--object-type", "BuildingPart",
        "--qtree-capacity", "280000",
    ]
    if format == "multi":
        cmd.append("--grid-export")
    elif format == "3dtiles":
        cmd.extend([
            "--3dtiles-metadata-class", "building",
            "--object-attribute", "bouwjaar:int",
            "--3dtiles-implicit",
            "--lod-building", "2.2",
            "--lod-building-part", "2.2"
        ])
    else:
        raise ValueError(f"invalid format: {format}, only 'multi' and '3dtiles' are "
                         f"allowed")
    context.log.debug(" ".join(cmd))
    context.resources.tyler.execute("tyler", " ".join(cmd), cwd=str(output_dir))
    return output_dir


@asset(
    non_argument_deps={
        AssetKey(("reconstruction", "reconstructed_building_models_nl"))
    },
    required_resource_keys={"tyler", "geoflow", "file_store", "file_store_fastssd"}
)
def reconstruction_output_multitiles_nl(context):
    """Tiles for distribution, in CityJSON, OBJ, GPKG formats.
    Generated with tyler."""
    return reconstruction_output_tiles_func(context, format="multi")


@asset(
    non_argument_deps={
        AssetKey(("reconstruction", "reconstructed_building_models_nl"))
    },
    required_resource_keys={"tyler", "geoflow", "file_store", "file_store_fastssd"}
)
def reconstruction_output_3dtiles_nl(context):
    """3D Tiles v1.1 generated with tyler."""
    return reconstruction_output_tiles_func(context, format="3dtiles")


@asset(
    non_argument_deps={
        AssetKey(("reconstruction", "reconstructed_building_models_zuid_holland"))
    },
    required_resource_keys={"tyler", "geoflow","file_store", "file_store_fastssd"},
    code_version=tyler_version()
)
def reconstruction_output_multitiles_zuid_holland(context):
    """Tiles for distribution, in CityJSON, OBJ, GPKG formats, for the
    province of Zuid-Holland. Generated with tyler."""
    return reconstruction_output_tiles_func(context, format="multi")


@asset(
    non_argument_deps={
        AssetKey(("reconstruction", "reconstructed_building_models_zuid_holland"))
    },
    required_resource_keys={"tyler", "geoflow","file_store", "file_store_fastssd"},
    code_version=tyler_version()
)
def reconstruction_output_3dtiles_zuid_holland(context):
    """3D Tiles v1.1 generated with tyler, for the province of Zuid-Holland."""
    return reconstruction_output_tiles_func(context, format="3dtiles")
