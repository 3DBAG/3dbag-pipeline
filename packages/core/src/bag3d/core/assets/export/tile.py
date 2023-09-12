import csv
from pathlib import Path
from typing import Iterator, Tuple, Sequence

from dagster import AssetKey, asset

from bag3d.common.utils.files import geoflow_crop_dir, bag3d_export_dir
from bag3d.common.resources.temp_until_configurableresource import tyler_version


def reconstruction_output_tiles_func(context, format: str):
    """Run tyler on the reconstruction output directory.
    Format is either 'multi' or '3dtiles'. See tyler docs for details.
    TODO: Generalize the paths that are currently hardcoded for gilfoyle.
    """
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.data_dir)
    output_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    context.log.debug(f"{reconstructed_root_dir=}")
    # on gilfoyle
    metadata_file = "/home/bdukai/software/tyler/resources/geof/metadata.json"
    # # Set the parallelism in tyler from the dagster instance configuration (the dagster.yaml in $DAGSTER_HOME)
    # num_threads = context.instance.run_coordinator.inst_data.config_dict["max_concurrent_runs"]
    num_threads = 4
    cmd = [
        f"RAYON_NUM_THREADS={num_threads}",
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
    required_resource_keys={"tyler", "geoflow", "file_store", "file_store_fastssd"},
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
    required_resource_keys={"tyler", "geoflow", "file_store", "file_store_fastssd"},
    code_version=tyler_version()
)
def reconstruction_output_3dtiles_zuid_holland(context):
    """3D Tiles v1.1 generated with tyler, for the province of Zuid-Holland."""
    return reconstruction_output_tiles_func(context, format="3dtiles")


def check_export_results(path_quadtree_tsv: Path, path_tiles_dir: Path) -> Iterator[
    Tuple[str, bool, bool, bool, str]]:
    """Parse the quadtree.tsv written by tyler, check if all formats exists for each
    tile, add the tile WKT.
    Returns a Generator of
     (leaf_id:str, has_cityjson:bool, has_all_gpkg:bool, has_all_obj:bool, wkt:str)"""
    with path_quadtree_tsv.open("r") as fo:
        csvreader = csv.reader(fo, delimiter="\t")
        # skip header, which is [id, level, nr_items, leaf, wkt]
        next(csvreader)
        for row in csvreader:
            if row[3] == "true" and int(row[2]) > 0:
                leaf_id = row[0]
                leaf_id_in_filename = leaf_id.replace("/", "-")
                has_cityjson = path_tiles_dir.joinpath(leaf_id,
                                                       f"{leaf_id_in_filename}.city.json").exists()
                gpkg_cnt = sum(1 for f in path_tiles_dir.joinpath(leaf_id).iterdir()
                               if f.suffix == ".gpkg")
                has_all_gpkg = gpkg_cnt == 1
                obj_cnt = sum(1 for f in path_tiles_dir.joinpath(leaf_id).iterdir()
                              if f.suffix == ".obj")
                has_all_obj = obj_cnt == 6
                yield leaf_id, has_cityjson, has_all_gpkg, has_all_obj, row[4]


def get_tile_ids() -> Sequence[str]:
    """Get the IDs of the distribution tiles from the file system."""
    # FIXME: hardcoded for gilfoyle
    if Path("/data").exists():
        HARDCODED_PATH_GILFOYLE = "/data"
        output_dir = bag3d_export_dir(HARDCODED_PATH_GILFOYLE)
        path_tiles_dir = output_dir.joinpath("tiles")
        path_quadtree_tsv = output_dir.joinpath("quadtree.tsv")
        if path_quadtree_tsv.exists():
            return [row[0] for row in check_export_results(path_quadtree_tsv, path_tiles_dir)]
        else:
            return []
    else:
        return []