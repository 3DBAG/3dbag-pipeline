import json
import os

from dagster import AssetKey, asset

from bag3d.common.utils.files import geoflow_crop_dir, bag3d_dir, bag3d_export_dir


def create_sequence_header_file(template_file, output_file, version_3dbag):
    with open(template_file, "r") as f:
        header = json.load(f)
        header["metadata"]["version"] = (
            version_3dbag  # example version string: "v2023.10.08"
        )
        metadata_url = "https://data.3dbag.nl/metadata/{}/metadata.json".format(
            version_3dbag.replace(".", "")
        )
        header["metadata"]["fullMetadataUrl"] = metadata_url

    with open(output_file, "w") as f:
        json.dump(header, f)


def reconstruction_output_tiles_func(context, format: str, **kwargs: dict):
    """Run tyler on the reconstruction output directory.
    Format is either 'multi' or '3dtiles'. See tyler docs for details.
    """
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.file_store.data_dir
    )
    output_dir = bag3d_export_dir(
        context.resources.file_store.file_store.data_dir,
        version=context.resources.version.version,
    )
    context.log.debug(f"{reconstructed_root_dir=}")
    version_3dbag = kwargs["version_3dbag"]

    sequence_header_file = (
        bag3d_dir(context.resources.file_store_fastssd.file_store.data_dir)
        / "metadata.json"
    )
    create_sequence_header_file(
        os.getenv("TYLER_METADATA_JSON"), sequence_header_file, version_3dbag
    )
    # # Set the parallelism in tyler from the dagster instance configuration (the dagster.yaml in $DAGSTER_HOME)
    # num_threads = context.instance.run_coordinator.inst_data.config_dict["max_concurrent_runs"]
    num_threads = 40
    cmd = [
        f"RAYON_NUM_THREADS={num_threads}",
        "RUST_LOG=info",
        f"TYLER_RESOURCES_DIR={os.getenv('TYLER_RESOURCES_DIR')}",
        "{exe}",
        "--metadata",
        str(sequence_header_file),
        "--features",
        str(reconstructed_root_dir),
        "--output",
        str(output_dir),
        "--format",
        format.lower(),
        "--exe-geof",
        str(context.resources.geoflow.app.exes["geof"]),
        "--object-type",
        "Building",
        "--object-type",
        "BuildingPart",
        "--qtree-capacity",
        "280000",
    ]
    if format == "multi":
        cmd.append("--grid-export")
    elif format == "3dtiles":
        cmd.extend(
            [
                "--3dtiles-metadata-class",
                "building",
                "--object-attribute",
                "bouwjaar:int",
                "--3dtiles-implicit",
                "--lod-building",
                "2.2",
                "--lod-building-part",
                "2.2",
            ]
        )
    else:
        raise ValueError(
            f"invalid format: {format}, only 'multi' and '3dtiles' are allowed"
        )
    context.log.debug(" ".join(cmd))
    context.resources.tyler.app.execute("tyler", " ".join(cmd), cwd=str(output_dir))
    return output_dir


@asset(
    deps={AssetKey(("reconstruction", "reconstructed_building_models_nl"))},
    required_resource_keys={
        "tyler",
        "geoflow",
        "file_store",
        "file_store_fastssd",
        "version",
    },
)
def reconstruction_output_multitiles_nl(context, metadata):
    """Tiles for distribution, in CityJSON, OBJ, GPKG formats.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        context, format="multi", version_3dbag=version_3dbag
    )
