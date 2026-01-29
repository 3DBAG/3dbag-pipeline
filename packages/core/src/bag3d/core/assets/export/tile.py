import json
import os
from pathlib import Path
from typing import Union

from bag3d.specs.core import CityJSONLocation, GpkgLocation, Cesium3dTilesLocation
from dagster import AssetKey, asset, Config

from bag3d.common.resources import resource_defs, Specs3DBAGResource
from bag3d.common.utils.files import geoflow_crop_dir, bag3d_dir, bag3d_export_dir


def create_sequence_header_file(template_file, output_file, version_3dbag):
    """Create the CityJSON metadata file."""
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


def generate_tyler_config(
    specs: Specs3DBAGResource,
    data_format: str,
    locations: Union[
        tuple[CityJSONLocation], tuple[GpkgLocation], tuple[Cesium3dTilesLocation]
    ],
    export_dir: Path,
) -> tuple[list[str], Path]:
    """Generate the CLI parameters for tyler based on the 3DBAG Specifications.

    Args:
        specs: The 3DBAG specifications
        data_format: Tyler output format (multi, cesium3dtiles)
        locations: The data format locations to generate the config for. Can only generate tyler config for cesium3dtiles for one location at a time, because the location contains the Level of Detail and we produce a separate tileset per LoD.
        output_dir: The directory there tyler will write the output
    Raises:
        ValueError: With `format=='cesium3dtiles'` if `if len(locations) > 1` or `if not isinstance(location, Cesium3dTilesLocation)`.
    """
    cli_params = []
    output_dir = None
    if data_format == "cesium3dtiles":
        if len(locations) > 1:
            raise ValueError(
                "Can only generate tyler config for cesium3dtiles for one location at a time, because the location contains the Level of Detail and we produce a separate tileset per LoD."
            )
        location = locations[0]
        if not isinstance(location, Cesium3dTilesLocation):
            raise ValueError(
                "With data_format 'cesium3dtiles' the location must be a single Cesium3dTilesLocation."
            )
        output_dir = export_dir.joinpath(data_format, str(location))
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--3dtiles-metadata-class=building",
                "--qtree-capacity=280000",
                "--grid-minz=-50",
                "--grid-maxz=400",
                "--object-type=Building",
                "--object-type=BuildingPart",
                f"--lod-building-part={location.lod}",
                f"--lod-building={location.lod}",
            ]
        )
        attributes = specs.applies_to(data_format=data_format, locations=locations)
        for a_name, a_spec in attributes:
            cli_params.append(f"--object-attribute={a_name}:{a_spec.type.as_geof()}")
    elif data_format == "multi":
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--format=multi",
                "--object-type=Building",
                "--object-type=BuildingPart",
                "--qtree-capacity=280000",
                "--grid-export",
            ]
        )
    else:
        raise ValueError(
            f"data_format must be one of 'multi', 'cesium3dtiles', got {data_format}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    return cli_params, output_dir


def reconstruction_output_tiles_func(context, data_format: str, **kwargs):
    """Run tyler on the reconstruction output directory.

    Args:
        data_format: Either 'multi' or 'cesium3dtiles'. See tyler docs for details.
    """
    reconstructed_root_dir = geoflow_crop_dir(
        context.resources.file_store_fastssd.file_store.data_dir
    )
    export_dir = bag3d_export_dir(
        context.resources.file_store.file_store.data_dir,
        version=context.resources.version.version,
    )
    context.log.debug(f"{reconstructed_root_dir=}")
    version_3dbag: str = kwargs["version_3dbag"]

    sequence_header_file = (
        bag3d_dir(context.resources.file_store_fastssd.file_store.data_dir)
        / "metadata.json"
    )
    create_sequence_header_file(
        os.getenv("TYLER_METADATA_JSON"), sequence_header_file, version_3dbag
    )
    num_threads = kwargs["rayon_num_threads"]
    cmd = [
        f"RAYON_NUM_THREADS={num_threads}",
        f"RUST_LOG={'debug' if kwargs.get('verbose', False) else 'info'}",
        f"TYLER_RESOURCES_DIR={os.getenv('TYLER_RESOURCES_DIR')}",
        "{exe}",
        "--metadata",
        str(sequence_header_file),
        "--features",
        str(reconstructed_root_dir),
        "--exe-geof",
        str(context.resources.geoflow.app.exes["geof"]),
    ]
    if data_format == "multi":
        exe_name = "tyler-multiformat"
    elif data_format == "cesium3dtiles":
        exe_name = "tyler"
    else:
        raise ValueError(
            f"invalid data_format: {data_format}, only 'multi' and 'cesium3dtiles' are allowed"
        )
    cli_params, output_dir = generate_tyler_config(
        specs=context.resources.specs,
        data_format=data_format,
        locations=kwargs["locations"],
        export_dir=export_dir,
    )
    cmd.extend(cli_params)
    context.log.debug(" ".join(cmd))
    context.resources.tyler.runner.run(
        " ".join(cmd),
        exe_name=exe_name,
        cwd=str(output_dir),
        context=context,
    )
    return output_dir


class TylerConfig(Config):
    concurrency: int
    verbose: bool = False


@asset(
    deps={AssetKey(("reconstruction", "reconstructed_building_models_nl"))},
    code_version=resource_defs["tyler"].runner.version("tyler-multiformat"),
    required_resource_keys={
        "tyler",
        "geoflow",
        "file_store",
        "file_store_fastssd",
        "version",
        "specs",
    },
)
def reconstruction_output_multitiles_nl(context, config: TylerConfig, metadata):
    """Tiles for distribution, in CityJSON, OBJ, GPKG formats.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        context,
        data_format="multi",
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=tuple(),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("reconstruction", "reconstructed_building_models_nl"))},
    code_version=resource_defs["tyler"].runner.version("tyler"),
    required_resource_keys={
        "tyler",
        "geoflow",
        "file_store",
        "file_store_fastssd",
        "version",
        "specs",
    },
)
def reconstruction_output_3dtiles_lod12_nl(context, config: TylerConfig, metadata):
    """Tiles for distribution, in Cesium 3D Tiles format, Level of Detail 1.2 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        context,
        data_format="cesium3dtiles",
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Cesium3dTilesLocation.lod12,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("reconstruction", "reconstructed_building_models_nl"))},
    code_version=resource_defs["tyler"].runner.version("tyler"),
    required_resource_keys={
        "tyler",
        "geoflow",
        "file_store",
        "file_store_fastssd",
        "version",
        "specs",
    },
)
def reconstruction_output_3dtiles_lod13_nl(context, config: TylerConfig, metadata):
    """Tiles for distribution, in Cesium 3D Tiles format, Level of Detail 1.3 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        context,
        data_format="cesium3dtiles",
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Cesium3dTilesLocation.lod13,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("reconstruction", "reconstructed_building_models_nl"))},
    code_version=resource_defs["tyler"].runner.version("tyler"),
    required_resource_keys={
        "tyler",
        "geoflow",
        "file_store",
        "file_store_fastssd",
        "version",
        "specs",
    },
)
def reconstruction_output_3dtiles_lod22_nl(context, config: TylerConfig, metadata):
    """Tiles for distribution, in Cesium 3D Tiles format, Level of Detail 2.2 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        context,
        data_format="cesium3dtiles",
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Cesium3dTilesLocation.lod22,),
        verbose=config.verbose,
    )
