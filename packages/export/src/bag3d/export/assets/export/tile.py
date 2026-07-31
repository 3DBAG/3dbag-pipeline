from enum import StrEnum

import json
from os import getenv
from pathlib import Path
from typing import Union

from bag3d.specs.core import CityJSONLocation, GpkgLocation, Cesium3dTilesLocation
from dagster import AssetKey, asset, Config, get_dagster_logger
from pydantic import Field

from bag3d.common.resources import tool_versions
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.executables import TylerResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource

logger = get_dagster_logger("export.tile")


class TylerOutputFormat(StrEnum):
    CESIUM3DTILES = "3dtiles"
    GPKG = "gpkg"
    CITYJSON = "cityjson"
    OBJ = "obj"
    TSV = "tsv"


def generate_tyler_config(
    specs: Specs3DBAGResource,
    data_format: TylerOutputFormat,
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
        export_dir:  The location of the export directory where all exported formats are stored.
    Raises:
        ValueError: With `format=='cesium3dtiles'` if `if len(locations) > 1` or `if not isinstance(location, Cesium3dTilesLocation)`.
    """
    cli_params = [
        f"--format={data_format}",
        "--qtree-capacity=280000",
    ]
    output_dir = None
    if data_format == TylerOutputFormat.CESIUM3DTILES:
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
                "--grid-minz=-50",
                "--grid-maxz=400",
                f"--lod-building-part={location.lod}",
                "--object-type=BuildingPart",
                "--include-parent-attributes",
            ]
        )
        attributes = specs.applies_to(data_format=data_format, locations=locations)
        for a_name, a_spec in attributes:
            cli_params.append(f"--object-attribute={a_name}:{a_spec.type.as_geof()}")
    elif data_format == TylerOutputFormat.GPKG:
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--grid-export",
                "--gpkg-split-lod",
                "--gpkg-include-semantics",
                "--gpkg-include-hierarchy",
            ]
        )
    elif data_format == TylerOutputFormat.CITYJSON:
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
            ]
        )
    elif data_format == TylerOutputFormat.OBJ:
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--object-type=BuildingPart",
            ]
        )
    else:
        raise ValueError(
            f"data_format must be one of 'cityjson', 'gpkg', 'obj', 'cesium3dtiles', got {data_format}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    return cli_params, output_dir


def reconstruction_output_tiles_func(
    data_format: TylerOutputFormat,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
    tyler: TylerResource,
    **kwargs,
) -> Path:
    """Run tyler on the reconstruction output directory.

    Args:
        data_format: Either 'multi' or 'cesium3dtiles'. See tyler docs for details.
    """
    reconstructed_root_dir = file_store.stage_dir("floors_estimation")
    export_dir = file_store.stage_subdir("export", version.version)
    logger.debug(f"{reconstructed_root_dir=}")

    num_threads = kwargs["rayon_num_threads"]
    exe_name = "tyler"
    cli_params, output_dir = generate_tyler_config(
        specs=specs,
        data_format=data_format,
        locations=kwargs["locations"],
        export_dir=export_dir,
    )
    cmd = [
        f"RAYON_NUM_THREADS={num_threads}",
        f"RUST_LOG={'debug' if kwargs.get('verbose', False) else 'info'}",
        "{exe}",
    ]
    cmd.extend(cli_params)
    # Append the input directory path as last
    cmd.append(str(reconstructed_root_dir))
    logger.debug(" ".join(cmd))
    tyler.runner.run(
        " ".join(cmd),
        exe_name=exe_name,
        cwd=str(output_dir),
        logger=logger,
    )
    return output_dir


class TylerConfig(Config):
    concurrency: int = Field(
        default_factory=lambda: int(getenv("BAG3D_CONCURRENCY_TOOL_TYLER", "1")),
        description="RAYON_NUM_THREADS for tyler",
    )
    verbose: bool = False


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_cityjson(
    config: TylerConfig,
    metadata,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Path:
    """Tiles for distribution in CityJSON format.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.CITYJSON,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=tuple(),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_gpkg(
    config: TylerConfig,
    metadata,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Path:
    """Tiles for distribution in GPKG format.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.GPKG,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=tuple(),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_obj(
    config: TylerConfig,
    metadata,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Path:
    """Tiles for distribution OBJ format.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OBJ,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=tuple(),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_3dtiles_lod12(
    config: TylerConfig,
    metadata,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Path:
    """Tiles for distribution, in Cesium 3D Tiles format, Level of Detail 1.2 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.CESIUM3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Cesium3dTilesLocation.lod12,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_3dtiles_lod13(
    config: TylerConfig,
    metadata,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Path:
    """Tiles for distribution, in Cesium 3D Tiles format, Level of Detail 1.3 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.CESIUM3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Cesium3dTilesLocation.lod13,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_3dtiles_lod22(
    config: TylerConfig,
    metadata,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Path:
    """Tiles for distribution, in Cesium 3D Tiles format, Level of Detail 2.2 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.CESIUM3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Cesium3dTilesLocation.lod22,),
        verbose=config.verbose,
    )
