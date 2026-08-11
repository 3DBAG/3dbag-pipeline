from enum import StrEnum

import csv
import json
from os import getenv
from pathlib import Path
from typing import Union

from bag3d.specs.core import CityJSONLocation, GpkgLocation, Ogc3dTilesLocation
from dagster import (
    AssetKey,
    AssetIn,
    Config,
    asset,
    get_dagster_logger,
)
from pydantic import Field

from bag3d.common.resources import tool_versions
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.executables import TylerResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.version import ReleaseVersionResource

logger = get_dagster_logger("export.tile")


class TylerOutputFormat(StrEnum):
    OGC3DTILES = "ogc3dtiles"
    GPKG = "gpkg"
    CITYJSON = "cityjson"
    OBJ = "obj"
    TSV = "tsv"


def generate_tyler_config(
    specs: Specs3DBAGResource,
    data_format: TylerOutputFormat,
    locations: Union[
        tuple[CityJSONLocation], tuple[GpkgLocation], tuple[Ogc3dTilesLocation]
    ],
    export_dir: Path,
) -> tuple[list[str], Path]:
    """Generate the CLI parameters for tyler based on the 3DBAG Specifications.

    Args:
        specs: The 3DBAG specifications
        data_format: Tyler output format (multi, ogc3dtiles)
        locations: The data format locations to generate the config for. Can only generate tyler config for ogc3dtiles for one location at a time, because the location contains the Level of Detail and we produce a separate tileset per LoD.
        export_dir:  The location of the export directory where all exported formats are stored.
    Raises:
        ValueError: With `format=='ogc3dtiles'` if `if len(locations) > 1` or `if not isinstance(location, Ogc3dTilesLocation)`.
    """
    cli_params = [
        f"--format={data_format}",
        "--qtree-capacity=280000",
    ]
    output_dir = None
    if data_format == TylerOutputFormat.OGC3DTILES:
        if len(locations) > 1:
            raise ValueError(
                "Can only generate tyler config for ogc3dtiles for one location at a time, because the location contains the Level of Detail and we produce a separate tileset per LoD."
            )
        location = locations[0]
        if not isinstance(location, Ogc3dTilesLocation):
            raise ValueError(
                "With data_format 'ogc3dtiles' the location must be a single Ogc3dTilesLocation."
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
        # TODO: temporary fix until Tyler renames the "3dtiles" format to "ogc3dtiles". We need "ogc3dtiles", because python identifiers cannot start with a number.
        data_format_temp_override = "ogc3dtiles"
        attributes = specs.applies_to(data_format=data_format_temp_override, locations=locations)
        for a_name, a_spec in attributes:
            cli_params.append(f"--object-attribute={a_name}:{a_spec.type.as_geof()}")
    elif data_format == TylerOutputFormat.GPKG:
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--debug-dump-grid",
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
            f"data_format must be one of 'cityjson', 'gpkg', 'obj', 'ogc3dtiles', got {data_format}"
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
        data_format: Either 'multi' or 'ogc3dtiles'. See tyler docs for details.
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


def _write_quadtree_tsv(debug_dir: Path) -> Path:
    """Normalize Tyler level-specific quadtree TSVs to the pipeline schema."""
    quadtree_path = debug_dir.joinpath("quadtree.tsv")
    if quadtree_path.is_file():
        return quadtree_path

    level_paths = sorted(debug_dir.glob("quadtree_level-*.tsv"))
    if not level_paths:
        raise FileNotFoundError(
            f"Tyler did not create quadtree TSV files in {debug_dir}"
        )

    rows = []
    node_ids = set()
    for level_path in level_paths:
        with level_path.open("r", newline="") as fo:
            for row in csv.DictReader(fo, delimiter="\t"):
                node_id = row["node_id"]
                rows.append(row)
                node_ids.add(node_id)

    with quadtree_path.open("w", newline="") as fo:
        writer = csv.writer(fo, delimiter="\t")
        writer.writerow(["id", "level", "nr_items", "leaf", "wkt"])
        for row in rows:
            level, x, y = (int(part) for part in row["node_id"].split("/"))
            child_ids = {
                f"{level + 1}/{2 * x + dx}/{2 * y + dy}"
                for dx in (0, 1)
                for dy in (0, 1)
            }
            writer.writerow(
                [
                    row["node_id"],
                    row["node_level"],
                    row["nr_items"],
                    str(not child_ids.intersection(node_ids)).lower(),
                    row["wkt"],
                ]
            )
    return quadtree_path


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
    export_dir = reconstruction_output_tiles_func(
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
    return export_dir


@asset(
    ins={
        "reconstruction_output_gpkg": AssetIn(
            key=AssetKey(("export", "reconstruction_output_gpkg"))
        )
    },
)
def merged_quadtree(reconstruction_output_gpkg: Path) -> Path:
    """Merge Tyler quadtree level files into the pipeline quadtree TSV."""
    return _write_quadtree_tsv(reconstruction_output_gpkg.joinpath("debug"))


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
    """Tiles for distribution, in Ogc 3D Tiles format, Level of Detail 1.2 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    # TODO: temporary fix until Tyler renames the "3dtiles" format to "ogc3dtiles". We need "ogc3dtiles", because python identifiers cannot start with a number.
    data_format_temp_override = "ogc3dtiles"
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OGC3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Ogc3dTilesLocation.lod12,),
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
    """Tiles for distribution, in Ogc 3D Tiles format, Level of Detail 1.3 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    # TODO: temporary fix until Tyler renames the "3dtiles" format to "ogc3dtiles". We need "ogc3dtiles", because python identifiers cannot start with a number.
    data_format_temp_override = "ogc3dtiles"
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OGC3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Ogc3dTilesLocation.lod13,),
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
    """Tiles for distribution, in Ogc 3D Tiles format, Level of Detail 2.2 buildings.
    Generated with tyler."""
    with metadata.open("r") as fo:
        metadata_lineage = json.load(fo)
    version_3dbag = metadata_lineage["identificationInfo"]["citation"]["edition"]
    # TODO: temporary fix until Tyler renames the "3dtiles" format to "ogc3dtiles". We need "ogc3dtiles", because python identifiers cannot start with a number.
    data_format_temp_override = "ogc3dtiles"
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OGC3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=version_3dbag,
        rayon_num_threads=config.concurrency,
        locations=(Ogc3dTilesLocation.lod22,),
        verbose=config.verbose,
    )
