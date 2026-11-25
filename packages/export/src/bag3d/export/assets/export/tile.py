import csv
import shlex
from enum import StrEnum
from os import getenv
from pathlib import Path

from bag3d.common.resources import tool_versions
from bag3d.common.resources.executables import TylerResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.specs import Specs3DBAGResource
from bag3d.common.resources.version import ReleaseVersionResource
from dagster import (
    AssetIn,
    AssetKey,
    Config,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
)
from pydantic import Field

from bag3d.export.assets.export.metadata import read_export_edition

logger = get_dagster_logger("export.tile")


class LevelOfDetail(StrEnum):
    lod12 = "lod12"
    lod13 = "lod13"
    lod22 = "lod22"

    @property
    def lod(self) -> str:
        return f"{self.value[3]}.{self.value[4]}"


class TylerOutputFormat(StrEnum):
    OGC3DTILES = "ogc3dtiles"
    GPKG = "gpkg"
    CITYJSON = "cityjson"
    OBJ = "obj"
    TSV = "tsv"


def generate_tyler_config(
    specs: Specs3DBAGResource,
    data_format: TylerOutputFormat,
    locations: tuple[LevelOfDetail, ...],
    export_dir: Path,
) -> tuple[list[str], Path]:
    """Generate the CLI parameters for tyler based on the 3DBAG Specifications.

    Args:
        specs: The 3DBAG specifications
        data_format: Tyler output format (multi, ogc3dtiles)
        locations: The data format locations to generate the config for. Can only generate tyler config for ogc3dtiles for one location at a time, because the location contains the Level of Detail and we produce a separate tileset per LoD.
        export_dir:  The location of the export directory where all exported formats are stored.
    Raises:
        ValueError: With `format=='ogc3dtiles'` if `if len(locations) > 1` or `if not isinstance(location, LevelOfDetail)`.
    """
    cli_params = [
        f"--format={data_format}",
        "--qtree-capacity=280000",
    ]
    output_dir = None
    if data_format == TylerOutputFormat.OGC3DTILES:
        if len(locations) != 1:
            raise ValueError(
                "Tyler ogc3dtiles export requires exactly one level of detail. "
                f"Received {locations!r}. Configure a separate export per LoD."
            )
        location = locations[0]
        if not isinstance(location, LevelOfDetail):
            raise ValueError(
                "Tyler ogc3dtiles export requires a LevelOfDetail value. "
                f"Received {location!r}."
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
        projected = specs.tileset(location.value)
        for name in projected.properties:
            # Tyler's CLI vocabulary belongs to the exporter adapter.
            base_type = specs.attributes[name].type.base_type.value
            tyler_type = {
                "date": "string",
                "datetime": "string",
                "array": "string",
            }.get(base_type, base_type)
            cli_params.append(f"--object-attribute={name}:{tyler_type}")
    elif data_format == TylerOutputFormat.GPKG:
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--debug-dump-grid",
                "--gpkg-split-lod",
                "--gpkg-include-semantics",
                "--gpkg-include-hierarchy",
                "--lod-building",
                "0",
                "--lod-building-part",
                "1.2",
                "--lod-building-part",
                "1.3",
                "--lod-building-part",
                "2.2",
            ]
        )
    elif data_format == TylerOutputFormat.CITYJSON:
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--lod-building",
                "0",
                "--lod-building-part",
                "1.2",
                "--lod-building-part",
                "1.3",
                "--lod-building-part",
                "2.2",
            ]
        )
    elif data_format == TylerOutputFormat.OBJ:
        if len(locations) != 1:
            raise ValueError(
                "Tyler obj export requires exactly one level of detail. "
                f"Received {locations!r}. Configure a separate export per LoD."
            )
        location = locations[0]
        if not isinstance(location, LevelOfDetail):
            raise ValueError(
                "Tyler obj export requires a LevelOfDetail value. "
                f"Received {location!r}."
            )
        output_dir = export_dir
        cli_params.extend(
            [
                f"--output={output_dir}",
                "--object-type=BuildingPart",
                f"--lod-building-part={location.lod}",
                f"--output-suffix=lod{location.lod.replace('.', '')}",
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
    *,
    version_3dbag: str,
    rayon_num_threads: int,
    locations: tuple[LevelOfDetail, ...],
    verbose: bool,
) -> Output[Path]:
    """Run Tyler on the reconstruction output directory."""
    reconstructed_root_dir = file_store.stage_dir("floors_estimation")
    export_dir = file_store.stage_subdir("export", version.version)
    cli_params, output_dir = generate_tyler_config(
        specs=specs,
        data_format=data_format,
        locations=locations,
        export_dir=export_dir,
    )
    cmd = [
        f"RAYON_NUM_THREADS={rayon_num_threads}",
        f"RUST_LOG={'debug' if verbose else 'info'}",
        "{exe}",
    ]
    # Quote CLI arguments independently of the shell environment assignments.
    cmd.append(shlex.join([*cli_params, str(reconstructed_root_dir)]))
    lods = ", ".join(location.lod for location in locations) or "0, 1.2, 1.3, 2.2"
    operation = (
        f"Tyler {data_format} export for {version_3dbag} (LoD {lods}) "
        f"from {reconstructed_root_dir} to {output_dir}"
    )
    logger.info("Starting %s with %d threads", operation, rayon_num_threads)
    try:
        result = tyler.runner.run(
            " ".join(cmd), exe_name="tyler", cwd=str(output_dir), logger=logger
        )
    except Exception as exc:
        raise RuntimeError(f"Could not run {operation}") from exc
    if not result.success:
        detail = (
            result.stderr.strip() or result.stdout.strip() or "no diagnostic output"
        )
        raise RuntimeError(
            f"{operation} failed (exit {result.returncode}). {detail[-1000:]}"
        )
    return Output(
        output_dir,
        metadata={
            "artifact path": MetadataValue.path(output_dir),
            "input directory": MetadataValue.path(reconstructed_root_dir),
            "format": str(data_format),
            "LoDs": lods,
            "release": version_3dbag,
        },
    )


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
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution in CityJSON format.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.CITYJSON,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(),
        verbose=config.verbose,
    )


def _write_quadtree_tsv(debug_dir: Path) -> Path:
    """Normalize Tyler level-specific quadtree TSVs to the pipeline schema."""
    quadtree_path = debug_dir.joinpath("quadtree.tsv")
    if quadtree_path.is_file():
        logger.info("Using existing Tyler quadtree at %s", quadtree_path)
        return quadtree_path

    level_paths = sorted(debug_dir.glob("quadtree_level-*.tsv"))
    if not level_paths:
        raise FileNotFoundError(
            f"No quadtree.tsv or quadtree_level-*.tsv files in {debug_dir}. "
            "Check the reconstruction_output_gpkg logs for Tyler's --debug-dump-grid output."
        )

    rows = []
    node_ids = set()
    required_fields = {"node_id", "node_level", "nr_items", "wkt"}
    for level_path in level_paths:
        with level_path.open("r", newline="") as fo:
            reader = csv.DictReader(fo, delimiter="\t")
            missing = required_fields.difference(reader.fieldnames or ())
            if missing:
                raise ValueError(
                    f"Tyler quadtree {level_path} is missing columns {sorted(missing)}. "
                    "Check the Tyler debug output format."
                )
            for row in reader:
                try:
                    node_id = row["node_id"]
                    level, x, y = (int(part) for part in node_id.split("/"))
                except (AttributeError, ValueError) as exc:
                    raise ValueError(
                        f"Invalid node_id {row['node_id']!r} in {level_path} "
                        f"at line {reader.line_num}. Expected level/x/y as integers."
                    ) from exc
                rows.append((row, level, x, y))
                node_ids.add(node_id)

    with quadtree_path.open("w", newline="") as fo:
        writer = csv.writer(fo, delimiter="\t")
        writer.writerow(["id", "level", "nr_items", "leaf", "wkt"])
        for row, level, x, y in rows:
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
    logger.info(
        "Merged %d quadtree nodes from %d TSV files into %s",
        len(rows),
        len(level_paths),
        quadtree_path,
    )
    return quadtree_path


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_gpkg(
    config: TylerConfig,
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution in GPKG format.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.GPKG,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(),
        verbose=config.verbose,
    )


@asset(
    ins={
        "reconstruction_output_gpkg": AssetIn(
            key=AssetKey(("export", "reconstruction_output_gpkg"))
        )
    },
)
def merged_quadtree(reconstruction_output_gpkg: Path) -> Output[Path]:
    """Merge Tyler quadtree level files into the pipeline quadtree TSV."""
    quadtree_path = _write_quadtree_tsv(reconstruction_output_gpkg.joinpath("debug"))
    return Output(
        quadtree_path, metadata={"artifact path": MetadataValue.path(quadtree_path)}
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_obj_lod12(
    config: TylerConfig,
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution, in OBJ format, LoD1.2 BuildingPart.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OBJ,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(LevelOfDetail.lod12,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_obj_lod13(
    config: TylerConfig,
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution, in OBJ format, LoD1.3 BuildingPart.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OBJ,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(LevelOfDetail.lod13,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_obj_lod22(
    config: TylerConfig,
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution, in OBJ format, LoD2.2 BuildingPart.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OBJ,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(LevelOfDetail.lod22,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_3dtiles_lod12(
    config: TylerConfig,
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution, in Ogc 3D Tiles format, Level of Detail 1.2 buildings.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OGC3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(LevelOfDetail.lod12,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_3dtiles_lod13(
    config: TylerConfig,
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution, in Ogc 3D Tiles format, Level of Detail 1.3 buildings.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OGC3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(LevelOfDetail.lod13,),
        verbose=config.verbose,
    )


@asset(
    deps={AssetKey(("floors_estimation", "save_cjfiles"))},
    code_version=tool_versions.get_version("tyler"),
    pool="tyler",
)
def reconstruction_output_3dtiles_lod22(
    config: TylerConfig,
    metadata: Path,
    tyler: TylerResource,
    file_store: FileStoreResource,
    version: ReleaseVersionResource,
    specs: Specs3DBAGResource,
) -> Output[Path]:
    """Tiles for distribution, in Ogc 3D Tiles format, Level of Detail 2.2 buildings.
    Generated with tyler."""
    return reconstruction_output_tiles_func(
        data_format=TylerOutputFormat.OGC3DTILES,
        file_store=file_store,
        version=version,
        specs=specs,
        tyler=tyler,
        version_3dbag=read_export_edition(metadata),
        rayon_num_threads=config.concurrency,
        locations=(LevelOfDetail.lod22,),
        verbose=config.verbose,
    )
