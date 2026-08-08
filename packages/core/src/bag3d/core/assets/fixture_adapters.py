"""Read-only adapters for the local integration-data snapshot."""

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from dagster import AssetExecutionContext, Output, asset
from bag3d.common.resources.files import FileStoreResource
from bag3d.core.assets.ahn.core import (
    partition_definition_ahn,
    partition_definition_ahn6_batches,
    tiles_in_batch,
)
from bag3d.core.assets.ahn.download import BatchLAZDownload, LAZDownload

FIXTURE_VERSION = "2"


@dataclass(frozen=True)
class FixtureManifest:
    root: Path
    value: Mapping[str, Any]

    def path(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        try:
            path.relative_to(self.root.resolve())
        except ValueError as exc:
            raise ValueError(
                f"Fixture path escapes snapshot root: {relative!r}"
            ) from exc
        if not path.exists():
            raise FileNotFoundError(f"Fixture path is missing: {relative}")
        return path


def _verify(root: Path, manifest: Mapping[str, Any], relative: str) -> None:
    files = manifest.get("files")
    path = (root / relative).resolve()
    if path.is_dir():
        for child in path.rglob("*"):
            if child.is_file() and str(child.relative_to(root)) not in files:
                raise ValueError(
                    f"Fixture manifest does not declare required file {child.relative_to(root)!s}"
                )
        return
    if not isinstance(files, Mapping) or relative not in files:
        raise ValueError(
            f"Fixture manifest does not declare required file {relative!r}"
        )
    if not path.is_file():
        raise FileNotFoundError(f"Fixture file is missing: {relative}")
    entry = (
        files[relative]
        if isinstance(files[relative], Mapping)
        else {"size": files[relative]}
    )
    if entry.get("size") is not None and path.stat().st_size != int(entry["size"]):
        raise ValueError(f"Fixture file size does not match manifest: {relative}")
    if (
        entry.get("sha256")
        and hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]
    ):
        raise ValueError(f"Fixture checksum does not match manifest: {relative}")


def validate_fixture_manifest(root: Path) -> FixtureManifest:
    root = root.resolve()
    path = root / "manifest.json"
    if not path.is_file():
        raise FileNotFoundError(
            f"Integration-data snapshot is incomplete: {path} is missing"
        )
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid integration-data manifest: {path}") from exc
    if not isinstance(manifest, Mapping):
        raise ValueError("Integration-data manifest must contain a JSON object")
    if str(manifest.get("fixture_version")) != FIXTURE_VERSION:
        raise ValueError(
            f"Unsupported integration-data fixture version {manifest.get('fixture_version')!r}; expected {FIXTURE_VERSION!r}"
        )
    aoi = manifest.get("aoi")
    expected_aoi = {"wkt", "minx", "miny", "maxx", "maxy"}
    if not isinstance(aoi, Mapping) or not expected_aoi.issubset(aoi):
        raise ValueError("Integration-data manifest is missing AOI metadata")
    clipping = manifest.get("clipping")
    if not isinstance(clipping, Mapping) or clipping.get("vectors") != "gdal" or clipping.get("pointclouds") != "las2las_keep_xy":
        raise ValueError("Integration-data manifest has unsupported clipping metadata")
    validation = manifest.get("validation")
    if not isinstance(validation, Mapping) or validation.get("buffer_metres") != 10.0:
        raise ValueError("Integration-data manifest is missing 10 m validation metadata")
    extents = validation.get("extents")
    if not isinstance(extents, Mapping) or not isinstance(extents.get("vectors"), Mapping) or not isinstance(extents.get("pointclouds"), Mapping):
        raise ValueError("Integration-data manifest is missing computed extents")
    bounds = (float(aoi["minx"]) - 10.0, float(aoi["miny"]) - 10.0, float(aoi["maxx"]) + 10.0, float(aoi["maxy"]) + 10.0)
    for category in ("vectors", "pointclouds"):
        for name, values in extents[category].items():
            if not isinstance(name, str) or not isinstance(values, list):
                raise ValueError("Invalid computed extent metadata")
            values_to_check = values if category == "vectors" else [values]
            for extent in values_to_check:
                if not isinstance(extent, list) or len(extent) != 4:
                    raise ValueError(f"Invalid computed extent metadata for {name}")
                extent_values = tuple(float(value) for value in extent)
                if not (bounds[0] <= extent_values[0] and bounds[1] <= extent_values[1] and extent_values[2] <= bounds[2] and extent_values[3] <= bounds[3]):
                    raise ValueError(f"Computed extent for {name} exceeds the AOI plus 10 m")
    sources = manifest.get("sources")
    required = {"bag", "bgt", "top10nl", "cbs_key_figures", "cbs_buurtkaart"}
    if not isinstance(sources, Mapping):
        raise ValueError("Integration-data manifest is missing sources")
    if missing := required - set(sources):
        raise ValueError(
            f"Integration-data manifest is missing sources: {sorted(missing)}"
        )
    ahn = manifest.get("ahn")
    if (
        not isinstance(ahn, Mapping)
        or not isinstance(ahn.get("indexes"), Mapping)
        or not isinstance(ahn.get("checksums"), Mapping)
        or not isinstance(ahn.get("partitions"), Mapping)
    ):
        raise ValueError(
            "Integration-data manifest is missing AHN indexes, checksums, or partitions"
        )
    if not {"3", "6"}.issubset(ahn["indexes"]):
        raise ValueError("Integration-data manifest must contain AHN3 and AHN6 indexes")
    if not {"3", "4", "5", "6"}.issubset(ahn["checksums"]):
        raise ValueError("Integration-data manifest must contain AHN3-AHN6 checksums")
    for version, partitions in ahn["partitions"].items():
        if not isinstance(partitions, Mapping):
            raise ValueError(f"Invalid AHN{version} partition mapping")
        values = (
            entry
            for batch in partitions.values()
            for entry in (
                batch.values()
                if version == "6" and isinstance(batch, Mapping)
                else [batch]
            )
        )
        for entry in values:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("path"), str):
                raise ValueError(f"Invalid AHN{version} partition entry")
            _verify(root, manifest, entry["path"])
    files = manifest.get("files")
    if not isinstance(files, Mapping):
        raise ValueError("Integration-data manifest is missing files")
    actual = {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file() and path.name != "manifest.json"}
    if actual != set(files):
        raise ValueError("Integration-data manifest file list does not match snapshot")
    for relative in actual:
        _verify(root, manifest, relative)
    for name, source in sources.items():
        if not isinstance(source, Mapping) or not isinstance(source.get("path"), str):
            raise ValueError(
                f"Invalid source entry in integration-data manifest: {name}"
            )
        _verify(root, manifest, source["path"])
    return FixtureManifest(root, manifest)


def _m(store):
    return validate_fixture_manifest(Path(store.root_dir))


def _src(m, name):
    return m.value["sources"][name]


def _path(m, name):
    return m.path(_src(m, name)["path"])


@asset(name="extract_bag", key_prefix="bag", group_name="bag")
def fixture_extract_bag(integration_data_store: FileStoreResource):
    m = _m(integration_data_store)
    s = _src(m, "bag")
    return Output((_path(m, "bag"), dict(s["metadata"]), str(s["shortdate"])))


@asset(name="extract_bgt", key_prefix="bgt", group_name="bgt")
def fixture_extract_bgt(integration_data_store: FileStoreResource):
    return _path(_m(integration_data_store), "bgt")


@asset(name="extract_top10nl", key_prefix="top10nl", group_name="top10nl")
def fixture_extract_top10nl(integration_data_store: FileStoreResource):
    return _path(_m(integration_data_store), "top10nl")


@asset(name="extract_cbs_key_figures", key_prefix="cbs", group_name="cbs")
def fixture_extract_cbs_key_figures(integration_data_store: FileStoreResource):
    value = json.loads(
        _path(_m(integration_data_store), "cbs_key_figures").read_text(encoding="utf-8")
    )
    if not isinstance(value, list):
        raise ValueError("Fixture CBS key figures must be a JSON list")
    return value


@asset(name="extract_cbs_buurtkaart", key_prefix="cbs", group_name="cbs")
def fixture_extract_cbs_buurtkaart(integration_data_store: FileStoreResource):
    return _path(_m(integration_data_store), "cbs_buurtkaart")


def _ahn(store, version):
    value = _m(store).value["ahn"]["indexes"].get(str(version))
    if not isinstance(value, Mapping):
        raise ValueError(f"Fixture AHN{version} index is missing")
    return dict(value)


@asset(name="tile_index_ahn", key_prefix="ahn", group_name="ahn")
def fixture_tile_index_ahn(integration_data_store: FileStoreResource):
    return _ahn(integration_data_store, 3)


@asset(name="tile_index_ahn6", key_prefix="ahn", group_name="ahn")
def fixture_tile_index_ahn6(integration_data_store: FileStoreResource):
    return _ahn(integration_data_store, 6)


def _checks(store, version):
    value = _m(store).value["ahn"]["checksums"].get(str(version))
    if not isinstance(value, Mapping):
        raise ValueError(f"Fixture AHN{version} checksums are missing")
    return {str(k): str(v) for k, v in value.items()}


@asset(name="md5_ahn3", key_prefix="ahn", group_name="ahn")
def fixture_md5_ahn3(integration_data_store: FileStoreResource):
    return _checks(integration_data_store, 3)


@asset(name="md5_ahn4", key_prefix="ahn", group_name="ahn")
def fixture_md5_ahn4(integration_data_store: FileStoreResource):
    return _checks(integration_data_store, 4)


@asset(name="sha256_ahn5", key_prefix="ahn", group_name="ahn")
def fixture_sha256_ahn5(integration_data_store: FileStoreResource):
    return _checks(integration_data_store, 5)


@asset(name="sha256_ahn6", key_prefix="ahn", group_name="ahn")
def fixture_sha256_ahn6(integration_data_store: FileStoreResource):
    return _checks(integration_data_store, 6)


def _laz(store, version, tile, pointcloud_store):
    m = _m(store)
    entries = m.value["ahn"]["partitions"].get(str(version), {})
    entry = entries.get(tile)
    if version == 6 and entry is None:
        entry = next(
            (
                batch.get(tile)
                for batch in entries.values()
                if isinstance(batch, Mapping) and tile in batch
            ),
            None,
        )
    if not isinstance(entry, Mapping) or not isinstance(entry.get("path"), str):
        raise ValueError(f"Fixture AHN{version} partition is missing tile {tile}")
    source = m.path(entry["path"])
    destination = Path(pointcloud_store.root_dir) / "integration-fixture" / f"AHN{version}" / source.name
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.is_file() or destination.stat().st_size != source.stat().st_size:
        shutil.copy2(source, destination)
    source_lax = source.with_suffix(".lax")
    destination_lax = destination.with_suffix(".lax")
    if source_lax.is_file() and (not destination_lax.is_file() or destination_lax.stat().st_size != source_lax.stat().st_size):
        shutil.copy2(source_lax, destination_lax)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    expected = entry.get("sha256")
    if expected and digest != expected:
        raise ValueError(f"Fixture AHN{version} checksum does not match manifest: {source}")
    return LAZDownload(str(entry.get("url", source.name)), destination, True, "sha256", digest, False, destination.stat().st_size / 1e6)


def _adapter(version):
    def adapter(
        context: AssetExecutionContext,
        integration_data_store: FileStoreResource,
        pointcloud_store: FileStoreResource,
    ):
        value = _laz(integration_data_store, version, context.partition_key, pointcloud_store)
        return Output(value, metadata=value.asdict())

    return adapter


fixture_laz_files_ahn3 = asset(
    name="laz_files_ahn3",
    key_prefix="ahn",
    group_name="ahn",
    partitions_def=partition_definition_ahn,
)(_adapter(3))
fixture_laz_files_ahn4 = asset(
    name="laz_files_ahn4",
    key_prefix="ahn",
    group_name="ahn",
    partitions_def=partition_definition_ahn,
)(_adapter(4))
fixture_laz_files_ahn5 = asset(
    name="laz_files_ahn5",
    key_prefix="ahn",
    group_name="ahn",
    partitions_def=partition_definition_ahn,
)(_adapter(5))


@asset(
    name="laz_files_ahn6",
    key_prefix="ahn",
    group_name="ahn",
    partitions_def=partition_definition_ahn6_batches,
)
def fixture_laz_files_ahn6(
    context: AssetExecutionContext,
    integration_data_store: FileStoreResource,
    pointcloud_store: FileStoreResource,
):
    m = _m(integration_data_store)
    entries = m.value["ahn"]["partitions"].get("6", {}).get(context.partition_key, {})
    tiles = {
        tile: _laz(integration_data_store, 6, tile, pointcloud_store)
        for tile in tiles_in_batch(context.partition_key)
        if tile in entries
    }
    return Output(
        BatchLAZDownload(context.partition_key, tiles),
        metadata={"batch": context.partition_key, "tiles": len(tiles)},
    )


fixture_assets = [
    fixture_extract_bag,
    fixture_extract_bgt,
    fixture_extract_top10nl,
    fixture_extract_cbs_key_figures,
    fixture_extract_cbs_buurtkaart,
    fixture_tile_index_ahn,
    fixture_tile_index_ahn6,
    fixture_md5_ahn3,
    fixture_md5_ahn4,
    fixture_sha256_ahn5,
    fixture_sha256_ahn6,
    fixture_laz_files_ahn3,
    fixture_laz_files_ahn4,
    fixture_laz_files_ahn5,
    fixture_laz_files_ahn6,
]
