"""Monthly, small-area source-data fixtures.

The assets in this module deliberately write files instead of loading data into
PostgreSQL.  The resulting directory is a portable test fixture and its manifest
is the commit marker for a complete snapshot.
"""

import json
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping
from zipfile import ZIP_DEFLATED, ZipFile

from dagster import AssetIn, AssetKey, AutomationCondition, Config, asset
from pydantic import Field

from bag3d.common.resources.files import FileStoreResource
from bag3d.common.resources.executables import GDALResource
from bag3d.common.utils.requests import download_file

AOI_WKT = "POLYGON ((121967 485750, 123354 485750, 123354 486550, 121967 486550, 121967 485750))"
AHN6_AOI_TILE_ALLOWLIST = [
    "121000_485000",
    "121000_486000",
    "122000_485000",
    "122000_486000",
    "123000_485000",
    "123000_486000",
]
_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


class IntegrationDataConfig(Config):
    """Configuration shared by the fixture assets."""

    geofilter: str = Field(default=AOI_WKT)
    cbs_year: str = Field(default="2025")
    ahn6_allowlist: list[str] = Field(
        default_factory=lambda: AHN6_AOI_TILE_ALLOWLIST.copy()
    )


def _aoi_bbox(wkt: str) -> tuple[float, float, float, float]:
    values = [float(v) for v in _NUMBER.findall(wkt)]
    if len(values) < 8:
        raise ValueError(f"Expected a polygon WKT, got {wkt!r}")
    points = list(zip(values[0::2], values[1::2]))
    return (
        min(x for x, _ in points),
        min(y for _, y in points),
        max(x for x, _ in points),
        max(y for _, y in points),
    )


def _bbox_intersects(
    a: tuple[float, float, float, float], b: tuple[float, float, float, float]
) -> bool:
    """Return true only for positive-area intersection."""
    return max(a[0], b[0]) < min(a[2], b[2]) and max(a[1], b[1]) < min(a[3], b[3])


def _geometry_bbox(geometry: Mapping[str, Any]) -> tuple[float, float, float, float]:
    coordinates = geometry.get("coordinates", [])
    values: list[float] = []

    def visit(value: Any) -> None:
        if isinstance(value, (list, tuple)):
            if len(value) >= 2 and all(isinstance(v, (int, float)) for v in value[:2]):
                values.extend((float(value[0]), float(value[1])))
            else:
                for child in value:
                    visit(child)

    visit(coordinates)
    if len(values) < 2:
        raise ValueError("Geometry has no coordinates")
    return min(values[0::2]), min(values[1::2]), max(values[0::2]), max(values[1::2])


def overlapping_tile_ids(
    tile_index: Mapping[str, Mapping[str, Any]],
    geofilter: str = AOI_WKT,
    allowlist: Iterable[str] | None = None,
) -> list[str]:
    """Select tile IDs whose indexed geometry overlaps the AOI."""
    aoi = _aoi_bbox(geofilter)
    allowed = set(allowlist) if allowlist is not None else None
    selected = []
    for tile_id, entry in tile_index.items():
        if allowed is not None and tile_id not in allowed:
            continue
        geometry = entry.get("geometry")
        if geometry and _bbox_intersects(_geometry_bbox(geometry), aoi):
            selected.append(tile_id)
    return sorted(selected)


def ahn6_tiles_for_aoi(
    tile_index: Mapping[str, Mapping[str, Any]],
    geofilter: str = AOI_WKT,
    allowlist: Iterable[str] = (),
) -> list[str]:
    """Select AOI tiles from AHN6, applying the explicit fixture allowlist."""
    return overlapping_tile_ids(tile_index, geofilter, allowlist)


def _xml_bbox(data: bytes) -> tuple[float, float, float, float] | None:
    values = [float(v) for v in _NUMBER.findall(data.decode("utf-8", errors="ignore"))]
    if len(values) < 4:
        return None
    # BAG geometries use x/y pairs; metadata values can also be present, so use
    # the complete coordinate envelope only when it is in the RD New range.
    pairs = list(zip(values[0::2], values[1::2]))
    pairs = [(x, y) for x, y in pairs if 0 < x < 300000 and 0 < y < 700000]
    if not pairs:
        return None
    return (
        min(x for x, _ in pairs),
        min(y for _, y in pairs),
        max(x for x, _ in pairs),
        max(y for _, y in pairs),
    )


def filter_bag_extract(
    source: Path, destination: Path, geofilter: str = AOI_WKT
) -> Path:
    """Copy a BAG extract while retaining its nested ZIP/XML layout."""
    destination.mkdir(parents=True, exist_ok=True)
    aoi = _aoi_bbox(geofilter)
    for source_file in source.iterdir():
        target = destination / source_file.name
        if source_file.suffix.lower() != ".zip":
            shutil.copy2(source_file, target)
            continue
        with (
            ZipFile(source_file) as source_zip,
            ZipFile(target, "w", ZIP_DEFLATED) as target_zip,
        ):
            for info in source_zip.infolist():
                data = source_zip.read(info)
                if info.filename.lower().endswith((".xml", ".gml")):
                    bbox = _xml_bbox(data)
                    if bbox is not None and not _bbox_intersects(bbox, aoi):
                        continue
                target_zip.writestr(info, data)
    return destination


def matching_cbs_codes(
    records: Iterable[Mapping[str, Any]],
    geometries: Mapping[str, Mapping[str, Any]],
    geofilter: str = AOI_WKT,
) -> list[dict[str, Any]]:
    """Keep key figures for intersecting gemeente, wijk, and buurt codes."""
    aoi = _aoi_bbox(geofilter)
    codes = {
        code
        for code, feature in geometries.items()
        if feature.get("geometry")
        and _bbox_intersects(_geometry_bbox(feature["geometry"]), aoi)
    }
    result = []
    for record in records:
        code = next(
            (
                record.get(key)
                for key in ("Codering", "Code", "code", "BU_CODE", "RegioS")
                if record.get(key)
            ),
            None,
        )
        if code is not None and str(code) in codes:
            result.append(dict(record))
    return result


def _copy_download(url: str, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / url.rsplit("/", 1)[-1]
    download_file(url, target, chunk_size=1024 * 1024)
    return target


def _write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


@asset(
    group_name="integration_data",
    ins={"extract_bag": AssetIn(key=AssetKey(["bag", "extract_bag"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_bag(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    extract_bag,
) -> Path:
    source = Path(extract_bag[0] if isinstance(extract_bag, tuple) else extract_bag)
    return filter_bag_extract(
        source, integration_data_store.path / "bag", config.geofilter
    )


@asset(
    group_name="integration_data",
    ins={"extract_bgt": AssetIn(key=AssetKey(["bgt", "extract_bgt"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_bgt(integration_data_store: FileStoreResource, extract_bgt) -> Path:
    target = integration_data_store.path / "bgt" / Path(extract_bgt).name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(extract_bgt, target)
    return target


@asset(
    group_name="integration_data",
    ins={"extract_top10nl": AssetIn(key=AssetKey(["top10nl", "extract_top10nl"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_top10nl(
    integration_data_store: FileStoreResource, extract_top10nl
) -> Path:
    target = integration_data_store.path / "top10nl" / Path(extract_top10nl).name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(extract_top10nl, target)
    return target


@asset(
    group_name="integration_data",
    ins={
        "extract_cbs_key_figures": AssetIn(
            key=AssetKey(["cbs", "extract_cbs_key_figures"])
        )
    },
    automation_condition=AutomationCondition.eager(),
)
def integration_cbs_key_figures(
    integration_data_store: FileStoreResource, extract_cbs_key_figures
) -> Path:
    target = integration_data_store.path / "cbs" / "key_figures.json"
    return _write_json(target, extract_cbs_key_figures)


@asset(
    group_name="integration_data",
    ins={
        "extract_cbs_buurtkaart": AssetIn(
            key=AssetKey(["cbs", "extract_cbs_buurtkaart"])
        )
    },
    automation_condition=AutomationCondition.eager(),
)
def integration_cbs_buurtkaart(
    integration_data_store: FileStoreResource,
    extract_cbs_buurtkaart,
    gdal: GDALResource,
) -> Path:
    target = integration_data_store.path / "cbs" / "buurtkaart.gpkg"
    target.parent.mkdir(parents=True, exist_ok=True)
    result = gdal.runner.run(
        "{exe} -f GPKG -clipsrc '{aoi}' '{local_path}' '{output}'",
        exe_name="ogr2ogr",
        kwargs={"aoi": AOI_WKT, "output": target},
        local_path=Path(extract_cbs_buurtkaart),
    )
    if not result.success:
        raise RuntimeError(f"Could not clip buurtkaart: {result.stderr}")
    return target


def _download_ahn_tiles(
    index: Mapping[str, Mapping[str, Any]],
    versions: Iterable[int],
    target: Path,
    geofilter: str,
    allowlist: Iterable[str] = (),
) -> Path:
    target.mkdir(parents=True, exist_ok=True)
    for version in versions:
        selected = overlapping_tile_ids(
            index, geofilter, allowlist if version == 6 else None
        )
        version_dir = target / f"AHN{version}"
        version_dir.mkdir(exist_ok=True)
        for tile_id in selected:
            entry = index[tile_id]
            url = entry.get("url") if version == 6 else entry.get(f"AHN{version}_LAZ")
            if url:
                _copy_download(url, version_dir)
    return target


@asset(
    group_name="integration_data",
    ins={"tile_index_ahn": AssetIn(key=AssetKey(["ahn", "tile_index_ahn"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_ahn3(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    tile_index_ahn,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn,
        (3,),
        integration_data_store.path / "pointclouds",
        config.geofilter,
    )


@asset(
    group_name="integration_data",
    ins={"tile_index_ahn": AssetIn(key=AssetKey(["ahn", "tile_index_ahn"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_ahn4(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    tile_index_ahn,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn,
        (4,),
        integration_data_store.path / "pointclouds",
        config.geofilter,
    )


@asset(
    group_name="integration_data",
    ins={"tile_index_ahn": AssetIn(key=AssetKey(["ahn", "tile_index_ahn"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_ahn5(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    tile_index_ahn,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn,
        (5,),
        integration_data_store.path / "pointclouds",
        config.geofilter,
    )


@asset(
    group_name="integration_data",
    ins={"tile_index_ahn6": AssetIn(key=AssetKey(["ahn", "tile_index_ahn6"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_ahn6(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    tile_index_ahn6,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn6,
        (6,),
        integration_data_store.path / "pointclouds",
        config.geofilter,
        config.ahn6_allowlist,
    )


@asset(
    group_name="integration_data",
    automation_condition=AutomationCondition.eager(),
    ins={
        "extract_bag": AssetIn(key=AssetKey(["bag", "extract_bag"])),
        "extract_bgt": AssetIn(key=AssetKey(["bgt", "extract_bgt"])),
        "extract_top10nl": AssetIn(key=AssetKey(["top10nl", "extract_top10nl"])),
        "extract_cbs_key_figures": AssetIn(
            key=AssetKey(["cbs", "extract_cbs_key_figures"])
        ),
        "extract_cbs_buurtkaart": AssetIn(
            key=AssetKey(["cbs", "extract_cbs_buurtkaart"])
        ),
        "tile_index_ahn": AssetIn(key=AssetKey(["ahn", "tile_index_ahn"])),
        "tile_index_ahn6": AssetIn(key=AssetKey(["ahn", "tile_index_ahn6"])),
        "md5_ahn3": AssetIn(key=AssetKey(["ahn", "md5_ahn3"])),
        "md5_ahn4": AssetIn(key=AssetKey(["ahn", "md5_ahn4"])),
        "sha256_ahn5": AssetIn(key=AssetKey(["ahn", "sha256_ahn5"])),
        "sha256_ahn6": AssetIn(key=AssetKey(["ahn", "sha256_ahn6"])),
    },
)
def integration_manifest(
    integration_data_store: FileStoreResource,
    integration_bag,
    integration_bgt,
    integration_top10nl,
    integration_cbs_key_figures,
    integration_cbs_buurtkaart,
    integration_ahn3,
    integration_ahn4,
    integration_ahn5,
    integration_ahn6,
    extract_bag,
    extract_bgt,
    extract_top10nl,
    extract_cbs_key_figures,
    extract_cbs_buurtkaart,
    tile_index_ahn,
    tile_index_ahn6,
    md5_ahn3,
    md5_ahn4,
    sha256_ahn5,
    sha256_ahn6,
) -> Path:
    """Write the complete, versioned snapshot manifest used by fixture mode."""
    root = integration_data_store.path
    files = {}
    for path in sorted(
        p for p in root.rglob("*") if p.is_file() and p.name != "manifest.json"
    ):
        relative = str(path.relative_to(root))
        files[relative] = {"size": path.stat().st_size}

    def rel(path):
        return str(Path(path).relative_to(root))

    bag_dir = Path(integration_bag)
    if not bag_dir.is_absolute():
        bag_dir = root / bag_dir
    bag_value = extract_bag if isinstance(extract_bag, tuple) else (extract_bag, {}, "")
    sources = {
        "bag": {
            "path": rel(bag_dir),
            "metadata": bag_value[1],
            "shortdate": bag_value[2],
        },
        "bgt": {"path": rel(integration_bgt)},
        "top10nl": {"path": rel(integration_top10nl)},
        "cbs_key_figures": {"path": rel(integration_cbs_key_figures)},
        "cbs_buurtkaart": {"path": rel(integration_cbs_buurtkaart)},
    }
    indexes = {"3": tile_index_ahn, "6": tile_index_ahn6}
    checksums = {"3": md5_ahn3, "4": md5_ahn4, "5": sha256_ahn5, "6": sha256_ahn6}
    partitions = {}
    for version in (3, 4, 5):
        version_dir = root / "pointclouds" / f"AHN{version}"
        partitions[str(version)] = {}
        for tile_id, entry in tile_index_ahn.items():
            url = entry.get(f"AHN{version}_LAZ")
            if url and (version_dir / Path(url).name).is_file():
                partitions[str(version)][tile_id] = {
                    "path": rel(version_dir / Path(url).name),
                    "url": url,
                }
    partitions["6"] = {}
    for tile_id, entry in tile_index_ahn6.items():
        url = entry.get("url")
        if url and (root / "pointclouds" / "AHN6" / Path(url).name).is_file():
            batch = f"{int(tile_id.split('_')[0]) // 10000 * 10000:06d}_{int(tile_id.split('_')[1]) // 10000 * 10000:06d}"
            partitions["6"].setdefault(batch, {})[tile_id] = {
                "path": rel(root / "pointclouds" / "AHN6" / Path(url).name),
                "url": url,
            }
    manifest = {
        "fixture_version": "1",
        "date": date.today().isoformat(),
        "sources": sources,
        "ahn": {"indexes": indexes, "checksums": checksums, "partitions": partitions},
        "files": files,
    }
    return _write_json(root / "manifest.json", manifest)
