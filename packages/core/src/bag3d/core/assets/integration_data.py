"""AOI-subset source-data fixtures for integration and pipeline tests."""

import hashlib
import json
import re
import shutil
import sqlite3
import tempfile
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from bag3d.common.resources.executables import GDALResource, LASToolsResource
from bag3d.common.resources.files import FileStoreResource
from bag3d.common.utils.requests import download_file
from dagster import AssetIn, AssetKey, AutomationCondition, Config, asset
from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]
from pydantic import Field

from bag3d.core.fixture_contract import (
    FIXTURE_VERSION,
    NATIVE_GML,
    VECTOR_PROCESSING,
    validate_native_extent,
)

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
_EXTENT = re.compile(
    r"Extent(?:\s+[^:]+)?:\s*\(([-+0-9.eE]+),\s*([-+0-9.eE]+)\)\s*-\s*\(([-+0-9.eE]+),\s*([-+0-9.eE]+)\)"
)
_LAS_BOUND = re.compile(
    r"^\s*(min|max) x y z:\s*([-+0-9.eE]+)\s+([-+0-9.eE]+)",
    re.MULTILINE | re.IGNORECASE,
)
_STAND_TAG = re.compile(rb"<(?:[A-Za-z_][\w.-]*:)?stand(?:\s|>)")
_POS_TAG = re.compile(rb"<(?:[A-Za-z_][\w.-]*:)?pos(?:\s|>)")
_BAG_LAYER_NAME = re.compile(
    r"(?:\d{4}[A-Za-z]+|GEM-WPL-RELATIE-)(\d{8})(?:\.zip)?", re.IGNORECASE
)
_AOI_COORDINATE = re.compile(
    rb"(?:^|\s|>)(?:12[12]\d{3}|123[0-3]\d{2})(?:\.\d+)?\s+(?:485[7-9]\d{2}|486[0-5]\d{2})(?:\.\d+)?(?:\s|$)"
)
BUFFER_METRES = 10.0


class IntegrationDataConfig(Config):
    geofilter: str = Field(default=AOI_WKT)
    cbs_year: str = Field(default="2025")
    ahn6_allowlist: list[str] = Field(
        default_factory=lambda: AHN6_AOI_TILE_ALLOWLIST.copy()
    )


def _aoi_bbox(wkt: str) -> tuple[float, float, float, float]:
    if not re.match(r"^\s*POLYGON\s*\(\(", wkt, re.IGNORECASE):
        raise ValueError(f"Expected a polygon WKT, got {wkt!r}")
    values = [float(value) for value in _NUMBER.findall(wkt)]
    points = list(zip(values[0::2], values[1::2]))
    if len(points) < 4 or points[0] != points[-1]:
        raise ValueError(f"Expected a closed polygon WKT, got {wkt!r}")
    extent = (
        min(x for x, _ in points),
        min(y for _, y in points),
        max(x for x, _ in points),
        max(y for _, y in points),
    )
    if extent[0] >= extent[2] or extent[1] >= extent[3]:
        raise ValueError(f"Polygon WKT has no area: {wkt!r}")
    return extent


def _bbox_intersects(
    a: tuple[float, float, float, float], b: tuple[float, float, float, float]
) -> bool:
    return max(a[0], b[0]) <= min(a[2], b[2]) and max(a[1], b[1]) <= min(a[3], b[3])


def _geometry_bbox(geometry: Mapping[str, Any]) -> tuple[float, float, float, float]:
    values: list[float] = []

    def visit(value: Any) -> None:
        if isinstance(value, (list, tuple)):
            if len(value) >= 2 and all(
                isinstance(item, (int, float)) for item in value[:2]
            ):
                values.extend((float(value[0]), float(value[1])))
            else:
                for child in value:
                    visit(child)

    visit(geometry.get("coordinates", []))
    if len(values) < 2:
        raise ValueError("Geometry has no coordinates")
    return min(values[0::2]), min(values[1::2]), max(values[0::2]), max(values[1::2])


def overlapping_tile_ids(
    tile_index: Mapping[str, Mapping[str, Any]],
    geofilter: str = AOI_WKT,
    allowlist: Iterable[str] | None = None,
) -> list[str]:
    aoi = _aoi_bbox(geofilter)
    allowed = set(allowlist) if allowlist is not None else None
    return sorted(
        tile_id
        for tile_id, entry in tile_index.items()
        if (allowed is None or tile_id in allowed)
        and entry.get("geometry")
        and _bbox_intersects(_geometry_bbox(entry["geometry"]), aoi)
    )


def ahn6_tiles_for_aoi(
    tile_index: Mapping[str, Mapping[str, Any]],
    geofilter: str = AOI_WKT,
    allowlist: Iterable[str] = (),
) -> list[str]:
    return overlapping_tile_ids(tile_index, geofilter, allowlist)


def _filter_bag_xml(
    data: bytes, aoi: tuple[float, float, float, float]
) -> bytes | None:
    if aoi == _aoi_bbox(AOI_WKT) and not _AOI_COORDINATE.search(data):
        return None
    if not _STAND_TAG.search(data) and not _POS_TAG.search(data):
        return data
    try:
        root = etree.fromstring(data)
    except etree.XMLSyntaxError as exc:
        raise ValueError("BAG XML is not well formed") from exc
    stands = list(root.iter("{*}stand"))
    buffered_aoi = (
        aoi[0] - BUFFER_METRES,
        aoi[1] - BUFFER_METRES,
        aoi[2] + BUFFER_METRES,
        aoi[3] + BUFFER_METRES,
    )
    if not stands:
        features = [child for child in root if list(child.iter("{*}pos"))]
        if not features:
            return data
        for feature in features:
            coordinates = []
            for pos in feature.iter("{*}pos"):
                values = [float(value) for value in _NUMBER.findall(pos.text or "")]
                dimension = int(pos.get("srsDimension", "3"))
                if dimension < 2 or len(values) % dimension:
                    coordinates = []
                    break
                coordinates.extend(
                    (values[index], values[index + 1])
                    for index in range(0, len(values), dimension)
                )
            if not coordinates:
                root.remove(feature)
                continue
            bbox = (
                min(x for x, _ in coordinates),
                min(y for _, y in coordinates),
                max(x for x, _ in coordinates),
                max(y for _, y in coordinates),
            )
            if not _bbox_intersects(bbox, aoi) or not (
                buffered_aoi[0] <= bbox[0]
                and buffered_aoi[1] <= bbox[1]
                and bbox[2] <= buffered_aoi[2]
                and bbox[3] <= buffered_aoi[3]
            ):
                root.remove(feature)
        return (
            etree.tostring(root, encoding="UTF-8", xml_declaration=True)
            if any(root.iter("{*}pos"))
            else None
        )
    for stand in stands:
        coordinates: list[tuple[float, float]] = []
        valid_geometry = True
        for position in stand.iter("{*}posList", "{*}pos"):
            values = [float(value) for value in _NUMBER.findall(position.text or "")]
            dimension_value = position.get("srsDimension")
            if dimension_value is None:
                dimension = 3 if len(values) % 3 == 0 else 2
            else:
                try:
                    dimension = int(dimension_value)
                except ValueError:
                    valid_geometry = False
                    break
            if dimension < 2 or len(values) % dimension:
                valid_geometry = False
                break
            coordinates.extend(
                (values[index], values[index + 1])
                for index in range(0, len(values), dimension)
            )
        if not valid_geometry:
            parent = stand.getparent()
            if parent is not None:
                parent.remove(stand)
            continue
        if not coordinates:
            parent = stand.getparent()
            if parent is not None:
                parent.remove(stand)
            continue
        bbox = (
            min(x for x, _ in coordinates),
            min(y for _, y in coordinates),
            max(x for x, _ in coordinates),
            max(y for _, y in coordinates),
        )
        if not _bbox_intersects(bbox, aoi) or not (
            buffered_aoi[0] <= bbox[0]
            and buffered_aoi[1] <= bbox[1]
            and bbox[2] <= buffered_aoi[2]
            and bbox[3] <= buffered_aoi[3]
        ):
            parent = stand.getparent()
            if parent is not None:
                parent.remove(stand)
    if not any(root.iter("{*}stand")):
        return None
    return etree.tostring(root, encoding="UTF-8", xml_declaration=True)


def _filter_bag_member(
    data: bytes, aoi: tuple[float, float, float, float], filename: str
) -> bytes | None:
    if filename.lower().endswith(".zip"):
        output = BytesIO()
        with (
            ZipFile(BytesIO(data)) as source_zip,
            ZipFile(output, "w", ZIP_DEFLATED) as target_zip,
        ):
            for info in source_zip.infolist():
                member = _filter_bag_member(source_zip.read(info), aoi, info.filename)
                if member is not None:
                    target_zip.writestr(info, member)
        return output.getvalue() if output.tell() else None
    if filename.lower().endswith((".xml", ".gml")) and data.strip():
        return _filter_bag_xml(data, aoi)
    return data


def filter_bag_extract(
    source: Path, destination: Path, shortdate: str, geofilter: str = AOI_WKT
) -> Path:
    aoi = _aoi_bbox(geofilter)
    destination.mkdir(parents=True, exist_ok=True)
    for source_file in source.iterdir():
        layer = _BAG_LAYER_NAME.fullmatch(source_file.name)
        if layer and layer[1] != shortdate:
            continue
        target = destination / source_file.name
        if source_file.is_dir():
            raise ValueError(
                f"Expected BAG layer ZIPs, found directory {source_file}. "
                "Rematerialize extract_bag to create an intact snapshot."
            )
        if source_file.suffix.lower() != ".zip":
            shutil.copy2(source_file, target)
            continue
        if re.search(r"(?:NUM|OPR|RELATIE)", source_file.name.upper()):
            shutil.copy2(source_file, target)
            continue
        with (
            ZipFile(source_file) as source_zip,
            ZipFile(target, "w", ZIP_DEFLATED) as target_zip,
        ):
            for info in source_zip.infolist():
                data = _filter_bag_member(source_zip.read(info), aoi, info.filename)
                if data is not None:
                    target_zip.writestr(info, data)
    # A reused fixture directory may still contain layers from an earlier run.
    for target in destination.iterdir():
        layer = _BAG_LAYER_NAME.fullmatch(target.name)
        if layer and layer[1] != shortdate:
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
    return destination


def _archive_layer(source: Path, names: tuple[str, ...]) -> str:
    with ZipFile(source) as archive:
        members = archive.namelist()
    for wanted in names:
        for member in members:
            if member.lower() == wanted.lower() or member.lower().endswith(
                wanted.lower()
            ):
                return member
    raise FileNotFoundError(f"Archive {source} does not contain one of {names}")


def _copy_native_gml_archive(source: Path, destination: Path, dataset: str) -> Path:
    """Publish the PDOK archive unchanged; objects are selected at download time."""
    _archive_layer(source, (f"{dataset}_{NATIVE_GML[dataset]['layer'].lower()}.gml",))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent) as temp_dir:
        output = Path(temp_dir) / destination.name
        shutil.copyfile(source, output)
        output.replace(destination)
    return destination


def _native_gml_extents(
    source: Path, dataset: str, gdal: GDALResource
) -> list[tuple[float, ...]]:
    """Check native geometry properties, then read extents using the loader's XSD."""
    spec = NATIVE_GML[dataset]
    member = _archive_layer(source, (f"{dataset}_{spec['layer'].lower()}.gml",))
    feature_tag = f"{{{spec['namespace']}}}{spec['layer']}"
    geometry_tag = f"{{{spec['namespace']}}}{spec['geometry']}"
    count = 0
    with ZipFile(source) as archive, archive.open(member) as stream:
        for _, feature in etree.iterparse(
            stream,
            events=("end",),
            tag=feature_tag,
            resolve_entities=False,
            no_network=True,
        ):
            count += 1
            geometry = feature.find(geometry_tag)
            positions = (
                []
                if geometry is None
                else list(geometry.iter("{*}pos", "{*}posList", "{*}coordinates"))
            )
            if (
                geometry is None
                or geometry.get("{http://www.w3.org/2001/XMLSchema-instance}nil")
                in {"true", "1"}
                or not any((position.text or "").strip() for position in positions)
            ):
                identifier = feature.get(
                    "{http://www.opengis.net/gml}id"
                ) or feature.get("{http://www.opengis.net/gml/3.2}id")
                raise ValueError(
                    f"Missing native {spec['geometry']} in {source}: {identifier}"
                )
            # Release completed feature members as well as their geometry trees.
            parent = feature.getparent()
            feature.clear()
            while parent is not None and parent.getprevious() is not None:
                del parent.getparent()[0]
    if not count:
        raise ValueError(
            f"No native {spec['layer']} objects in {source}; rematerialize the AOI extract"
        )
    result = gdal.runner.run(
        "{exe} -ro -so -oo WRITE_GFS=NO -oo XSD='{xsd}' '/vsizip/{local_path}/{member}' {layer}",
        exe_name="ogrinfo",
        local_path=source,
        kwargs={"xsd": spec["xsd"], "member": member, "layer": spec["layer"]},
    )
    if _command_has_gdal_error(result):
        raise RuntimeError(
            f"ogrinfo failed for {source}: {result.stderr or result.stdout}"
        )
    if dataset == "bgt":
        # PGDump enforces the XSD's NOT NULL geometry without touching a database.
        check = gdal.runner.run(
            "{exe} -f PGDump /vsistdout/ -oo WRITE_GFS=NO -oo XSD='{xsd}' '/vsizip/{local_path}/{member}' {layer}",
            exe_name="ogr2ogr",
            local_path=source,
            kwargs={"xsd": spec["xsd"], "member": member, "layer": spec["layer"]},
        )
        if _command_has_gdal_error(check):
            raise ValueError(
                f"Unreadable native geometrie2d in {source}: {check.stderr or check.stdout}"
            )
    # BGT has nine label-point fields; only the footprint determines AOI overlap.
    output = result.stdout
    if dataset == "bgt":
        output = "\n".join(
            line
            for line in output.splitlines()
            if line.startswith("Extent (geometrie2d):")
        )
    extents = [tuple(float(v) for v in match) for match in _EXTENT.findall(output)]
    if not extents:
        raise ValueError(f"No native geometry extent for {source}")
    return extents


def _copy_download(url: str, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / url.rsplit("/", 1)[-1]
    download_file(url, target, chunk_size=1024 * 1024)
    return target


def _write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _codes_from_clipped_buurtkaart(path: Path) -> set[str]:
    codes: set[str] = set()
    with sqlite3.connect(path) as connection:
        tables = connection.execute(
            "SELECT table_name FROM gpkg_contents WHERE data_type = 'features'"
        ).fetchall()
        for (table,) in tables:
            columns = [
                row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')
            ]
            code_columns = [
                column
                for column in columns
                if any(
                    token in column.lower() for token in ("code", "codering", "regio")
                )
            ]
            if not code_columns:
                continue
            selected = ", ".join(f'"{column}"' for column in code_columns)
            for row in connection.execute(f'SELECT {selected} FROM "{table}"'):
                codes.update(str(value).strip() for value in row if value is not None)
    return codes


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
    source, _, shortdate = extract_bag
    return filter_bag_extract(
        Path(source), integration_data_store.path / "bag", shortdate, config.geofilter
    )


@asset(
    group_name="integration_data",
    ins={"extract_bgt": AssetIn(key=AssetKey(["bgt", "extract_bgt"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_bgt(
    integration_data_store: FileStoreResource,
    extract_bgt,
) -> Path:
    return _copy_native_gml_archive(
        Path(extract_bgt), integration_data_store.path / "bgt" / "bgt.zip", "bgt"
    )


@asset(
    group_name="integration_data",
    ins={"extract_top10nl": AssetIn(key=AssetKey(["top10nl", "extract_top10nl"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_top10nl(
    integration_data_store: FileStoreResource,
    extract_top10nl,
) -> Path:
    return _copy_native_gml_archive(
        Path(extract_top10nl),
        integration_data_store.path / "top10nl" / "top10nl.zip",
        "top10nl",
    )


@asset(
    group_name="integration_data",
    ins={
        "extract_cbs_key_figures": AssetIn(
            key=AssetKey(["cbs", "extract_cbs_key_figures"])
        ),
        "clipped_buurtkaart": AssetIn(key=AssetKey(["integration_cbs_buurtkaart"])),
    },
    automation_condition=AutomationCondition.eager(),
)
def integration_cbs_key_figures(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    extract_cbs_key_figures,
    clipped_buurtkaart,
) -> Path:
    codes = _codes_from_clipped_buurtkaart(Path(clipped_buurtkaart))
    records = [
        record
        for record in extract_cbs_key_figures
        if any(
            str(record.get(key, "")).strip() in codes
            for key in (
                "WijkenEnBuurten",
                "Codering",
                "Code",
                "code",
                "BU_CODE",
                "RegioS",
            )
        )
    ]
    return _write_json(
        integration_data_store.path / "cbs" / "key_figures.json", records
    )


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
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    extract_cbs_buurtkaart,
    gdal: GDALResource,
) -> Path:
    _aoi_bbox(config.geofilter)
    target = integration_data_store.path / "cbs" / "buurtkaart.gpkg"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    result = gdal.runner.run(
        "{exe} -f GPKG -clipsrc '{aoi}' '{output}' '{source}'",
        exe_name="ogr2ogr",
        kwargs={
            "aoi": config.geofilter,
            "output": target,
            "source": Path(extract_cbs_buurtkaart),
        },
    )
    if not result.success or not target.is_file():
        raise RuntimeError(f"Could not clip buurtkaart: {result.stderr}")
    return target


def _download_ahn_tiles(
    index: Mapping[str, Mapping[str, Any]],
    version: int,
    target: Path,
    geofilter: str,
    lastools: LASToolsResource,
    allowlist: Iterable[str] = (),
) -> Path:
    selected = overlapping_tile_ids(
        index, geofilter, allowlist if version == 6 else None
    )
    version_dir = target / f"AHN{version}"
    version_dir.mkdir(parents=True, exist_ok=True)
    minx, miny, maxx, maxy = _aoi_bbox(geofilter)
    for tile_id in selected:
        entry = index[tile_id]
        url = entry.get("url")
        if not url:
            continue
        with tempfile.TemporaryDirectory(dir=target) as temp_dir:
            source = _copy_download(url, Path(temp_dir))
            destination = version_dir / source.name
            result = lastools.runner.run(
                "{exe} -i '{source}' -keep_xy {minx} {miny} {maxx} {maxy} -o '{output}'",
                exe_name="las2las",
                kwargs={
                    "source": source,
                    "minx": minx,
                    "miny": miny,
                    "maxx": maxx,
                    "maxy": maxy,
                    "output": destination,
                },
            )
            if (
                not result.success
                or not destination.is_file()
                or destination.stat().st_size == 0
            ):
                raise RuntimeError(
                    f"las2las failed for AHN{version} tile {tile_id}: {result.stderr}"
                )
    return target


@asset(
    group_name="integration_data",
    ins={"tile_index_ahn3": AssetIn(key=AssetKey(["ahn", "tile_index_ahn3"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_ahn3(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    tile_index_ahn3,
    lastools: LASToolsResource,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn3,
        3,
        integration_data_store.path / "pointclouds",
        config.geofilter,
        lastools,
    )


@asset(
    group_name="integration_data",
    ins={"tile_index_ahn4": AssetIn(key=AssetKey(["ahn", "tile_index_ahn4"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_ahn4(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    tile_index_ahn4,
    lastools: LASToolsResource,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn4,
        4,
        integration_data_store.path / "pointclouds",
        config.geofilter,
        lastools,
    )


@asset(
    group_name="integration_data",
    ins={"tile_index_ahn5": AssetIn(key=AssetKey(["ahn", "tile_index_ahn5"]))},
    automation_condition=AutomationCondition.eager(),
)
def integration_ahn5(
    config: IntegrationDataConfig,
    integration_data_store: FileStoreResource,
    tile_index_ahn5,
    lastools: LASToolsResource,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn5,
        5,
        integration_data_store.path / "pointclouds",
        config.geofilter,
        lastools,
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
    lastools: LASToolsResource,
) -> Path:
    return _download_ahn_tiles(
        tile_index_ahn6,
        6,
        integration_data_store.path / "pointclouds",
        config.geofilter,
        lastools,
        config.ahn6_allowlist,
    )


def _command_has_gdal_error(result: Any) -> bool:
    return result.returncode != 0 or bool(
        re.search(r"(?im)^\s*(?:error|fatal|critical)\b", result.stdout + result.stderr)
    )


def _ogrinfo_extents(
    gdal_runner: Any, path: Path, require_extent: bool = True
) -> list[tuple[float, float, float, float]]:
    result = gdal_runner.run(
        "{exe} -so -al '{local_path}'", exe_name="ogrinfo", local_path=path
    )
    if _command_has_gdal_error(result):
        raise RuntimeError(
            f"ogrinfo failed for {path}: {result.stderr or result.stdout}"
        )
    extents: list[tuple[float, float, float, float]] = []
    for min_x, min_y, max_x, max_y in _EXTENT.findall(result.stdout):
        extents.append((float(min_x), float(min_y), float(max_x), float(max_y)))
    if not extents and require_extent:
        raise RuntimeError(f"ogrinfo returned no extent for {path}")
    return extents


def _ogrinfo_xml(
    gdal_runner: Any, data: bytes, suffix: str, require_extent: bool = True
) -> list[tuple[float, float, float, float]]:
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / Path(suffix).name
        path.write_bytes(data)
        return _ogrinfo_extents(gdal_runner, path, require_extent)


def _nested_bag_xml(data: bytes, prefix: str = "") -> Iterable[tuple[str, bytes]]:
    with ZipFile(BytesIO(data)) as archive:
        for info in archive.infolist():
            member = archive.read(info)
            name = f"{prefix}!{info.filename}" if prefix else info.filename
            if info.filename.lower().endswith(".zip"):
                yield from _nested_bag_xml(member, name)
            elif info.filename.lower().endswith((".xml", ".gml")) and member.strip():
                yield name, member


def _is_spatial_bag_xml(data: bytes) -> bool:
    return bool(_STAND_TAG.search(data) or _POS_TAG.search(data))


def _buffered_extent(
    aoi: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    return (
        aoi[0] - BUFFER_METRES,
        aoi[1] - BUFFER_METRES,
        aoi[2] + BUFFER_METRES,
        aoi[3] + BUFFER_METRES,
    )


def _validate_extent(
    extent: tuple[float, float, float, float],
    allowed: tuple[float, float, float, float],
    label: str,
) -> None:
    if not (
        allowed[0] <= extent[0]
        and allowed[1] <= extent[1]
        and extent[2] <= allowed[2]
        and extent[3] <= allowed[3]
    ):
        raise ValueError(
            f"Extent for {label} is outside the AOI plus {BUFFER_METRES:g} m: {extent}"
        )


def _validate_outputs(
    root: Path,
    bag: Path,
    bgt: Path,
    top10nl: Path,
    cbs: Path,
    ahn_roots: Iterable[Path],
    aoi: tuple[float, float, float, float],
    gdal: GDALResource,
    lastools: LASToolsResource,
) -> dict[str, Any]:
    allowed = _buffered_extent(aoi)
    gdal_runner = gdal.runner
    vector_extents: dict[str, list[list[float]]] = {}
    for archive in sorted(bag.rglob("*.zip")):
        for name, data in _nested_bag_xml(
            archive.read_bytes(), str(archive.relative_to(root))
        ):
            if not _is_spatial_bag_xml(data):
                continue
            extents = [
                _validate_and_return(extent, allowed, f"{archive.name}!{name}")
                for extent in _ogrinfo_xml(
                    gdal_runner, data, name, require_extent=False
                )
            ]
            vector_extents[f"{archive.name}!{name}"] = [
                list(extent) for extent in extents
            ]
    for path in sorted(bag.rglob("*")):
        if (
            path.is_file()
            and path.suffix.lower() in {".xml", ".gml"}
            and path.stat().st_size
        ):
            data = path.read_bytes()
            if not _is_spatial_bag_xml(data):
                continue
            extents = [
                _validate_and_return(extent, allowed, str(path.relative_to(root)))
                for extent in _ogrinfo_extents(gdal_runner, path)
            ]
            vector_extents[str(path.relative_to(root))] = [
                list(extent) for extent in extents
            ]

    for archive, dataset in ((bgt, "bgt"), (top10nl, "top10nl")):
        extents = _native_gml_extents(archive, dataset, gdal)
        for extent in extents:
            validate_native_extent(extent, aoi, str(archive))
        vector_extents[str(archive.relative_to(root))] = [
            list(extent) for extent in extents
        ]

    extents = [
        _validate_and_return(extent, allowed, str(cbs))
        for extent in _ogrinfo_extents(gdal_runner, cbs)
    ]
    vector_extents[str(cbs.relative_to(root))] = [list(extent) for extent in extents]

    pointcloud_extents: dict[str, list[float]] = {}
    lastools_runner = lastools.runner
    # AHN assets share a pointcloud root; overlapping roots and aliases may also
    # refer to the same file. Keep the original path for manifest labels.
    pointcloud_files: dict[Path, Path] = {}
    for pointcloud_root in dict.fromkeys(ahn_roots):
        for path in sorted(pointcloud_root.rglob("*")):
            if path.suffix.lower() in {".las", ".laz"} and path.is_file():
                pointcloud_files.setdefault(path.resolve(), path)
    for path in sorted(pointcloud_files.values()):
        result = lastools_runner.run(
            "{exe} -i '{local_path}'", exe_name="lasinfo", local_path=path
        )
        if result.returncode != 0 or bool(
            re.search(
                r"(?im)^\s*(?:error|fatal|critical)\b",
                result.stdout + result.stderr,
            )
        ):
            raise RuntimeError(
                f"lasinfo failed for {path}: {result.stderr or result.stdout}"
            )
        las_output = result.stdout + result.stderr
        bounds = {
            kind: (float(x), float(y)) for kind, x, y in _LAS_BOUND.findall(las_output)
        }
        if set(bounds) != {"min", "max"}:
            raise RuntimeError(f"lasinfo returned no complete XY bounds for {path}")
        extent = (
            bounds["min"][0],
            bounds["min"][1],
            bounds["max"][0],
            bounds["max"][1],
        )
        _validate_extent(extent, allowed, str(path.relative_to(root)))
        pointcloud_extents[str(path.relative_to(root))] = list(extent)
    return {
        "buffer_metres": BUFFER_METRES,
        "extents": {"vectors": vector_extents, "pointclouds": pointcloud_extents},
    }


def _validate_and_return(
    extent: tuple[float, float, float, float],
    allowed: tuple[float, float, float, float],
    label: str,
) -> tuple[float, float, float, float]:
    _validate_extent(extent, allowed, label)
    return extent


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
        "tile_index_ahn3": AssetIn(key=AssetKey(["ahn", "tile_index_ahn3"])),
        "tile_index_ahn4": AssetIn(key=AssetKey(["ahn", "tile_index_ahn4"])),
        "tile_index_ahn5": AssetIn(key=AssetKey(["ahn", "tile_index_ahn5"])),
        "tile_index_ahn6": AssetIn(key=AssetKey(["ahn", "tile_index_ahn6"])),
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
    tile_index_ahn3,
    tile_index_ahn4,
    tile_index_ahn5,
    tile_index_ahn6,
    gdal: GDALResource,
    lastools: LASToolsResource,
) -> Path:
    root = integration_data_store.path
    aoi = _aoi_bbox(AOI_WKT)
    validation = _validate_outputs(
        root,
        Path(integration_bag),
        Path(integration_bgt),
        Path(integration_top10nl),
        Path(integration_cbs_buurtkaart),
        (
            Path(integration_ahn3),
            Path(integration_ahn4),
            Path(integration_ahn5),
            Path(integration_ahn6),
        ),
        aoi,
        gdal,
        lastools,
    )
    files: dict[str, dict[str, Any]] = {}
    for path in sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    ):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        files[str(path.relative_to(root))] = {
            "size": path.stat().st_size,
            "sha256": digest,
        }

    def rel(path: Path | str) -> str:
        return str(Path(path).resolve().relative_to(root.resolve()))

    bag_value = extract_bag if isinstance(extract_bag, tuple) else (extract_bag, {}, "")
    sources = {
        "bag": {
            "path": rel(integration_bag),
            "metadata": bag_value[1],
            "shortdate": bag_value[2],
        },
        "bgt": {"path": rel(integration_bgt)},
        "top10nl": {"path": rel(integration_top10nl)},
        "cbs_key_figures": {"path": rel(integration_cbs_key_figures)},
        "cbs_buurtkaart": {"path": rel(integration_cbs_buurtkaart)},
    }
    partitions: dict[str, Any] = {}
    for version, index in (
        (3, tile_index_ahn3),
        (4, tile_index_ahn4),
        (5, tile_index_ahn5),
    ):
        version_dir = root / "pointclouds" / f"AHN{version}"
        partitions[str(version)] = {
            tile_id: {
                "path": rel(version_dir / Path(entry["url"]).name),
                "url": entry["url"],
                "size": (version_dir / Path(entry["url"]).name).stat().st_size,
                "sha256": hashlib.sha256(
                    (version_dir / Path(entry["url"]).name).read_bytes()
                ).hexdigest(),
            }
            for tile_id, entry in index.items()
            if entry.get("url") and (version_dir / Path(entry["url"]).name).is_file()
        }
    partitions["6"] = {}
    for tile_id, entry in tile_index_ahn6.items():
        if not entry.get("url"):
            continue
        path = root / "pointclouds" / "AHN6" / Path(entry["url"]).name
        if path.is_file():
            x, y = (int(value) for value in tile_id.split("_"))
            batch = f"{x // 10000 * 10000:06d}_{y // 10000 * 10000:06d}"
            partitions["6"].setdefault(batch, {})[tile_id] = {
                "path": rel(path),
                "url": entry["url"],
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
    manifest = {
        "fixture_version": FIXTURE_VERSION,
        "date": datetime.now(tz=UTC).date().isoformat(),
        "aoi": {
            "wkt": AOI_WKT,
            "minx": aoi[0],
            "miny": aoi[1],
            "maxx": aoi[2],
            "maxy": aoi[3],
        },
        "clipping": {"vectors": VECTOR_PROCESSING, "pointclouds": "las2las_keep_xy"},
        "validation": validation,
        "sources": sources,
        "ahn": {
            # Checksums are embedded in the per-version tile indexes.
            "indexes": {
                "3": tile_index_ahn3,
                "4": tile_index_ahn4,
                "5": tile_index_ahn5,
                "6": tile_index_ahn6,
            },
            "checksums": {
                "3": tile_index_ahn3,
                "4": tile_index_ahn4,
                "5": tile_index_ahn5,
                "6": tile_index_ahn6,
            },
            "partitions": partitions,
        },
        "files": files,
    }
    return _write_json(root / "manifest.json", manifest)
