"""Inspect an opened GeoPackage using GDAL JSON and SQLite result rows."""

import json
import shlex
import sqlite3
from pathlib import Path

from bag3d.common.resources.executables import CommandRunner
from bag3d.common.resources.specs import Specs3DBAGResource


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _ogr_json(runner: CommandRunner, path: Path, *, sql: str | None = None) -> dict:
    options = (
        "-ro -al -so -json"
        if sql is None
        else f"-ro -json -features -dialect SQLite -sql {shlex.quote(sql)}"
    )
    result = runner.run(
        f"{{exe}} {options} '{{local_path}}/{path.name}'",
        exe_name="ogrinfo",
        local_path=path.parent,
    )
    if not result.success:
        raise RuntimeError(
            f"GeoPackage {path.name}: ogrinfo failed for {sql or 'schema'}: {result.stderr}"
        )
    return json.loads(result.stdout)


def inspect_geopackage(
    path: Path, specs: Specs3DBAGResource, runner: CommandRunner
) -> tuple[dict, dict[str, int | None], list[str]]:
    info = _ogr_json(runner, path)
    counts = {"nr_building": None, "nr_buildingpart": None, "nr_invalid_2d_geom": None}
    failures = []
    building_counts, part_counts, invalid_counts = [], [], []
    observed = {layer["name"]: layer for layer in info["layers"]}
    with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        if integrity != [("ok",)]:
            failures.append(f"{path.name}: SQLite integrity check failed: {integrity}")
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        for name in tables - observed.keys():
            if not name.startswith(("sqlite_", "gpkg_", "rtree_")):
                # Include non-spatial user tables even if GDAL omitted them.
                info["layers"].append({"name": name, "fields": []})
                observed[name] = info["layers"][-1]
        for name, layer in observed.items():
            if name not in tables:
                failures.append(
                    f"{path.name}: GDAL layer {name!r} is absent from SQLite"
                )
                continue
            storage = connection.execute(
                f"PRAGMA table_info({_identifier(name)})"
            ).fetchall()
            storage_by_name = {row[1]: row for row in storage}
            for entry in layer["fields"]:
                if entry["name"] in storage_by_name:
                    entry["storageType"] = storage_by_name[entry["name"]][2]
            container = specs.geopackage_container(name)
            if container is None or container.selector is None:
                continue
            selector = container.selector
            count = connection.execute(
                f"SELECT COUNT(DISTINCT cityobject_id) FROM {_identifier(name)}"
            ).fetchone()
            if count is None:
                failures.append(
                    f"{path.name}: empty count query for {name}; observed layers: {sorted(observed)}"
                )
                continue
            if selector.city_object_type == "Building":
                building_counts.append(count[0])
            elif selector.city_object_type == "BuildingPart":
                part_counts.append(count[0])
        for name, layer in observed.items():
            container = specs.geopackage_container(name)
            if container is None or container.kind == "hierarchy":
                continue
            if container.kind != "semantics" and (
                container.selector is None
                or container.selector.geometry_family
                not in {"MultiSurface", "CompositeSurface"}
            ):
                continue
            sql = f"SELECT COUNT(DISTINCT cityobject_id) AS invalid_count FROM {_identifier(name)} WHERE ST_IsValid(geom) = 0"
            query = _ogr_json(runner, path, sql=sql)
            rows = [
                feature["properties"]
                for result_layer in query.get("layers", [])
                for feature in result_layer.get("features", [])
            ]
            if len(rows) != 1 or type(rows[0].get("invalid_count")) is not int:
                failures.append(
                    f"{path.name}: empty or invalid aggregate for query {sql!r}; observed layers: {sorted(observed)}"
                )
            else:
                invalid_counts.append(rows[0]["invalid_count"])
    for key, values, aggregate in (
        ("nr_building", building_counts, min),
        ("nr_buildingpart", part_counts, min),
        ("nr_invalid_2d_geom", invalid_counts, max),
    ):
        if values:
            counts[key] = aggregate(values)
        else:
            failures.append(
                f"{path.name}: no results for {key}; observed layers: {sorted(observed)}"
            )
    return info, counts, failures
