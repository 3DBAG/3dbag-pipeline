"""Manifest discovery and transactional PostGIS streetlidar catalogs.

Paths in tile rows are relative to the catalog root. Footprints are 2D RD;
the point coordinates use the compound RD + NAP CRS, EPSG:7415.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from psycopg import Connection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from bag3d.common.types import PostgresTableIdentifier

TILE_INDEX = PostgresTableIdentifier("streetlidar", "tile_index")
CRS = "EPSG:7415"
LOCK_ID = 741528992


def fingerprint(value: Any) -> str:
    return sha256(
        json.dumps(value, sort_keys=True, allow_nan=False).encode()
    ).hexdigest()


def contained_path(root: Path, relative: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError(f"Expected a relative streetlidar path: {relative!r}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Streetlidar path escapes root: {relative!r}")
    return path


def validate_manifest(value: Any) -> dict:
    if not isinstance(value, dict) or value.get("version") != 1:
        raise ValueError("Expected a version-1 streetlidar manifest")
    if value.get("crs") != CRS:
        raise ValueError(f"Streetlidar coordinates must use {CRS} (RD + NAP)")
    datasets = value.get("datasets")
    if not isinstance(datasets, list):
        raise ValueError("Streetlidar manifest must list datasets")
    ids = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise ValueError("Invalid streetlidar dataset entry")
        identifier = dataset.get("id")
        if (
            not isinstance(identifier, str)
            or not identifier.strip()
            or identifier in ids
        ):
            raise ValueError(
                f"Invalid or duplicate streetlidar dataset ID: {identifier!r}"
            )
        ids.add(identifier)
        relative = dataset.get("path")
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or ".." in Path(relative).parts
            or relative == "."
        ):
            raise ValueError(f"Invalid streetlidar dataset directory: {relative!r}")
    return value


@dataclass(frozen=True)
class Tile:
    dataset_id: str
    location: str
    size: int
    mtime_ns: int
    bounds: tuple[float, float, float, float]
    point_count: int
    metadata: dict

    def validate(self) -> None:
        if (
            len(self.bounds) != 4
            or not all(math.isfinite(v) for v in self.bounds)
            or self.bounds[0] > self.bounds[2]
            or self.bounds[1] > self.bounds[3]
            or self.point_count <= 0
            or self.size <= 0
        ):
            raise ValueError(f"Invalid or empty streetlidar tile: {self.location}")

    def path(self, root: Path) -> Path:
        path = contained_path(root, self.location)
        stat = path.stat()
        if not path.is_file() or (stat.st_size, stat.st_mtime_ns) != (
            self.size,
            self.mtime_ns,
        ):
            raise ValueError(f"Missing or changed streetlidar tile: {path}")
        return path


@dataclass
class Catalog:
    root: Path | None
    manifest: dict
    manifest_sha256: str
    tiles: list[Tile]
    source_fingerprint: str | None = None

    def validate_root(self, root: Path) -> Path:
        """Require consumers to use the store from the published catalog."""
        root = root.resolve()
        if self.root is not None and self.root.resolve() != root:
            raise ValueError(
                "Streetlidar store differs from the published catalog; "
                "refresh streetlidar/tile_index after changing the directory"
            )
        return root

    @property
    def identity(self) -> str:
        return fingerprint(
            {
                "version": 1,
                "root": str(self.root) if self.root else None,
                "manifest": self.manifest,
                "manifest_sha256": self.manifest_sha256,
                "tiles": [
                    asdict(tile)
                    for tile in sorted(self.tiles, key=lambda t: t.location)
                ],
                "source_fingerprint": self.source_fingerprint,
            }
        )

    @property
    def datasets(self) -> dict[str, int]:
        counts = Counter(tile.dataset_id for tile in self.tiles)
        return {
            dataset["id"]: counts[dataset["id"]]
            for dataset in self.manifest["datasets"]
        }


def empty_catalog() -> Catalog:
    manifest = {"version": 1, "crs": CRS, "datasets": []}
    return Catalog(None, manifest, fingerprint(manifest), [])


def inventory(root: Path) -> tuple[dict, str, dict[str, tuple[str, int, int]]]:
    payload = (root / "manifest.json").read_bytes()
    manifest = validate_manifest(json.loads(payload))
    files = {}
    seen_files = set()
    for dataset in manifest["datasets"]:
        directory = contained_path(root, dataset["path"])
        if not directory.is_dir():
            raise ValueError(f"Missing streetlidar dataset directory: {directory}")
        count = 0
        for path in sorted(directory.rglob("*")):
            if path.suffix.lower() not in {".las", ".laz"}:
                continue
            if path.is_symlink() and not path.is_file():
                raise ValueError(f"Missing streetlidar symlink target: {path}")
            if not path.is_file():
                continue
            location = path.relative_to(root).as_posix()
            resolved = contained_path(root, location)
            stat = resolved.stat()
            physical_id = (stat.st_dev, stat.st_ino)
            if physical_id in seen_files:
                raise ValueError(f"Duplicate streetlidar file: {path}")
            seen_files.add(physical_id)
            files[location] = (dataset["id"], stat.st_size, stat.st_mtime_ns)
            count += 1
        if count == 0:
            raise ValueError(f"Streetlidar dataset contains no tiles: {dataset['id']}")
    return manifest, sha256(payload).hexdigest(), files


def discover(
    root: Path,
    inspect: Callable[[Path], tuple[tuple[float, float, float, float], int, dict]],
    previous: Mapping[str, Tile] | None = None,
    force: bool = False,
) -> Catalog:
    root = root.resolve()
    manifest, digest, files = inventory(root)
    tiles = []
    for location, (dataset_id, size, mtime_ns) in sorted(files.items()):
        old = (previous or {}).get(location)
        if (
            not force
            and old
            and (old.dataset_id, old.size, old.mtime_ns) == (dataset_id, size, mtime_ns)
        ):
            tile = old
        else:
            bounds, count, metadata = inspect(contained_path(root, location))
            tile = Tile(dataset_id, location, size, mtime_ns, bounds, count, metadata)
        tile.validate()
        tiles.append(tile)
    if inventory(root) != (manifest, digest, files):
        raise ValueError(
            "Streetlidar source changed during discovery; retry after copying finishes"
        )
    return Catalog(root, manifest, digest, tiles)


def ensure_tables(connection: Connection) -> None:
    connection.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_ID,))
    connection.execute("CREATE SCHEMA IF NOT EXISTS streetlidar")
    connection.execute("""CREATE TABLE IF NOT EXISTS streetlidar.tile_index (
        location text PRIMARY KEY, dataset_id text NOT NULL,
        size bigint NOT NULL, mtime_ns bigint NOT NULL, point_count bigint NOT NULL,
        boundary geometry(Polygon,28992) NOT NULL, metadata jsonb NOT NULL)""")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS streetlidar_boundary_idx ON streetlidar.tile_index USING gist(boundary)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS streetlidar_dataset_idx ON streetlidar.tile_index(dataset_id)"
    )
    connection.execute("""CREATE TABLE IF NOT EXISTS streetlidar.catalog_state (
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
        root text, manifest jsonb NOT NULL, manifest_sha256 text NOT NULL,
        fingerprint text NOT NULL, source_fingerprint text,
        datasets jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())""")


def read_catalog(
    connection: Connection, table: PostgresTableIdentifier = TILE_INDEX
) -> Catalog:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SELECT * FROM streetlidar.catalog_state WHERE singleton")
        state = cursor.fetchone()
        if state is None:
            raise ValueError(
                "Streetlidar catalog is not published; materialize streetlidar/tile_index"
            )
        cursor.execute(
            sql.SQL("""SELECT dataset_id, location, size, mtime_ns, point_count, metadata,
            ST_XMin(boundary) minx, ST_YMin(boundary) miny,
            ST_XMax(boundary) maxx, ST_YMax(boundary) maxy FROM {} ORDER BY location""").format(
                table.id
            )
        )
        tiles = [
            Tile(
                row["dataset_id"],
                row["location"],
                row["size"],
                row["mtime_ns"],
                (row["minx"], row["miny"], row["maxx"], row["maxy"]),
                row["point_count"],
                row["metadata"],
            )
            for row in cursor
        ]
    return Catalog(
        Path(state["root"]) if state["root"] else None,
        state["manifest"],
        state["manifest_sha256"],
        tiles,
        state["source_fingerprint"],
    )


def publish(connection: Connection, catalog: Catalog) -> dict:
    """Caller holds the refresh advisory lock and commits only on success."""
    identity = catalog.identity
    stats = {
        "tiles": len(catalog.tiles),
        "datasets": catalog.datasets,
        "fingerprint": identity,
        "manifest_sha256": catalog.manifest_sha256,
        "added": 0,
        "changed": 0,
        "removed": 0,
    }
    state = connection.execute(
        "SELECT fingerprint FROM streetlidar.catalog_state WHERE singleton"
    ).fetchone()
    if state and state[0] == identity:
        return stats
    with connection.cursor() as cursor:
        cursor.execute("SELECT location FROM streetlidar.tile_index")
        before = {row[0] for row in cursor}
        changed = added = 0
        for tile in catalog.tiles:
            tile.validate()
            minx, miny, maxx, maxy = tile.bounds
            with connection.cursor() as writer:
                writer.execute(
                    """INSERT INTO streetlidar.tile_index AS old
                    (location,dataset_id,size,mtime_ns,point_count,boundary,metadata)
                    VALUES (%s,%s,%s,%s,%s,ST_MakeEnvelope(%s,%s,%s,%s,28992),%s)
                    ON CONFLICT(location) DO UPDATE SET
                    dataset_id=excluded.dataset_id,size=excluded.size,mtime_ns=excluded.mtime_ns,
                    point_count=excluded.point_count,boundary=excluded.boundary,metadata=excluded.metadata
                    WHERE (old.dataset_id,old.size,old.mtime_ns,old.point_count,old.boundary,old.metadata)
                    IS DISTINCT FROM (excluded.dataset_id,excluded.size,excluded.mtime_ns,
                                      excluded.point_count,excluded.boundary,excluded.metadata)
                    RETURNING location""",
                    (
                        tile.location,
                        tile.dataset_id,
                        tile.size,
                        tile.mtime_ns,
                        tile.point_count,
                        minx,
                        miny,
                        maxx,
                        maxy,
                        Jsonb(tile.metadata),
                    ),
                )
                if writer.fetchone():
                    if tile.location in before:
                        changed += 1
                    else:
                        added += 1
        cursor.execute(
            "DELETE FROM streetlidar.tile_index WHERE NOT (location = ANY(%s))",
            ([t.location for t in catalog.tiles],),
        )
        removed = cursor.rowcount
        cursor.execute(
            """INSERT INTO streetlidar.catalog_state
            (singleton,root,manifest,manifest_sha256,fingerprint,source_fingerprint,datasets)
            VALUES (true,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(singleton) DO UPDATE SET root=excluded.root,manifest=excluded.manifest,
            manifest_sha256=excluded.manifest_sha256,fingerprint=excluded.fingerprint,
            source_fingerprint=excluded.source_fingerprint,datasets=excluded.datasets,updated_at=now()""",
            (
                str(catalog.root) if catalog.root else None,
                Jsonb(catalog.manifest),
                catalog.manifest_sha256,
                identity,
                catalog.source_fingerprint,
                Jsonb(catalog.datasets),
            ),
        )
    return {**stats, "added": added, "changed": changed, "removed": removed}


def selected_locations(
    connection: Connection,
    source: PostgresTableIdentifier,
    polygon_buffer: float,
    table: PostgresTableIdentifier = TILE_INDEX,
) -> set[str]:
    """Conservative coverage query; the estimator performs exact crop selection."""
    rows = connection.execute(
        sql.SQL("""SELECT DISTINCT t.location FROM {} t
        JOIN {} g ON t.boundary && ST_Expand(g.geom,%s)""").format(table.id, source.id),
        (max(0.0, polygon_buffer),),
    )
    return {row[0] for row in rows}


def fixture_catalog(
    root: Path, relative: str, verify: Callable[[str], None]
) -> Catalog:
    """Load only declared fixture files; relocation may change filesystem mtimes."""
    path = contained_path(root, relative)
    verify(relative)
    value = json.loads(path.read_bytes())
    if value.get("version") != 1 or value.get("crs") != CRS:
        raise ValueError("Invalid streetlidar fixture catalog")
    manifest = validate_manifest(value["manifest"])
    datasets = {dataset["id"]: dataset["path"] for dataset in manifest["datasets"]}
    tiles = []
    locations = set()
    fixture_root = path.parent
    for record in value["tiles"]:
        location = record["location"]
        tile_path = contained_path(fixture_root, location)
        verify(tile_path.relative_to(root.resolve()).as_posix())
        stat = tile_path.stat()
        dataset_path = datasets.get(record["dataset_id"])
        if dataset_path is None or not tile_path.is_relative_to(
            contained_path(fixture_root, dataset_path)
        ):
            raise ValueError(f"Tile outside declared streetlidar dataset: {location}")
        tile = Tile(
            record["dataset_id"],
            location,
            record["size"],
            stat.st_mtime_ns,
            tuple(record["bounds"]),
            record["point_count"],
            record["metadata"],
        )
        tile.validate()
        tile.path(fixture_root)
        if location in locations:
            raise ValueError(f"Duplicate streetlidar fixture tile: {location}")
        locations.add(location)
        tiles.append(tile)
    catalog = Catalog(
        fixture_root,
        manifest,
        fingerprint(manifest),
        tiles,
        value["source_fingerprint"],
    )
    if any(count == 0 for count in catalog.datasets.values()):
        raise ValueError("Streetlidar fixture declares a dataset without tiles")
    return catalog
