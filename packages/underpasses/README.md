# Underpasses

Detect 2D underpass polygons by comparing the current BAG and BGT building
footprints. This package adapts `underpass_detection_2d` from
`underpass-detection-experiments` into an independent 3DBAG Dagster workflow.

## Inputs and algorithm

The detection stage now consumes core's `input/intermediary/bag_bgt_join` asset
instead of materializing its own BAG/BGT join. That table matches the BGT BAG
number to the BAG identifier after its 14-character prefix, unions matching BGT
polygons per building, and applies core's relative-height filtering.

The detection portion of the `underpasses` job materializes three assets in order;
edge classification and offset follow them:

1. `underpasses/bag_minus_bgt`: BAG minus BGT, retaining differences that survive
   erosion and dilation (the double-buffer test).
2. `underpasses/snapped_differences`: intersect the two differences obtained by
   snapping BAG to BGT and BGT to BAG, for buildings surviving the previous stage.
3. `underpasses/geometries`: split results into polygons, repeat the double-buffer
   test on each polygon, and assign sequential integer IDs.

The buffering is a filter: output polygons retain their original boundaries.
Existing geometry helpers retain their empty-result fallback behavior for geometry
errors. Unexpected worker and database failures fail the asset and roll back its
table replacement. Processing streams ordered chunks with bounded queued work;
worker completion order does not affect output IDs.

## Running

Install independently with `uv --project packages/underpasses sync`. Run
`make local_dev`, or start the standard Docker Compose stack. The Docker code
location `underpasses` serves gRPC on port 4004. Materialize the BAG and BGT input
assets first, then launch the `underpasses` job. The manifest's pipeline stage
runs after reconstruction and the filled AHN terrain job. Preprocessing supplies
the BAG, BGT, and adjacency inputs.

The package uses the shared `computation_db` resource and its existing
`BAG3D_PG_*` environment configuration. It needs no separate database credentials
or sibling checkout. Each stage returns a `PostgresTableIdentifier`, persisted by
the shared Dagster instance's default filesystem IO manager. Local and Docker
workspace configurations use the same instance artifact storage for cross-location
input loading.

Configure geometry assets through their Dagster run configuration, for example:

```yaml
ops:
  bag_minus_bgt:
    config:
      concurrency: 4
      chunk_size: 1000
      buffer_distance: 0.2
  snapped_differences:
    config:
      concurrency: 4
      chunk_size: 1000
      snap_tolerance: 0.01
  geometries:
    config:
      concurrency: 4
      chunk_size: 1000
      buffer_distance: 0.2
```

These values are the defaults. `BAG3D_CONCURRENCY_TOOL_UNDERPASSES` overrides the
default worker count. Concurrency and chunk size must be positive integers;
distances must be finite and nonnegative, in metres (EPSG:28992).

Select an individual asset to rerun a stage using materialized upstream tables;
this replaces the original script's `--skip-step*` options. Tables are replaced on
rerun. Avoid overlapping runs that rebuild the same tables.

## Output

The detection and height results remain in the computation database. Roofer
incorporates them into its reconstructed CityJSON corpus, which feeds party-wall
analysis, floor estimation, and export.

| Column | Type |
| --- | --- |
| `underpass_id` | `INTEGER PRIMARY KEY` |
| `identificatie` | `TEXT NOT NULL`, full BAG identifier |
| `geom` | `geometry(Polygon, 28992) NOT NULL` |

The final table is `underpasses.geometries`, with an identifier index and a GiST
geometry index. IDs are deterministic for unchanged input and settings, but are
regenerated on each run and are not stable identifiers across source changes.
Intermediate tables live in the same schema. Materialization metadata includes
row counts and, for geometry stages, processed-building and written-row counts.

## Tests

```bash
uv --project packages/underpasses run pytest packages/underpasses/tests -v
```

Unit tests run offline. To also run isolated PostGIS tests, set
`BAG3D_UNDERPASSES_TEST_DSN` to a disposable test database connection string. Tests
create unique schemas and remove them during cleanup; they never use production
BAG/BGT or underpasses tables.

## Edge classification and selective offset

After detection, `underpasses/edges` classifies boundaries as `interior`,
`exterior`, or `shared`. It consumes `input/intermediary/bag_adjacency` through the
shared IO manager and uses neighbours' **BGT** footprints from `bag_bgt_join`.
Materialize the core preprocessing job before running this extended workflow.
Classification defaults are `grid_size: 0.001` and `snap_tolerance: 0.03` metres.
Hole boundaries are interior edges.

The `offset_edges` computation produces two coupled assets:
`underpasses/extended_geometries` and `underpasses/skipped_underpasses`.
Exterior boundaries move by `offset_distance: 0.05` metres by default;
interior/shared boundaries remain on their fixed supporting lines. The offset
uses the experiment's boolean-patch strategy with `tolerance: 0.001` metres.
Signed finite distances are supported; zero requests the validated original
polygon. Override these settings under `ops.offset_edges.config` in run config.
Both new stages also accept `concurrency` and `chunk_size` as above.

`edges` stores an integer `edge_id`, original `underpass_id`, full BAG
`identificatie`, `edge_type`, and `geom geometry(LineString,28992)`.
`extended_geometries` stores `identificatie`, `underpass_id`, `offset_distance`,
and `geom geometry(Polygon,28992)`. `skipped_underpasses` stores `identificatie`,
`underpass_id`, `skip_reason`, and `skipped_at`. Offset tables are keyed by the
building/underpass pair. IDs from detection are preserved.

Every detected underpass gets an offset polygon or a skip record. Known skips
include `no_movable_edges`, `invalid_input_polygon`, `geometry_offset_failed`,
`edge_matching_failed`, and `value_error`. A failed nonzero offset never silently
returns the original polygon. Unexpected errors fail the stage and roll back
both table replacements. Tables are rebuilt on reruns to avoid stale IDs and
settings; successes and skips materialize together. Metadata reports processed,
written, and skipped counts and a breakdown of skip reasons.

Before the first classification run, materialize core's
`input/intermediary/bag_adjacency` with its missing upstream assets. It produces
`reconstruction_input.bag_adjacency` and stores the table identifier under the
same fully prefixed asset key in shared Dagster storage. The database table alone
is insufficient for the filesystem IO manager to load this input.

If classification previously failed while loading `bag_adjacency`, deploy the
corrected code location, materialize that core prerequisite, and then select
`underpasses/edges`, `underpasses/extended_geometries`, and
`underpasses/skipped_underpasses`. Existing detection outputs can be reused;
there is no need to rerun detection or change national-data mode.

## Heights for Roofer reconstruction

The complete job runs before Roofer. `underpasses/heights` consumes
`underpasses/extended_geometries` and the manifest returned by
`terrain/ahn_filled`. The separate `terrain_ahn` job prepares the four AHN stages
without generating viewer tiles. Its filled 5 m rasters retain NAP elevations;
viewer terrain tiles use ellipsoidal heights and are not inputs to this job.

Heights are rebuilt atomically in `underpasses.heights`, with the same building /
underpass key and geometry as the offset table. Columns include a nullable scalar
`underpass_candidate_elevations` (absolute NAP metres), terrain elevation,
confidence, source, status, timestamp, and JSON diagnostics including the terrain
manifest SHA-256. Every offset polygon gets a result, including polygons outside
lidar coverage. Missing terrain never produces unfiltered lidar peaks.

Streetlidar comes from the managed `streetlidar/tile_index` asset in core,
backed by the computation database's `streetlidar.tile_index` table. Configure
its source with `BAG3D_STREETLIDAR_ROOT` and a root `manifest.json` declaring
`"crs": "EPSG:7415"` and dataset IDs/directories. Materialize the index after
adding data; `source_input` also refreshes it during complete runs. Heights
queries intersecting tiles, combines all active datasets, and records the catalog
fingerprint alongside terrain provenance. An unconfigured or explicitly empty
source retains heuristic results. A configured missing or invalid source fails.
The source root must be readable at the same path by core and underpasses.
See the streetlidar dataset guide for onboarding, mounts, and fixture generation.

The default profile follows the experiment: 500 m spatial grid, 16 batch workers,
5 point-in-polygon workers and 1 height worker per batch, with 3 Rayon threads.
These are launch settings, and can be reduced for smaller machines. Chunk size is
one million points and each polygon has a ten-million-point memory limit. Crops
remain in memory. Terrain sampling uses native-grid P90, pixel centres within the
polygon, intersecting pixels when no centre exists, and first-valid raster order
in overlaps. Peaks within 2 m of terrain are excluded. Native-grid sampling can
differ numerically from the previous Web Mercator tile sampling.

Roofer consumes `underpasses/heights` during each reconstruction partition. A
per-tile view selects all polygons matching the tile's building IDs, preserving
holes and multiple underpasses per building. `underpass_candidate_elevations`
contains absolute NAP ceiling elevations; NULL values are passed through to
Roofer, which uses `underpass-default-height = 2.5` above the building floor only
for those values. Measured elevations are used directly. This requires Roofer's
updated NULL-height fallback support.

The resulting corpus lives in `stages/reconstruction/` and party-wall processing
reads it through `reconstruction_index`. Roofer owns ceiling semantics and
geometry diagnostics; the old carving reports and custom surface provenance are
retired. Historical `stages/underpasses/` data is unused.

The height implementation uses `underpass-tools` 0.3.0, with a caller-owned
tile-record API so database access remains in the pipeline adapter. The pinned
Linux x86_64 wheel comes from the immutable
[v0.3.0 release](https://github.com/3DBAG/underpass-tools/releases/tag/v0.3.0).
Its URL is pinned in `pyproject.toml` and its SHA-256 is recorded in the lockfile.
It bundles the Zig point-in-polygon library and needs no compiler during
installation. The companion repository retains its standalone GeoPackage CLI
interface. Upstream tool tests run there; the pipeline retains its adapter,
fixture, and optional PostGIS tests.

Materialize preprocessing and `terrain/ahn_filled`, then run the `underpasses` job
before reconstructing and regenerating downstream outputs. Avoid overlapping
complete jobs because detection and height assets replace shared database tables.

The tools-image manifest pins Roofer by digest. Build and publish the updated tools
image before building dependent pipeline images, then record its registry digest
through the existing tools-image workflow. The new tools tag must not be consumed
with the previous tools-image digest. If Roofer's NULL-height fallback ships in a
replacement image, update its Roofer digest and rebuild the tools image as well.
