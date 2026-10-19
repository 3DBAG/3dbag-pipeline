# Underpasses

Detect 2D underpass polygons by comparing the current BAG and BGT building
footprints. This package adapts `underpass_detection_2d` from
`underpass-detection-experiments` into an independent 3DBAG Dagster workflow.

## Inputs and algorithm

The first asset consumes the existing `bag/bag_pandactueelbestaand` and
`bgt/bgt_pandactueelbestaand` table identifiers. It matches the BGT BAG number to
the BAG identifier after its 14-character prefix, requires overlapping bounding
boxes, and unions matching BGT polygons per building. All matching BGT relative
height levels are included, preserving the experiment's behavior. Core's
`intermediary/bag_bgt_join` has different relative-height filtering and is not used.

The `underpasses` job materializes four assets in order:

1. `underpasses/bag_bgt_join`: BAG footprints and unioned BGT footprints.
2. `underpasses/bag_minus_bgt`: BAG minus BGT, retaining differences that survive
   erosion and dilation (the double-buffer test).
3. `underpasses/snapped_differences`: intersect the two differences obtained by
   snapping BAG to BGT and BGT to BAG, for buildings surviving the previous stage.
4. `underpasses/geometries`: split results into polygons, repeat the double-buffer
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
runs after preprocessing, which materializes these inputs.

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

Results remain in the computation database; reconstruction and export do not
consume them in this integration.

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
