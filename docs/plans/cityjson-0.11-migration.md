# CityJSON 0.11 Pipeline Migration Plan

## Goal

Remove the vendored `cjlib` and `cjindex` wheels and migrate the pipeline to
the published `cityjson-lib==0.11.0` and `cityjson-index==0.11.0` Python
packages. Preserve the existing Dagster asset graph and the `z/x/y` layout of
the reconstruction, party-walls, and floors-estimation stage outputs.

This work starts only after both 0.11.0 distributions are available from
PyPI. The upstream API and release work is specified in the cityjson-rs plan
[`cityjson-index-package-source-paths.md`](../../../cityjson-rs/docs/plans/cityjson-index-package-source-paths.md).

## 1. Dependency migration

Use exact pins for the CityJSON release line:

- `bag3d-common` directly depends on `cityjson-index==0.11.0` and
  `cityjson-lib==0.11.0`, because it imports both packages;
- `bag3d-floors-estimation` and `bag3d-party-walls` directly depend on
  `cityjson-index==0.11.0`, because their asset modules import it; and
- `bag3d-core` and `bag3d-export` receive both packages transitively through
  `bag3d-common` and do not retain unused direct CityJSON dependencies.

Remove every `cjindex` and `cjlib` entry from project dependencies and every
corresponding `[tool.uv.sources]` path override. Regenerate the committed
`uv.lock` files for common, core, export, floors estimation, and party walls
from PyPI. The resulting locks must resolve both CityJSON distributions to
0.11.0 registry artifacts with hashes and contain no `docker/vendor` paths.

Before changing the manifests, verify the published metadata and perform a
clean temporary installation of both packages. Confirm that
`cityjson-index==0.11.0` requires `cityjson-lib==0.11.0` and that both native
modules import on the pipeline's supported Linux environment.

## 2. Shared CityJSON access layer

Keep `CityIndexResource`, `open_ready_index`, and the existing
`bag3d.common.resources.cjindex` module path. Renaming that pipeline API is
outside this migration.

Replace the optional `cjindex` import with the required `cityjson_index`
module. Remove the vendored-wheel error messages and the `_native.LIB` probe;
the published binding loads its native library during import/open and should
surface its own error if that fails. Give `CityIndexResource.open()` and
`open_ready_index()` concrete `cityjson_index.OpenedIndex` return types.

Add shared helpers for the new package API:

- keyset-page `PackageRef` values with
  `package_ref_page_after_record_id(after_record_id, page_size)`, advancing the
  cursor to the final `record_id` in each page;
- obtain the total from `feature_bounds_summary().package_count` when an asset
  needs progress reporting;
- resolve package paths once per page with
  `package_source_paths(refs)`, relying on its input-order contract; and
- read a package through `OpenedIndex.read_package(ref)`, serialize the
  returned `CityModel` as a CityJSONFeature dictionary, and close the native
  model in `finally`.

All asset-level index handles must be closed after use. Worker processes may
retain one process-local index for their lifetime, as they do today.

Change the CityJSONSeq utility import from `cjlib` to `cityjson_lib`. The
`CityModel` parsing and `write_cityjsonseq_auto_transform_bytes()` behavior is
otherwise unchanged.

## 3. Asset migration

### Party walls

Replace every `FeatureRef` annotation and value with `PackageRef`. Use
`PackageRef.model_id` where the pipeline currently uses `feature_id`.

During the initial keyset scan:

1. resolve source paths for the whole page;
2. build the model-id-to-package-ref lookup used by adjacency processing; and
3. derive tile IDs from each returned source path relative to the
   reconstruction stage root.

Pass package refs to workers. Each worker opens the reconstruction index once
and converts `read_package(ref)` to feature JSON before invoking
`building_surfaces`. Preserve the existing process-pool behavior, timing
metadata, and output grouping.

### Floors estimation

Use keyset package pages for both `bag3d_features` and `save_cjfiles`.
`bag3d_features` extracts attributes from package JSON under `ref.model_id`.

For `save_cjfiles`, batch-resolve source paths alongside every page and pass
the path separately to the floor-injection worker. Continue deriving the
`z/x/y` tile directory from that path and writing one CityJSONSeq file per
tile. Preserve existing party-wall attributes while adding `b3_bouwlagen`.

### Export evaluation

Change `feature_evaluation` to keyset-page package refs, use `model_id` for the
reconstructed-building set, and inspect package JSON through the shared model
conversion helper. CSV fields and missing-building behavior remain unchanged.

## 4. Remove vendoring and update documentation

Delete the tracked `docker/vendor` directory, including both 0.3.0 wheels and
its `.gitignore`.

In all four pipeline Dockerfiles:

- remove bind mounts for the vendored wheels;
- remove `COPY docker/vendor ...`; and
- retain the existing frozen two-stage `uv sync` flow so dependencies are
  downloaded from the registry in the cached third-party layer.

Remove the `_copy_vendor_balazs` Make target and the `docker/vendor` exception
from `.dockerignore`.

Update the CityJSON index resource docstrings, `docs/data_flow.md`, and ADR
0004 implementation notes to describe package refs, keyset paging, actionable
`CityModel` reads, and batched package provenance. Historical references to
the `cjindex` CLI may remain, but runtime dependency and import examples must
use `cityjson-index` and `cityjson_index`.

## 5. Tests and acceptance criteria

Remove the `cjindex` and `cjlib` module stubs from package `conftest.py` files.
The test environments now install the real published bindings.

Update unit/data-flow tests to construct `cityjson_index.PackageRef` values,
mock cursor pages and ordered path batches, and mock or exercise model-to-JSON
conversion as appropriate.

Add a focused real-binding test in `bag3d-common` that creates a temporary
CityJSONSeq dataset under `10/434/716`, then verifies:

- opening and reindexing through `CityIndexResource`;
- keyset pagination and package count;
- ordered `package_source_paths()` results;
- package model IDs and feature JSON reads; and
- source paths that relativize to the expected tile.

Retain the stage-to-stage tests proving that:

- party walls are written under the original tile directory;
- floors estimation preserves party-wall attributes and adds the expected
  floor count;
- transforms and metadata survive CityJSONSeq rewriting; and
- export evaluation reports reconstructed and missing buildings correctly.

Run:

```shell
make lint
make test
docker compose -f docker/compose.yaml build \
  bag3d-core \
  bag3d-floors-estimation \
  bag3d-party-walls \
  bag3d-export
```

Finally, search the tracked source and lockfiles and require all of the
following:

- no vendored wheels or `docker/vendor` references remain;
- no `cjlib` or `cjindex` dependency/source declarations remain;
- no `import cjlib` or `import cjindex` statements remain; and
- every package environment resolves the two CityJSON distributions to
  exactly 0.11.0.

## Out of scope

- Renaming the pipeline's `cjindex.py` resource module.
- Changing Dagster asset keys, dependencies, schedules, or resource names.
- Flattening or otherwise redesigning intermediate stage storage.
- Reading the cityjson-index SQLite schema directly or walking source files to
  reconstruct provenance.
