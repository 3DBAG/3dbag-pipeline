# ADR 0004: cjindex as Stage Interface and CityJSONSeq over Per-Feature Files

**Status:** Accepted
**Date:** 2026-04-01

## Context

The intermediate stages in the pipeline (`reconstruction`, `party_walls`, `floors_estimation`)
carry building-level CityJSON features forward between workflows. Historically, downstream code
treated those stages as **directories of individual feature files**:

- feature discovery depended on walking the filesystem or materializing `{id: path}` maps
- DAG ordering included dedicated `features_file_index` assets whose only job was to open or
  refresh a `cjindex` SQLite index
- consumers often assumed that one feature corresponded to one file on disk
- byte-oriented reads (`read_feature_bytes`, `get_bytes`) encouraged downstream code to rebuild
  ad-hoc CityJSON structures instead of consuming actionable feature data directly

This was especially visible in the `party_walls` integration, where downstream code wrapped raw
feature bytes into a fake CityJSON object just to call geometry code. That is the wrong boundary.
`cjindex` sits on top of `cjlib`, which already understands CityJSON and CityJSONFeature data;
the pipeline should consume `cjindex` as the stage access layer, not work around it.

At the same time, the physical storage layout is evolving. A stage can be stored as:

- many per-feature `.city.jsonl` files
- tile-level CityJSONSeq / NDJSON files containing many features

From the pipeline consumer's perspective, these should be equivalent as long as the stage is
indexable by `cjindex`.

## Decision

Use `cjindex` as the **logical stage interface** for intermediate CityJSON data.

Concretely:

1. Downstream assets must access stage features through `cjindex` resources and APIs such as
   `open_ready_index(...)`, `feature_ref_page(...)`, `read_feature_json(...)`, `get_json(...)`,
   `read_feature(...)`, or `get(...)`.

2. The pipeline must not model index refresh as a separate asset boundary. Remove dedicated
   `features_file_index` assets and let consuming assets open or refresh the index directly.

3. The stage contract is no longer “directory of per-feature files”. The stage contract is
   “directory tree that `cjindex` can index as a corpus of CityJSON features”.

4. Prefer CityJSONSeq / NDJSON tile files over one-feature-per-file layouts when writing new
   intermediate stage data, provided the stage remains indexable by `cjindex`.

5. Downstream code must not reconstruct fake CityJSON wrappers from raw bytes when `cjindex`
   already provides semantically usable JSON or model-level data.

## Rationale

### 1. Decouple logical access from physical layout

The pipeline cares about operations such as:

- enumerate features in a stage
- fetch one feature by feature id
- fetch one feature by `FeatureRef`
- preserve feature provenance (`source_path`, offsets, source id)

Those are indexing concerns, not filesystem-layout concerns. Once consumers use `cjindex`,
changing the physical representation from per-feature files to CityJSONSeq becomes an internal
producer choice rather than a cross-pipeline migration.

### 2. CityJSONSeq is a more natural storage format for tiled stage outputs

One-feature-per-file layouts create unnecessary filesystem overhead:

- large inode counts
- deep object-directory trees
- expensive directory traversal when indexing or validating stages
- weaker locality for tile-oriented processing

CityJSONSeq keeps many features in a tile-level stream while still allowing per-feature random
access through `cjindex` feature references. This matches how the pipeline already thinks about
reconstruction and export in tile space.

### 3. `cjindex` should return actionable data

The correct abstraction is not “here are some bytes; downstream can patch them up”. The correct
abstraction is “here is the feature you asked for”. The newer Python API supports this directly:

- `read_feature_json(ref)` / `get_json(feature_id)` for JSON-level consumption
- `read_feature(ref)` / `get(feature_id)` for model-level consumption

This makes the integration align with the Rust API and with the broader `cjlib` / `serde_cityjson`
stack.

### 4. Remove false asset boundaries from the Dagster graph

`features_file_index` did not produce durable domain data. It was an operational convenience
wrapper around `open_ready_index(...)`. Modeling that as an asset:

- added unnecessary nodes to the graph
- obscured the real stage dependency (`reconstruction` -> `party_walls`, `party_walls` ->
  `floors_estimation`)
- created tests and jobs around an implementation detail rather than business output

The real assets are the stage-writing assets; index readiness is part of how consumers open the
stage, not a separate data product.

## Implementation Notes

The current implementation follows this decision:

- `party_walls.building_surfaces` opens the reconstruction index directly and now depends on
  `reconstruction.reconstructed_building_models`
- `floors_estimation.bag3d_features` and `floors_estimation.save_cjfiles` open the
  `party_walls` index directly and depend on `party_walls.building_surfaces`
- the dedicated `features_file_index` assets were removed
- the dedicated `party_walls_index` job was removed
- `party_walls` now consumes feature JSON from `cjindex` directly instead of constructing a fake
  CityJSON wrapper from raw bytes

The helper `_tile_id_from_source_path(...)` in `party_walls` remains intentionally tolerant of
both layouts. It derives tile ids from path segments under the stage root so the consumer can
work with either:

- per-feature files under tile directories
- tile-level CityJSONSeq files under the same tile hierarchy

## Consequences

- Stage consumers are simpler and more ergonomic: they consume features, not byte blobs.
- The pipeline graph now expresses real data dependencies instead of index-maintenance steps.
- Producers can change intermediate storage from per-feature files to CityJSONSeq without forcing
  downstream logic changes, as long as `cjindex` keeps exposing stable feature access.
- Tests should focus on stage semantics and indexed feature access, not on dedicated
  `features_file_index` assets.
- Local development must keep `cjindex` and `cjlib` resolvable in package environments because
  they are now part of the normal execution path rather than optional helpers.

## Rejected Alternatives

### Keep `features_file_index` as a graph-visible asset

Rejected because it models index refresh as a data product. That boundary adds noise to the DAG
without adding domain value.

### Keep per-feature files as the required stage contract

Rejected because it hard-codes a storage optimization problem into all downstream consumers.
It prevents the pipeline from using tile-level CityJSONSeq corpora even though `cjindex` can
address individual features within them.

### Keep byte-oriented `cjindex` integration in downstream code

Rejected because it leaks serialization details across layers and encourages ad-hoc repair code,
including fake CityJSON wrappers that should not exist at the pipeline boundary.

## Related Files

| File | Role |
|------|------|
| `packages/common/src/bag3d/common/resources/cjindex.py` | Shared `cjindex` Dagster resource and `open_ready_index(...)` helper |
| `packages/party_walls/src/bag3d/party_walls/assets/party_walls.py` | Direct indexed consumption of reconstruction features |
| `packages/floors_estimation/src/bag3d/floors_estimation/assets/floors_estimation.py` | Direct indexed consumption of `party_walls` stage features |
| `packages/export/src/bag3d/export/assets/export/metadata.py` | Indexed read of reconstruction-stage features for evaluation |
| `docs/data_flow.md` | Current stage-to-stage description after removing `features_file_index` assets |
