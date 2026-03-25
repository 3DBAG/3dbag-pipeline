# ADR 0003: building_surfaces Performance — ProcessPoolExecutor Initializer Pattern

**Status:** Accepted
**Date:** 2026-03-25

## Context

The `building_surfaces` asset in the party_walls workflow processes 39,267 buildings by loading
reconstructed CityJSONFeature files, querying adjacent building IDs, and computing shared walls.
A production run profiled with `BAG3D_PROFILE_BUILDING_SURFACES=1` revealed unexpectedly slow
execution: **3.2 hours wall-clock time** (11,511s `processing_total_s`) despite mean per-building
compute time of only 0.111s.

With 6 worker processes and 39,267 buildings:
- **Ideal wall-clock time:** 39,267 × 0.111s / 6 ≈ 726s (12 minutes)
- **Actual wall-clock time:** 11,511s (3.2 hours)
- **Overhead ratio:** 11,511 / 726 ≈ **16×**

This indicated severe parallelism overhead, not per-building compute slowness. Profiling analysis
revealed two distinct bottlenecks:

### Bottleneck 1: PyVista `.cell_normals` Computed Property in Loop

In `3dbag-surfaces` `geometry.py:area_by_surface()`, roof triangle area classification calls:

```python
for idx in triangle_idxs:
    if sized.cell_normals[idx].dot([0, 0, 1]) < sloped_threshold:
```

PyVista's `.cell_normals` property is **computed on every index access**; calling `.cell_normals[idx]`
inside a loop triggers `compute_normals()` for the entire mesh R times (where R = number of roof
triangles). For buildings with many roof triangles, this is O(N²) hidden recomputation.

**Fix:** Hoist the property fetch outside the loop:
```python
all_normals = sized.cell_normals
for idx in triangle_idxs:
    if all_normals[idx].dot([0, 0, 1]) < sloped_threshold:
```

This reuses the same cached normal array, reducing computation from O(N²) to O(N). The same
pattern was applied elsewhere (`face_planes()` → hoist `all_normals`). Implemented in 3dbag-surfaces
v2026.1010.

### Bottleneck 2: ProcessPoolExecutor Pickling Large Dicts Per `submit()` Call

The main bottleneck came from the concurrent executor pattern in `building_surfaces()`:

```python
with ProcessPoolExecutor(max_workers=6) as executor:
    for tile_id, buildings in tiles_with_buildings.items():
        for pand_id, path in buildings:
            future = executor.submit(
                _process_building,
                pand_id,
                path,
                adjacency,              # 52,692 entries
                features_file_index,    # 39,267 Path objects
                transform,
                output_dir,
                config.profile,
            )
```

**The problem:** Every `executor.submit()` call pickles ALL arguments to serialize them to a
worker process. This includes:
- `adjacency`: dict of 52,692 string entries
- `features_file_index`: dict of 39,267 Path objects

With 39,267 submit calls, the parent process serializes these two large dicts **39,267 times**:
- Estimated pickle size per call: ~9.5 MB (adjacency + features_index)
- Total: 39,267 × 9.5 MB ≈ **370 GB** of cumulative serialize/deserialize work
- This happens sequentially in the parent process, blocking submission of new futures

The executor's thread pool can only submit futures as fast as the parent process can pickle them,
creating a severe bottleneck.

## Decision

Apply the **initializer pattern** to ProcessPoolExecutor: send large read-only data to workers
**once per worker** (via `initializer`/`initargs`) instead of **once per work item** (via
`submit()` arguments).

## Implementation

### Before (Bottleneck)

```python
with ProcessPoolExecutor(max_workers=config.concurrency) as executor:
    for pand_id, path in buildings:
        future = executor.submit(
            _process_building,
            pand_id,
            path,
            adjacency,              # pickled 39,267 times
            features_file_index,    # pickled 39,267 times
            transform,
            output_dir,
            config.profile,
        )
```

### After (Initializer Pattern)

**Define worker-level globals and initializer:**

```python
_worker_adjacency: dict[str, list[str]] = {}
_worker_features_index: dict[str, Path] = {}
_worker_transform: dict = {}

def _init_worker(
    adjacency: dict[str, list[str]],
    features_index: dict[str, Path],
    transform: dict,
) -> None:
    """Initializer for ProcessPoolExecutor workers.

    Stores the large read-only dicts once per worker process instead of
    pickling them with every submit() call.
    """
    global _worker_adjacency, _worker_features_index, _worker_transform
    _worker_adjacency = adjacency
    _worker_features_index = features_index
    _worker_transform = transform
```

**Simplify worker function signature and use globals:**

```python
def _process_building(
    pand_id: str,
    target_path: Path,
    output_dir: Path,
    profile: bool,
) -> BuildingProcessingResult:
    """Process a single building.

    Accesses adjacency, features_index, and transform from worker-level globals
    set by _init_worker.
    """
    target_cm, target_part_id = _load_feature_as_citymodel(
        target_path, _worker_transform
    )
    for adj_id in _worker_adjacency.get(pand_id, []):
        adj_path = _worker_features_index.get(adj_id)
        ...
```

**Create executor with initializer:**

```python
with ProcessPoolExecutor(
    max_workers=config.concurrency,
    initializer=_init_worker,
    initargs=(adjacency, features_file_index, transform),
) as executor:
    for pand_id, path in buildings:
        future = executor.submit(
            _process_building,
            pand_id,
            path,
            output_dir,
            config.profile,
        )  # Only small per-item args; large dicts sent once
```

**Pickle savings:** The large dicts are now sent **6 times** (once per worker) instead of
**39,267 times** (once per building). Parent process no longer serializes megabyte-sized dicts
for every future; it only pickles small strings and Path objects.

## Results

Production profiling after both fixes (3dbag-surfaces v2026.1010 + this change):

| Metric | Before | After | Speedup |
|--------|--------|-------|---------|
| `asset_total_s` | 11,515.86s | 423.54s | **27.2×** |
| `processing_total_s` | 11,511.89s | 418.83s | **27.5×** |
| `shared_walls_total_s` | 4,291.96s | 2,311.90s | **1.86×** |
| `mean_building_total_s` | 0.111s | 0.0597s | **1.86×** |

**Breakdown:**
- The **1.86×** reduction in per-building shared_walls time comes from the library fix
  (PyVista `.cell_normals` hoisting).
- The **~7×** reduction in wall-clock parallelism overhead (11,511s → 419s processing) comes
  from eliminating the ProcessPoolExecutor pickle bottleneck.
- Combined: **27×** overall speedup. Execution time reduced from **3.2 hours to 7 minutes**.

## Time Breakdown After Fixes (Reference Baseline)

With both fixes in place (commit d8ce362, production run: 39,267 buildings, 6 workers), the
per-building time distribution shows where `building_surfaces` spends its wall-clock time:

| Stage | Cumulative (s) | % of per-building time |
|-------|---|---|
| `shared_walls()` | 2,311.90 | **98.6%** |
| Write output | 16.93 | 0.72% |
| Load adjacent features | 8.00 | 0.34% |
| Load target feature | 7.15 | 0.30% |
| **Total per-building** | **2,343.98** | 100% |

Additional context:
- **Adjacency query** (one-time cost): 0.50s
- **Processing wall-clock** (`processing_total_s`): 418.83s (with 6 workers; ~7% executor overhead)
- **Mean time per building**: 0.0597s

### Key Observations

1. **`shared_walls()` dominates at 98.6%** — Future optimization of `building_surfaces` throughput
   must come from improving the `3dbag-surfaces` library, not from pipeline I/O, concurrency, or
   database layers. The library's geometric computation is the critical path.

2. **File I/O and database queries are negligible** — Combined load + query time = 0.34 + 0.30 +
   0.50 (adjacency) = 1.14s total; even if optimized to zero, the asset would only save 0.27%
   of time. No value in pursuing these as optimization targets.

3. **Executor overhead is acceptable** — The ~7% wall-clock overhead (2,344s cumulative / 6 workers
   ≈ 391s ideal vs. 419s actual) is reasonable and was specifically reduced by the initializer
   pattern (was 16× before the fix).

This breakdown serves as a reference for evaluating future optimization proposals.

## Pattern Recognition

This ADR captures two anti-patterns to avoid:

### Pattern 1: PyVista Computed Properties in Loops

**Anti-pattern:**
```python
for i in range(mesh.n_cells):
    normal = mesh.cell_normals[i]  # Recomputes normals for entire mesh every iteration
```

**Correct pattern:**
```python
normals = mesh.cell_normals  # Compute once
for i in range(mesh.n_cells):
    normal = normals[i]  # Index into cached result
```

This applies to any property that triggers expensive computation: `.cell_normals`, `.face_normals`,
`.cell_centers`, etc.

### Pattern 2: ProcessPoolExecutor with Large Shared Data

**Anti-pattern:**
```python
with ProcessPoolExecutor(max_workers=N) as executor:
    for item in items:
        executor.submit(
            worker_func,
            item,
            large_shared_dict_1,  # pickled once per item = N × M times
            large_shared_dict_2,
        )
```

**Correct pattern:**
```python
def init_worker(shared_dict_1, shared_dict_2):
    global _shared_1, _shared_2
    _shared_1, _shared_2 = shared_dict_1, shared_dict_2

with ProcessPoolExecutor(max_workers=N, initializer=init_worker,
                         initargs=(large_shared_dict_1, large_shared_dict_2)) as executor:
    for item in items:
        executor.submit(worker_func, item)  # Only pickles item, accesses globals
```

When all workers need the same large read-only data, send it via `initializer`/`initargs`
(once per worker) not as function arguments (once per work item).

## Related Files

| File | Role |
|------|------|
| `packages/party_walls/src/bag3d/party_walls/assets/party_walls.py` | `_init_worker`, `_process_building`, `ProcessPoolExecutor` setup |
| `packages/party_walls/tests/test_integration.py` | Integration test; profiling output used for timing validation |
| `3dbag-surfaces` package (v2026.1010) | Upstream library fixes (`geometry.py` `.cell_normals` hoisting) |

## Consequences

- **Performance improvement** — Wall-clock execution time for full 39,267-building processing
  reduced from 3.2 hours to 7 minutes. All downstream workflows benefit.
- **Maintainability** — The initializer pattern is standard Python concurrent.futures idiom;
  no unusual dependencies or workarounds introduced.
- **Worker-local state** — Module-level globals in `party_walls.py` are set per-worker by the
  initializer, ensuring no inter-worker state sharing or race conditions.
