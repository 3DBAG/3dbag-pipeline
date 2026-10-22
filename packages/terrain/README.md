# Netherlands terrain

`bag3d-terrain` generates the Netherlands terrain used by the 3DBAG viewer:
AHN5 + AHN6 **5 m DTM**, neighbor-aware nodata interpolation over **500 ground
meters**, ellipsoidal GLO-30 fallback for larger water gaps, and **512 px
Terrarium lossless WebP XYZ tiles at zooms 0–14**, accompanied by TileJSON.
PMTiles bundles are retained as intermediate outputs.

The workflow has its own environment and code location. It needs no building
reconstruction, point clouds, database, running cache downloader, sibling
Mapterhorn checkout, or serving infrastructure.

## Install and run locally

Use Python 3.12 or later and `uv`. Install GDAL command-line tools with the COG
driver and LERC codec (`gdalbuildvrt`, `gdalwarp`, `gdal_translate`). Rasterio and
PyProj wheels supply their own Python-side native libraries. On NixOS those
wheels also need the compiler runtime, zlib and Expat available through the
development shell's `LD_LIBRARY_PATH`.

From this directory:

```bash
uv sync --frozen
uv run dg check defs
uv run dg list defs --json
```

Create a persistent local Dagster instance and an absolute file-store directory.
Keep the same instance and file store for acquisition and processing so Dagster
can reload the upstream manifest paths through its filesystem IO manager:

```bash
mkdir -p /tmp/terrain-dagster
export DAGSTER_HOME=/tmp/terrain-dagster
```

Create a run configuration, for example `/tmp/terrain-run.yaml`:

```yaml
resources:
  file_store:
    config:
      root_dir: /absolute/path/to/terrain-data
  terrain:
    config:
      # Omit bounds for the complete Netherlands dataset.
      bounds: [4.35, 52.0, 4.38, 52.025]
      interpolation_radius_m: 500
      workers: 4
      base_url: http://localhost:8001
      # Optional if GDAL is not on PATH:
      # gdal_bin_dir: /absolute/path/to/gdal/bin
```

Start with a small bounding box, then omit `bounds` for a national run:

```bash
uv run dg launch --job terrain_download --config-file /tmp/terrain-run.yaml
uv run dg launch --job terrain_process --config-file /tmp/terrain-run.yaml
# Or run acquisition and processing together:
uv run dg launch --job terrain --config-file /tmp/terrain-run.yaml
```

The launchpad in `uv run dg dev` exposes the same jobs and settings. The shared
local workspace also includes `bag3d.terrain.code_location`. `BAG3D_FILESTORE`
can supply the file-store root instead of run configuration.

The final materialization reports a manifest under
`stages/terrain/builds/<content-id>/terrain_xyz/manifest.json`. The neighboring
`xyz/` directory contains `tilejson.json` and `{z}/{x}/{y}.webp`. The base URL is
metadata for the eventual HTTP server; this package does not start or configure
a server. The viewer should use Terrarium encoding and the generated TileJSON.

## Asset flow

All keys have the `terrain/` prefix:

```text
ahn_catalog -> ahn_download -> ahn_rasters -> ahn_filled -> ahn_ellipsoid --+
                  |                                     ^              |
                  +-> glo30_download -> glo30_ellipsoid -|--------------+
                                               ^       |              |
proj_grids_download ----------------------------+-------+              |
                                                                      v
aggregation_plan -> terrain_tiles -> terrain_overviews -> terrain_bundles
                                                               |
                                                               v
                                                          terrain_xyz
```

The four acquisition assets are the only network users. They save paginated AHN
metadata, raw AHN rasters, regional GLO-30 rasters and these mandatory grids:

- `nl_nsgi_rdtrans2018.tif`
- `nl_nsgi_nlgeo2018.tif`
- `us_nga_egm08_25.tif`

Bounding-box selection includes neighboring AHN inputs for interpolation and
source transitions. The output extent is the requested box, or the AHN source
bounds for national processing; it never expands to the Copernicus footprint.

Downloads are validated, content-addressed and revalidated using HTTP ETag or
Last-Modified headers where provided. Partial downloads are never published.
The bundled regional GLO-30 inventory is a snapshot of Mapterhorn's source
catalog for `[0,49,10,56]`. Omitted entries in this region identify ocean cells;
network errors are failures, never evidence of ocean. Update the inventory and
its provenance deliberately when the source catalog changes.

Processing preserves the local Mapterhorn behavior:

- Keep valid AHN measurements during interpolation, within the 1 mm COG
  compression tolerance. Remaining gaps use GLO-30.
- Convert NAP using RDNAPTRANS2018/NLGEO2018 on the native RD grid. Record the
  ellipsoidal band reference explicitly; assigning a horizontal CRS is not a
  height conversion. Reject already-converted AHN input.
- Convert GLO-30 EGM2008 heights, including zero-EGM2008 ocean cells, before
  merging. Both inputs must be ellipsoidal. The AHN conversion follows the
  existing ETRS89/WGS84 ensemble approximation, without epoch adjustment.
- Mosaic before cubic-spline reprojection onto aligned Web Mercator grids;
  prefer AHN and blend transitions to the fallback.
- Preserve Mapterhorn's zoom-dependent elevation rounding, 2×2 averaging of
  decoded heights, and decoded-height interpolation for missing static levels.
  Do not interpolate encoded RGB channels or apply another viewer geoid offset.
- Fail on unresolved nodata inside the output domain instead of silently
  encoding ellipsoidal zero. Full XYZ cells can extend beyond the advertised box.

## Storage, checkpoints and concurrency

Downloads live under `stages/terrain/downloads/`. Generated stages have separate
content-based directories under `stages/terrain/builds/`. Assets exchange small
JSON manifest paths, not arrays. Manifests retain content hashes, source URLs,
spatial bounds and height references. Each stage reports counts, bytes and
relevant coverage metadata to Dagster.

Input hashes, settings, processing code and native library versions determine
stage identity. A failed work unit resumes from its last verified checkpoint;
missing or changed outputs are rebuilt. Source, radius or code changes produce
separate stages, avoiding contamination from earlier outputs. Changing the
worker count does not invalidate calculations; changing the base URL rewrites
TileJSON while retaining identical terrain tiles.

Pools default to four workers and GDAL uses one processing thread per worker.
Aggregation areas follow Mapterhorn's maximum 64×64 terrain-tile dimensions;
allow several GiB per worker and put intermediate rasters on SSD. Stage-level
assets have local work-unit checkpoints rather than one Dagster partition per
source tile. Concurrent writes to the same checkpoint/export fail promptly on
file locks. Old completed builds are retained; remove them explicitly when no
longer needed.

## Validation

```bash
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run pyright
uv run dg check defs
```

An opt-in acceptance test runs all thirteen assets using small synthetic AHN
and GLO rasters, real local transformation grids, and GDAL. It then reruns the
processing job with requests blocked, verifies water fallback and the complete
XYZ coverage, and checks that changing the URL leaves tile files untouched:

```bash
TERRAIN_TEST_GRID_DIR=/absolute/path/to/grids uv run pytest -m integration
```

For national acceptance, run acquisition and processing without `bounds`.
Compare decoded heights at IJsselmeer `(5.5,52.7)`, Markermeer `(5.2,52.5)` and
Haringvliet `(4.2,51.77)` against the previous terrain generated from the same
inputs. The original local rebuild recorded approximately 42.625, 42.875 and
44.375 ellipsoidal meters at zoom 14. These are historical inspection samples,
not fixed truths for changing AHN inputs. Inspect representative source
boundaries and water transitions in the viewer, and record runtime and disk use.

## Imported code and maintenance

Selected Mapterhorn code lives in `_mapterhorn/`, with its BSD-3-Clause license.
`MAPTERHORN_PROVENANCE.json` records the baseline commit, hashes of the original
files, selected functions and local uncommitted inputs. It also covers scripts
used as references when replacing the filesystem queue and CLI orchestration.

The internal terrain engine replaces those wrappers with explicit paths, local
file access, content checkpoints, public PMTiles writer APIs and Dagster assets.
It carries no runtime Mapterhorn dependency. Review and port upstream fixes
explicitly; keep algorithm changes separate from orchestration changes.

AHN and Copernicus data license notices are shipped in `data/`; the generated
TileJSON and archives retain their source attribution. New workflow code uses
the repository's Apache-2.0 / MIT licensing. Hosting, publishing, schedules and
additional source resolutions are outside this package's current scope.
