# Streetlidar datasets

Streetlidar point clouds supply underpass ceiling measurements. Dagster discovers
manually supplied datasets through a root manifest and publishes their tile
footprints in the computation database, in `streetlidar.tile_index`. The index
contains only the datasets listed in the last successful refresh.

## Add or retire a dataset

On Gilfoyle, place complete datasets under `/data/streetlidar`, for example:

```text
/data/streetlidar/
  manifest.json
  amsterdam/2025/
    georizon-dataset-.../
      110100_490400.laz
  other-city/2026/
    tile.laz
```

Create or edit `manifest.json` after copying finishes:

```json
{
  "version": 1,
  "crs": "EPSG:7415",
  "datasets": [
    {"id": "amsterdam/2025", "path": "amsterdam/2025"},
    {"id": "other-city/2026", "path": "other-city/2026"}
  ]
}
```

Each entry contributes all LAS/LAZ files recursively, including uppercase
extensions. IDs must be unique. Paths must stay within the root, and the same
physical file cannot belong to multiple entries. Unlisted directories are
ignored. Remove an entry to retire its tiles; this does not delete data. Keep
retired files until calculations already using them have finished.

`EPSG:7415` specifies RD coordinates and NAP heights in metres. Horizontal-only
RD headers and headers without CRS metadata use the manifest's declaration for
missing components. Conflicting horizontal or vertical metadata fails discovery.
Tile footprints use the horizontal CRS, EPSG:28992. No reprojection is performed.

Materialize `streetlidar/tile_index` in the core code location, or launch its
`streetlidar_index` job. Its metadata reports dataset counts, added/changed/removed
tiles, manifest SHA-256, and catalog fingerprint. New entries need no code change
or code-location reload. All intersecting active datasets contribute to height
estimation; retire an old version explicitly if it should stop contributing.

Refreshes reuse header metadata for unchanged paths, file sizes, and modification
times. Set the asset's `force` config to `true` to reread all headers. Discovery
uses header metadata rather than decompressing or hashing entire production
point clouds; payload errors still fail when the data is read for calculations.

A missing directory, empty dataset, invalid header, or source change during the
refresh fails the whole run. PostGIS updates are transactional and concurrent
refresh writers are serialized, preserving the previous catalog on failure.
Readers take a consistent catalog snapshot. `streetlidar.catalog_state` stores
the manifest, effective root, coordinate declaration, and catalog fingerprint.

## Configure live inputs

Production configures the source alongside the AHN pointcloud directory:

```sh
BAG3D_POINTCLOUD_DIR=/data/pointcloud
BAG3D_STREETLIDAR_ROOT=/data/streetlidar
```

The production environment files in the admin and `3dbag-ctl` repositories set
`BAG3D_STREETLIDAR_ROOT`. The existing `/data:/data` mount exposes the directory
to core and underpasses at the same path. Streetlidar uses ordinary deployment
storage configuration; no dedicated overlay or b3d source-selection logic is
needed. The discovery asset reads the inputs and writes its index to PostGIS.

For development or direct Compose runs using only the shared `/data/volume`
Docker volume, place the source in that volume and set
`BAG3D_STREETLIDAR_ROOT` to its container path, or configure an ordinary bind mount
in the deployment configuration. For execution outside Docker, set the variable
directly or supply the index asset's `root` config. The effective root must be
readable at the same path by core and underpasses.

`source_input` includes the catalog refresh, so clean complete pipeline runs
prepare the input before underpasses. `underpasses/heights` consumes the managed
index and records its fingerprint alongside terrain provenance. Direct height
asset settings `index`, `pointcloud_root`, and `index_layer`, and
`BAG3D_STREETLIDAR_INDEX`, have been removed. Worker and estimation settings remain
configurable. After a manual index refresh, materialize heights and downstream
outputs when you want to use the new data.

Leaving the source unconfigured, or publishing an explicit empty manifest,
produces an empty catalog and the existing heuristic fallback. A configured
missing source is an error.

## Integration snapshots

Generate snapshots in a live-input deployment, with `BAG3D_INPUT_MODE` unset.
The `integration_data` job refreshes the source index, then
`integration_streetlidar` selects and crops tiles to the integration AOI plus
10 m. It replaces its generated subtree after successful cropping, removing
stale tiles from earlier generations. Generate a replacement snapshot outside
any directory being consumed by running fixture deployments.

The output contains cropped LAS/LAZ files and `streetlidar/catalog.json`, which
embeds the dataset manifest, relative locations, cropped extents, and source
catalog fingerprint. The root fixture manifest references this JSON catalog as
`sources.streetlidar` and includes checksums and size/extent validation for the
files. This format is portable between host directories and deployments.

In fixture mode, core's replacement `streetlidar/tile_index` asset loads only
manifest-declared snapshot files into PostGIS. Underpasses receives a read-only
snapshot mount. Live streetlidar sources are never consulted. Fixture versions
2 and 3 without a streetlidar section remain supported and publish an empty
catalog; newly generated snapshots explicitly record an empty selection when
no streetlidar is available.

## Tests

Ordinary unit tests cover manifest discovery, CRS checks, fixtures, and Dagster
wiring. Optional database tests use a **disposable** database selected through
`BAG3D_STREETLIDAR_TEST_DSN`. They create and remove the `streetlidar` schema.
The full crop/height integration test lives in the underpasses package and needs
the repository checkout to exercise core's fixture producer, plus
`BAG3D_TEST_PDAL` / `BAG3D_TEST_LAS2LAS` (or those executables on PATH).
