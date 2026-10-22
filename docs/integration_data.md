# Integration data

The `integration_data` Dagster job builds a small, reproducible snapshot of the
source datasets used by integration tests and by the optional fixture-backed
pipeline mode. It downloads source data, clips it to the integration area, checks
the results, and writes a manifest. It does not load the snapshot into the
pipeline database.

## What the job does

The job runs the source assets it needs and then creates these fixture outputs:

| Source | Fixture operation | Output |
| --- | --- | --- |
| BAG | Filters the nested BAG extract to buildings in the area (with a 10 m buffer). | `bag/` |
| BGT | Clips the `bgt_pand.gml` layer. | `bgt/bgt.zip` |
| TOP10NL | Clips the `top10nl_gebouw.gml` layer. | `top10nl/top10nl.zip` |
| CBS key figures and buurtkaart | Clips the buurtkaart and keeps key-figure records for the resulting neighbourhoods. | `cbs/buurtkaart.gpkg`, `cbs/key_figures.json` |
| AHN3, AHN4, AHN5, AHN6 | Finds tiles intersecting the area, downloads them, and crops their point data to the area. AHN6 is also restricted to a tile allowlist. | `pointclouds/AHN3/` through `pointclouds/AHN6/` |

Before writing the manifest, the job checks the vector and point-cloud extents.
They must fit within the area plus 10 m. `manifest.json` records the area,
source metadata, AHN indexes/checksums/partitions, output file sizes and SHA-256
hashes, and the validated extents. Fixture adapters verify this manifest when
the snapshot is consumed.

## Inputs and requirements

The job uses the latest source extracts available when it runs:

- BAG from the LVBAG extract service;
- BGT and TOP10NL source extracts;
- CBS key figures and the CBS Wijken- en Buurtenkaart;
- AHN tile indexes and checksum metadata for AHN3–AHN6.

The run therefore needs network access to the source services. It also needs
GDAL (`ogr2ogr`, `ogrinfo`) for vector clipping and validation, and LAStools
(`las2las`, `lasinfo`) for point-cloud cropping and validation. These tools are
included in the pipeline tools image.

## Run the job

In the Dagster UI, open **Jobs → `integration_data`** and launch a run. The job
includes its source assets, so a separate source-data job is not required. The
standard integration area is the default; no asset config is needed for that
run.

For a local development instance, run `make local_dev` and open
`http://localhost:3000`. Configure the resources requested by the run in the
Dagster launchpad, or use a deployment where those resources are configured
from its environment.

The job is also available as the `integration_data_monthly` schedule. It runs at
02:00 on the first of each month in the `Europe/Amsterdam` timezone, but is
stopped by default. Enable it in the Dagster UI if the snapshot should refresh
automatically.

## Configure the snapshot

The fixture root is the `integration_data_store` resource. In deployments that
read configuration from environment variables, set `BAG3D_INTEGRATION_DATA_DIR`
to the directory where the snapshot should be written. If unset, it defaults to
`$BAG3D_FILESTORE/integration-data`. Ensure the directory is writable by the
Dagster run worker.

The default area is the WKT polygon defined as `AOI_WKT` in
`packages/core/src/bag3d/core/assets/integration_data.py` (EPSG:28992
coordinates). The integration assets expose a `geofilter` config field and AHN6
exposes `ahn6_allowlist`. However, the current manifest validation records and
validates against the built-in `AOI_WKT`, so changing the area is not a
supported end-to-end configuration. The monthly schedule also explicitly
passes this default area to the BGT and TOP10NL extract assets. Keep the default
area. The AHN6 allowlist is configured on the `integration_ahn6` asset and
limits which intersecting AHN6 tiles are downloaded; its default is
`121000_485000`, `121000_486000`, `122000_485000`, `122000_486000`,
`123000_485000`, and `123000_486000`.

## Output and using it with Docker

The directory configured above contains `manifest.json` and the fixture
directories listed above. Each successful job run also publishes an immutable
snapshot under `https://data.3dbag.nl/integration-data/<snapshot_id>/`. The
`snapshot_id` combines the UTC run date and Dagster run ID. Each published
directory contains `integration-data.zip` and a standalone `manifest.json`.

To use a published snapshot, select its `snapshot_id` and extract the archive
into the host directory configured for the read-only fixture mount:

```sh
snapshot_id='YYYY-MM-DD-DAGSTER_RUN_ID'
host_dir="$PWD/integration-data"
mkdir -p "$host_dir"
curl -fL "https://data.3dbag.nl/integration-data/$snapshot_id/integration-data.zip" \
  -o /tmp/integration-data.zip
unzip -o /tmp/integration-data.zip -d "$host_dir"
```

The extracted directory has `manifest.json` at its root. Point
`BAG3D_INTEGRATION_DATA_HOST_DIR` at that directory and start Compose:

To run the pipeline in fixture-backed mode, set
`BAG3D_INTEGRATION_DATA_HOST_DIR` to the extracted snapshot directory and start
Compose with the integration-data overlay:

```sh
export BAG3D_INTEGRATION_DATA_HOST_DIR=/path/to/integration-data
docker compose \
  -f docker/compose.yaml \
  -f docker/compose.integration-data.yaml up -d
```

The overlay mounts the snapshot read-only at `/data/volume/integration-data`
and sets `BAG3D_INPUT_MODE=integration_data`. See [Docker](../docker/README.md)
for the rest of the Compose setup.
