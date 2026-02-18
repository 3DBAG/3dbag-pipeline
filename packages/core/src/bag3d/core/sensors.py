import json

from dagster import (
    AssetKey,
    DefaultSensorStatus,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SensorDefinition,
    SkipReason,
    multi_asset_sensor,
)

from bag3d.core.assets.ahn.core import download_ahn_index
from bag3d.core.assets.ahn.download import URL_LAZ_SHA, get_checksums
from bag3d.core.jobs import job_ahn3, job_ahn4, job_ahn5

# Maps AHN version to: job name, checksum asset key, tile_index URL key
_AHN_VERSIONS = {
    3: {
        "job_name": "ahn3",
        "asset_key": AssetKey(["ahn", "md5_ahn3"]),
        "url_key": "AHN3_LAZ",
    },
    4: {
        "job_name": "ahn4",
        "asset_key": AssetKey(["ahn", "md5_ahn4"]),
        "url_key": "AHN4_LAZ",
    },
    5: {
        "job_name": "ahn5",
        "asset_key": AssetKey(["ahn", "sha256_ahn5"]),
        "url_key": "AHN5_LAZ",
    },
}


def _build_filename_to_tile_id(tile_index: dict, url_key: str) -> dict[str, str]:
    """Build {filename: tile_id} mapping from tile_index for a specific AHN version.

    Each AHN version may have a different filename convention in its download URLs.
    The tile_index maps tile_id -> {"AHN3_LAZ": url, ...}, and the filename is
    the last path component of the URL.
    """
    mapping = {}
    for tile_id, info in tile_index.items():
        url = info.get(url_key)
        if url:
            filename = url.split("/")[-1]
            mapping[filename] = tile_id
    return mapping


def ahn_checksum_sensor(default_status: DefaultSensorStatus) -> SensorDefinition:
    """Factory that returns the AHN checksum sensor with the given default status.

    The sensor watches for new materializations of the checksum assets (md5_ahn3,
    md5_ahn4, sha256_ahn5). When triggered, it downloads the tile index to build
    a filename→tile_id mapping, reads the checksums, compares against previously
    stored checksums in the cursor, and triggers partition runs only for tiles
    whose checksum has changed.

    On first run (no cursor), it establishes a baseline without triggering any runs.
    """

    @multi_asset_sensor(
        monitored_assets=[
            AssetKey(["ahn", "md5_ahn3"]),
            AssetKey(["ahn", "md5_ahn4"]),
            AssetKey(["ahn", "sha256_ahn5"]),
        ],
        jobs=[job_ahn3, job_ahn4, job_ahn5],
        default_status=default_status,
        name="ahn_checksum_sensor",
    )
    def _sensor(context: MultiAssetSensorEvaluationContext):
        """Detect AHN LAZ file changes by comparing checksums against stored state."""
        # Check which checksum assets have new materializations
        events = context.latest_materialization_records_by_key()
        updated_versions = [
            version
            for version, cfg in _AHN_VERSIONS.items()
            if events.get(cfg["asset_key"]) is not None
        ]

        if not updated_versions:
            return SkipReason("No new checksum materializations")

        # Load tile index for filename→tile_id mapping (no geometry needed)
        tile_index = download_ahn_index(with_geom=False)

        previous = json.loads(context.cursor) if context.cursor else {}
        current = dict(previous)  # preserve checksums for un-updated versions
        run_requests = []

        for version in updated_versions:
            cfg = _AHN_VERSIONS[version]
            key = f"ahn{version}"

            try:
                checksums = get_checksums(URL_LAZ_SHA, ahn_version=version)
            except Exception:
                context.log.warning(f"Failed to read checksums for AHN{version}")
                continue

            filename_to_tile = _build_filename_to_tile_id(tile_index, cfg["url_key"])
            current[key] = checksums
            prev_checksums = previous.get(key)

            if prev_checksums is None:
                # First run for this version — establish baseline, don't trigger
                context.log.info(
                    f"AHN{version}: baseline established ({len(checksums)} tiles)"
                )
                continue

            # Find tiles whose checksum changed (new or updated)
            for filename, new_hash in checksums.items():
                old_hash = prev_checksums.get(filename)
                if old_hash != new_hash:
                    tile_id = filename_to_tile.get(filename)
                    if tile_id is None:
                        context.log.warning(
                            f"AHN{version}: no tile_id mapping for {filename}"
                        )
                        continue
                    run_requests.append(
                        RunRequest(
                            run_key=f"ahn{version}-{tile_id}-{new_hash[:8]}",
                            job_name=cfg["job_name"],
                            partition_key=tile_id,
                        )
                    )

        context.advance_all_cursors()
        context.update_cursor(json.dumps(current))

        if not run_requests:
            return SkipReason("No checksum changes detected")
        context.log.info(f"Triggering {len(run_requests)} partition updates")
        return run_requests

    return _sensor
