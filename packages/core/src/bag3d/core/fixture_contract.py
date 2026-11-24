"""Versioned integration snapshot geometry and processing contract."""

import math

FIXTURE_VERSION = "3"
VECTOR_PROCESSING = {
    "bag": "native_subset",
    "bgt": "pdok_aoi_whole_objects",
    "top10nl": "pdok_aoi_whole_objects",
    "cbs_buurtkaart": "gdal",
}


def validate_dataset_extents(
    extents: list[tuple[float, ...]], aoi: tuple[float, ...], label: str
) -> None:
    """Validate each bound and require the combined dataset extent to overlap AOI."""
    if not extents:
        raise ValueError(f"Missing dataset extent for {label}")
    for extent in extents:
        if (
            len(extent) != 4
            or not all(math.isfinite(value) for value in extent)
            or extent[0] > extent[2]
            or extent[1] > extent[3]
        ):
            raise ValueError(f"Invalid computed extent for {label}: {extent}")
    combined = (
        min(extent[0] for extent in extents),
        min(extent[1] for extent in extents),
        max(extent[2] for extent in extents),
        max(extent[3] for extent in extents),
    )
    if max(combined[0], aoi[0]) > min(combined[2], aoi[2]) or max(
        combined[1], aoi[1]
    ) > min(combined[3], aoi[3]):
        raise ValueError(f"Computed extent for {label} does not overlap the AOI")
