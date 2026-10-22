# Derived from Mapterhorn, Copyright (c) 2025, mapterhorn. BSD-3-Clause.
# See LICENSE and the package MAPTERHORN_PROVENANCE.json.
import numpy as np
import imagecodecs


def get_rounded_elevation_data(data, z):
    # full terrarium resolution of 1/256 at `full_resolution_zoom`
    # multiples of 2 of full terrarium resolution at lower zooms
    full_resolution_zoom = 19
    factor = 2 ** (full_resolution_zoom - z) / 256
    if factor > 1:
        factor = 1
    return np.round(data / factor) * factor


def save_terrarium_tile(data, filepath):
    filename = filepath.split("/")[-1]
    z = int(filename.split("-")[0])

    data = get_rounded_elevation_data(data, z)

    data += 32768
    rgb = np.zeros((512, 512, 3), dtype=np.uint8)
    np.seterr(all="raise", under="ignore")
    try:
        rgb[..., 0] = data // 256
        rgb[..., 1] = data % 256
        rgb[..., 2] = (data - np.floor(data)) * 256
    except FloatingPointError:
        print(f"FloatingPointError raised in {filepath}")
        raise FloatingPointError()
    with open(filepath, "wb") as f:
        f.write(imagecodecs.webp_encode(rgb, lossless=True))
