# Derived from Mapterhorn, Copyright (c) 2025, mapterhorn. BSD-3-Clause.
# See LICENSE and the package MAPTERHORN_PROVENANCE.json.
import mercantile

from . import constants as utils


def get_mercator_resolutions(minzoom, maxzoom):
    resolutions = []
    for z in range(minzoom, maxzoom + 1):
        tile = mercantile.Tile(x=0, y=0, z=z)
        bounds = mercantile.xy_bounds(tile)
        resolutions.append((bounds.right - bounds.left) / 512)
    return resolutions


def bounds_intersect_no_anitmeridian_crossing(a, b):
    left_a, bottom_a, right_a, top_a = a
    left_b, bottom_b, right_b, top_b = b
    dont_intersect = False
    dont_intersect |= right_a <= left_b
    dont_intersect |= right_b <= left_a
    dont_intersect |= top_a <= bottom_b
    dont_intersect |= top_b <= bottom_a
    return not dont_intersect


def split_at_antimeridian(bbox):
    left, bottom, right, top = bbox
    if left < right:
        return [bbox]
    bbox_1 = (left, bottom, utils.X_MAX_3857, top)
    bbox_2 = (utils.X_MIN_3857, bottom, right, top)
    return [bbox_1, bbox_2]


def bounds_intersect(a, b):
    for aa in split_at_antimeridian(a):
        for bb in split_at_antimeridian(b):
            if bounds_intersect_no_anitmeridian_crossing(aa, bb):
                return True
    return False


def get_intersecting_tiles_dfs(bounds, tile, zoom):
    tile_bounds = mercantile.xy_bounds(tile)
    if not bounds_intersect(bounds, tile_bounds):
        return []
    if tile.z == zoom:
        return [tile]
    result = []
    for child in mercantile.children(tile, zoom=tile.z + 1):
        result += get_intersecting_tiles_dfs(bounds, child, zoom)
    return result


def get_smallest_overzoom(
    left, bottom, right, top, width, height, mercator_resolutions
):
    horizontal_resolution = (
        (right - left) / width if left < right else (left - right) / width
    )
    vertical_resolution = (top - bottom) / height

    for z in range(len(mercator_resolutions)):
        if (
            mercator_resolutions[z] < horizontal_resolution
            and mercator_resolutions[z] < vertical_resolution
        ):
            return z
    raise ValueError(
        f"No overzoom found. (left, bottom, right, top, width, height) = {(left, bottom, right, top, width, height)}"
    )


def add_group_ids(macrotile_map):
    for tile_tuple in macrotile_map:
        group_id_parts = set({})
        for source in macrotile_map[tile_tuple]["sources"]:
            for source_item in macrotile_map[tile_tuple]["sources"][source]:
                group_id_parts.add((source, source_item["maxzoom"]))
        group_id = tuple(sorted(list(group_id_parts)))
        macrotile_map[tile_tuple]["group_id"] = group_id


def get_aggregation_tiles_dfs(candidate, macrotile_map):
    if candidate.z == utils.macrotile_z:
        return [candidate]
    macrotiles = list(mercantile.children(candidate, zoom=utils.macrotile_z))
    group_ids = set({})
    for macrotile in macrotiles:
        tile_tuple = (macrotile.x, macrotile.y)
        if tile_tuple in macrotile_map:
            group_ids.add(macrotile_map[tile_tuple]["group_id"])
    if len(group_ids) == 0:
        return []
    if len(group_ids) == 1:
        group_id = list(group_ids)[0]
        maxzoom = 0
        for part in group_id:
            maxzoom = max(maxzoom, part[1])
        if candidate.z >= maxzoom - utils.num_overviews:
            return [candidate]
    result = []
    for child in mercantile.children(candidate, zoom=candidate.z + 1):
        result += get_aggregation_tiles_dfs(child, macrotile_map)
    return result


def get_aggregation_tiles(macrotile_map):
    candidates = set({})
    for tile_tuple in macrotile_map.keys():
        candidates.add(
            mercantile.parent(
                mercantile.Tile(x=tile_tuple[0], y=tile_tuple[1], z=utils.macrotile_z),
                zoom=utils.macrotile_z - utils.num_overviews,
            )
        )
    aggregation_tiles = []
    for candidate in candidates:
        aggregation_tiles += get_aggregation_tiles_dfs(candidate, macrotile_map)
    return aggregation_tiles
