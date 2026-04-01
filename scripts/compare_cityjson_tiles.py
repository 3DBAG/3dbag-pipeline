"""Compare two sets of .city.json.gz tile exports for differences in CityObjects,
attributes, and geometries. Writes a Markdown report to the specified output file.

Usage:
    python scripts/compare_cityjson_tiles.py <dir_a> <dir_b> [--output report.md] [--geom-tolerance 0.001]
"""

import argparse
import gzip
import json
import math
from collections import defaultdict
from pathlib import Path


def load_cityjson(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def decompress_vertices(vertices, transform):
    scale = transform["scale"]
    translate = transform["translate"]
    return [
        [v[0] * scale[0] + translate[0], v[1] * scale[1] + translate[1], v[2] * scale[2] + translate[2]]
        for v in vertices
    ]


def round_coord(coord, tolerance):
    factor = 1.0 / tolerance
    return (
        math.floor(coord[0] * factor + 0.5) / factor,
        math.floor(coord[1] * factor + 0.5) / factor,
        math.floor(coord[2] * factor + 0.5) / factor,
    )


def boundary_indices(boundaries):
    """Recursively collect all vertex indices from nested boundary arrays."""
    if not boundaries:
        return []
    if isinstance(boundaries[0], int):
        return list(boundaries)
    result = []
    for sub in boundaries:
        result.extend(boundary_indices(sub))
    return result


def ring_to_coord_tuple(ring, vertices, tolerance):
    """Convert a ring (list of vertex indices) to a frozenset of rounded coords."""
    return frozenset(round_coord(vertices[i], tolerance) for i in ring)


def surfaces_from_boundaries(boundaries, vertices, tolerance):
    """Convert MultiSurface / Solid boundaries to a frozenset of surface frozensets."""
    surfaces = set()

    def extract_surfaces(b, depth):
        if not b:
            return
        if depth == 0:
            # b is a ring (list of ints)
            surfaces.add(ring_to_coord_tuple(b, vertices, tolerance))
        else:
            for item in b:
                extract_surfaces(item, depth - 1)

    # CityJSON boundary nesting:
    # MultiSurface: [surface [ring [idx]]]  → depth 2 to reach ring
    # Solid: [shell [surface [ring [idx]]]] → depth 3
    if boundaries and isinstance(boundaries[0], list):
        if boundaries[0] and isinstance(boundaries[0][0], list):
            if boundaries[0][0] and isinstance(boundaries[0][0][0], list):
                # Solid (depth 3)
                for shell in boundaries:
                    for surface in shell:
                        for ring in surface:
                            surfaces.add(ring_to_coord_tuple(ring, vertices, tolerance))
            else:
                # MultiSurface (depth 2)
                for surface in boundaries:
                    for ring in surface:
                        surfaces.add(ring_to_coord_tuple(ring, vertices, tolerance))
        else:
            # Single surface (depth 1)
            for ring in boundaries:
                surfaces.add(ring_to_coord_tuple(ring, vertices, tolerance))

    return frozenset(surfaces)


def geometry_by_lod(geometry_list, vertices, tolerance):
    """Return dict {lod_str: frozenset_of_surfaces} for one CityObject."""
    result = {}
    for geom in geometry_list:
        lod = str(geom.get("lod", "?"))
        surfaces = surfaces_from_boundaries(geom.get("boundaries", []), vertices, tolerance)
        result[lod] = surfaces
    return result


def normalise(v):
    if isinstance(v, float):
        return round(v, 0)
    return v


def compare_objects(obj_a, obj_b, verts_a, verts_b, tolerance, ignore_attrs=frozenset()):
    """Compare two CityObjects. Returns a dict with keys:
    - attr_removed: list of attribute names
    - attr_added: list of attribute names
    - attr_changed: list of (name, val_a, val_b)
    - lod_removed: list of lod strings
    - lod_added: list of lod strings
    - geom_changed: list of (lod, surfaces_only_in_a_count, surfaces_only_in_b_count)
    """
    diffs = {}

    # Attribute comparison
    attrs_a = obj_a.get("attributes", {})
    attrs_b = obj_b.get("attributes", {})
    keys_a = set(attrs_a)
    keys_b = set(attrs_b)

    removed = sorted(keys_a - keys_b)
    added = sorted(keys_b - keys_a)
    changed = []
    for k in sorted(keys_a & keys_b):
        if k in ignore_attrs:
            continue
        va, vb = attrs_a[k], attrs_b[k]
        if k in ["b3_opp_buitenmuur","b3_opp_grond","b3_opp_scheidingsmuur","b3_opp_dak_schuin","b3_opp_dak_plat",]:
            if not math.isclose( float(va), float(vb), abs_tol=1):
                # print(va, vb)
                changed.append((k, va, vb))

    if removed:
        diffs["attr_removed"] = removed
    if added:
        diffs["attr_added"] = added
    if changed:
        diffs["attr_changed"] = changed

    # Geometry comparison
    # geom_a = geometry_by_lod(obj_a.get("geometry", []), verts_a, tolerance)
    # geom_b = geometry_by_lod(obj_b.get("geometry", []), verts_b, tolerance)
    lods_a = set(g.get("lod") for g in obj_a.get("geometry", []))
    lods_b = set(g.get("lod") for g in obj_b.get("geometry", []))

    lod_removed = sorted(lods_a - lods_b)
    lod_added = sorted(lods_b - lods_a)
    # geom_changed = []
    # for lod in sorted(lods_a & lods_b):
    #     only_in_a = geom_a[lod] - geom_b[lod]
    #     only_in_b = geom_b[lod] - geom_a[lod]
    #     if only_in_a or only_in_b:
    #         geom_changed.append((lod, len(only_in_a), len(only_in_b)))

    if lod_removed:
        diffs["lod_removed"] = lod_removed
    if lod_added:
        diffs["lod_added"] = lod_added
    # if geom_changed:
    #     diffs["geom_changed"] = geom_changed

    return diffs


def find_tiles(root):
    """Return dict {relative_path_str: absolute_path} for all .city.json.gz under root."""
    root = Path(root)
    return {
        str(p.relative_to(root)): p
        for p in root.rglob("*.city.json.gz")
    }


def compare_tile(path_a, path_b, tolerance, ignore_attrs=frozenset()):
    """Compare one tile pair. Returns dict with file-level diff info."""
    data_a = load_cityjson(path_a)
    data_b = load_cityjson(path_b)

    verts_a = decompress_vertices(data_a["vertices"], data_a["transform"])
    verts_b = decompress_vertices(data_b["vertices"], data_b["transform"])

    objects_a = data_a.get("CityObjects", {})
    objects_b = data_b.get("CityObjects", {})

    ids_a = set(objects_a)
    ids_b = set(objects_b)

    missing_from_b = sorted(ids_a - ids_b)
    missing_from_a = sorted(ids_b - ids_a)

    object_diffs = {}
    for obj_id in sorted(ids_a & ids_b):
        d = compare_objects(objects_a[obj_id], objects_b[obj_id], verts_a, verts_b, tolerance, ignore_attrs=ignore_attrs)
        if d:
            object_diffs[obj_id] = d

    return {
        "objects_a": len(ids_a),
        "objects_b": len(ids_b),
        "missing_from_b": missing_from_b,
        "missing_from_a": missing_from_a,
        "object_diffs": object_diffs,
    }


def format_report(dir_a, dir_b, tiles_only_a, tiles_only_b, tile_results):
    lines = []
    lines.append("# CityJSON Tile Comparison Report\n")
    lines.append(f"**Dir A (baseline):** `{dir_a}`\n")
    lines.append(f"**Dir B (target):** `{dir_b}`\n")
    lines.append("")

    # Summary
    total_tiles = len(tile_results)
    total_objs_a = sum(r["objects_a"] for r in tile_results.values())
    total_objs_b = sum(r["objects_b"] for r in tile_results.values())
    total_missing_b = sum(len(r["missing_from_b"]) for r in tile_results.values())
    total_missing_a = sum(len(r["missing_from_a"]) for r in tile_results.values())
    total_obj_diffs = sum(len(r["object_diffs"]) for r in tile_results.values())

    # Aggregate attribute diff counts
    attr_removed_counts = defaultdict(int)
    attr_added_counts = defaultdict(int)
    attr_changed_counts = defaultdict(int)
    geom_diff_count = 0

    for r in tile_results.values():
        for obj_diffs in r["object_diffs"].values():
            for k in obj_diffs.get("attr_removed", []):
                attr_removed_counts[k] += 1
            for k in obj_diffs.get("attr_added", []):
                attr_added_counts[k] += 1
            for k, _, _ in obj_diffs.get("attr_changed", []):
                attr_changed_counts[k] += 1
            if obj_diffs.get("lod_removed") or obj_diffs.get("lod_added") or obj_diffs.get("geom_changed"):
                geom_diff_count += 1

    lines.append("## Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Tiles compared | {total_tiles} |")
    lines.append(f"| Tiles only in A | {len(tiles_only_a)} |")
    lines.append(f"| Tiles only in B | {len(tiles_only_b)} |")
    lines.append(f"| CityObjects in A (matched tiles) | {total_objs_a} |")
    lines.append(f"| CityObjects in B (matched tiles) | {total_objs_b} |")
    lines.append(f"| Objects missing from B | {total_missing_b} |")
    lines.append(f"| Objects missing from A | {total_missing_a} |")
    lines.append(f"| Objects with any diff | {total_obj_diffs} |")
    lines.append(f"| Objects with geometry diff | {geom_diff_count} |")
    lines.append("")

    if tiles_only_a or tiles_only_b:
        lines.append("## Files Present in Only One Directory\n")
        for t in sorted(tiles_only_a):
            lines.append(f"- Only in A: `{t}`")
        for t in sorted(tiles_only_b):
            lines.append(f"- Only in B: `{t}`")
        lines.append("")

    if attr_removed_counts or attr_added_counts or attr_changed_counts:
        lines.append("## Attribute Diff Summary (across all objects)\n")
        if attr_removed_counts:
            lines.append("### Attributes removed in B\n")
            lines.append("| Attribute | # Objects affected |")
            lines.append("|-----------|-------------------|")
            for k, cnt in sorted(attr_removed_counts.items(), key=lambda x: -x[1]):
                lines.append(f"| `{k}` | {cnt} |")
            lines.append("")
        if attr_added_counts:
            lines.append("### Attributes added in B\n")
            lines.append("| Attribute | # Objects affected |")
            lines.append("|-----------|-------------------|")
            for k, cnt in sorted(attr_added_counts.items(), key=lambda x: -x[1]):
                lines.append(f"| `{k}` | {cnt} |")
            lines.append("")
        if attr_changed_counts:
            lines.append("### Attributes with changed values\n")
            lines.append("| Attribute | # Objects affected |")
            lines.append("|-----------|-------------------|")
            for k, cnt in sorted(attr_changed_counts.items(), key=lambda x: -x[1]):
                lines.append(f"| `{k}` | {cnt} |")
            lines.append("")

    lines.append("## Per-Tile Details\n")

    for tile_path, r in sorted(tile_results.items()):
        has_diffs = (
            r["missing_from_b"]
            or r["missing_from_a"]
            or r["object_diffs"]
        )
        status = "DIFF" if has_diffs else "OK"
        lines.append(f"### `{tile_path}` — {status}\n")
        lines.append(f"Objects: A={r['objects_a']}, B={r['objects_b']}\n")

        if r["missing_from_b"]:
            lines.append(f"**Missing from B ({len(r['missing_from_b'])}):**")
            for obj_id in r["missing_from_b"]:
                lines.append(f"- `{obj_id}`")
            lines.append("")

        if r["missing_from_a"]:
            lines.append(f"**Missing from A / new in B ({len(r['missing_from_a'])}):**")
            for obj_id in r["missing_from_a"]:
                lines.append(f"- `{obj_id}`")
            lines.append("")

        if r["object_diffs"]:
            lines.append(f"**Objects with diffs ({len(r['object_diffs'])}):**\n")
            for obj_id, diffs in sorted(r["object_diffs"].items()):
                lines.append(f"<details><summary><code>{obj_id}</code></summary>\n")

                if diffs.get("attr_removed"):
                    lines.append(f"Attributes removed: {', '.join(f'`{k}`' for k in diffs['attr_removed'])}\n")
                if diffs.get("attr_added"):
                    lines.append(f"Attributes added: {', '.join(f'`{k}`' for k in diffs['attr_added'])}\n")
                if diffs.get("attr_changed"):
                    lines.append("Changed attribute values:\n")
                    lines.append("| Attribute | A value | B value |")
                    lines.append("|-----------|---------|---------|")
                    for k, va, vb in diffs["attr_changed"]:
                        lines.append(f"| `{k}` | `{va}` | `{vb}` |")
                    lines.append("")
                if diffs.get("lod_removed"):
                    lines.append(f"LoDs removed: {', '.join(diffs['lod_removed'])}\n")
                if diffs.get("lod_added"):
                    lines.append(f"LoDs added: {', '.join(diffs['lod_added'])}\n")
                if diffs.get("geom_changed"):
                    lines.append("Geometry differences by LoD:\n")
                    lines.append("| LoD | Surfaces only in A | Surfaces only in B |")
                    lines.append("|-----|-------------------|-------------------|")
                    for lod, cnt_a, cnt_b in diffs["geom_changed"]:
                        lines.append(f"| {lod} | {cnt_a} | {cnt_b} |")
                    lines.append("")

                lines.append("</details>\n")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Compare two sets of .city.json.gz tiles.")
    parser.add_argument("dir_a", help="Baseline tile directory")
    parser.add_argument("dir_b", help="Target tile directory")
    parser.add_argument(
        "--output",
        default="comparison_report.md",
        help="Output Markdown report path (default: comparison_report.md)",
    )
    parser.add_argument(
        "--geom-tolerance",
        type=float,
        default=0.001,
        help="Coordinate rounding tolerance in metres (default: 0.001)",
    )
    parser.add_argument(
        "--ignore-attr",
        action="append",
        default=["b3_t_run"],
        metavar="ATTR",
        help="Attribute name to ignore in comparison (can be repeated, default: b3_t_run)",
    )
    args = parser.parse_args()

    print(f"Scanning {args.dir_a} ...")
    tiles_a = find_tiles(args.dir_a)
    print(f"  Found {len(tiles_a)} files")

    print(f"Scanning {args.dir_b} ...")
    tiles_b = find_tiles(args.dir_b)
    print(f"  Found {len(tiles_b)} files")

    keys_a = set(tiles_a)
    keys_b = set(tiles_b)
    tiles_only_a = sorted(keys_a - keys_b)
    tiles_only_b = sorted(keys_b - keys_a)
    matched = sorted(keys_a & keys_b)

    print(f"\nTiles only in A: {len(tiles_only_a)}")
    print(f"Tiles only in B: {len(tiles_only_b)}")
    print(f"Matched tiles: {len(matched)}")

    tile_results = {}
    for i, tile_path in enumerate(matched, 1):
        print(f"  [{i}/{len(matched)}] {tile_path}", end="\r", flush=True)
        tile_results[tile_path] = compare_tile(
            tiles_a[tile_path], tiles_b[tile_path], args.geom_tolerance,
            ignore_attrs=frozenset(args.ignore_attr),
        )
    print()

    report = format_report(args.dir_a, args.dir_b, tiles_only_a, tiles_only_b, tile_results)

    out_path = Path(args.output)
    out_path.write_text(report, encoding="utf-8")
    print(f"\nReport written to {out_path}")


if __name__ == "__main__":
    main()
