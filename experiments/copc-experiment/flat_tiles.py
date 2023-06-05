import json
from time import time, time_ns
from pathlib import Path

from shapely.geometry import shape
from shapely.strtree import STRtree
import pdal

if __name__ == "__main__":
    with open("/home/bdukai/software/copc-experiment/tindex.geojson", "r") as fo:
        tindex = json.load(fo)
    index_by_id = {}
    for t in tindex["features"]:
        tile = shape(t["geometry"])
        index_by_id[id(tile)] = (tile, Path(t["properties"]["location"]).name)

    tileindex = STRtree(v[0] for v in index_by_id.values())

    with open("/home/bdukai/software/copc-experiment/buildings.geojson", "r") as fo:
        buildings = json.load(fo)

    print(f"Total nr. of buildings: {len(buildings['features'])}")
    print("id,identificatie,nr_points,run_ns,nr_laz_tiles,wkt")
    time_start = time()
    for i, building in enumerate(buildings["features"]):
        time_f_start = time_ns()
        polygon = shape(building["geometry"])
        laztiles_isect = [index_by_id[id(tile)] for tile in tileindex.query(polygon)]
        total_points = 0
        for tile in laztiles_isect:
            reader = pdal.Reader().las(
                f"/data/pointcloud/AHN4/ept_test/tiles_200m/{tile[1]}")
            filter_crop = pdal.Filter.crop(
                bounds=f"([{polygon.bounds[0]}, {polygon.bounds[2]}], [{polygon.bounds[1]}, {polygon.bounds[3]}])")
            total_points += pdal.Pipeline((reader, filter_crop)).execute()
        print(f"{i}\t{building['properties']['identificatie']}\t{total_points}\t{time_ns() - time_f_start}\t{len(laztiles_isect)}\t{polygon.wkt}")

    print(f"Point query time: {round((time() - time_start) / 60)} minutes")
