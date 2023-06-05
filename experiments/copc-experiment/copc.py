import json
from time import time, time_ns
from zipfile import ZipFile

from shapely.geometry import shape
import copclib as copc

if __name__ == "__main__":
    # Create a reader object
    reader = copc.FileReader("/data/pointcloud/AHN4/ept_test/copc/ahn_2.copc.laz")

    with ZipFile('/home/bdukai/software/copc-experiment/buildings.geojson.zip') as myzip:
        with myzip.open('buildings.geojson', "r") as myfile:
            buildings = json.load(myfile)

    print(f"Total nr. of buildings: {len(buildings['features'])}")
    print("id,identificatie,nr_points,run_ns,wkt")
    time_start = time()
    for i,building in enumerate(buildings["features"]):
        time_f_start = time_ns()
        polygon = shape(building["geometry"])
        box = copc.Box(polygon.bounds)
        points = reader.GetPointsWithinBox(box)
        print(f"{i}\t{building['properties']['identificatie']}\t{len(points)}\t{time_ns() - time_f_start}\t{polygon.wkt}")

    print(f"Point query time: {round((time() - time_start)/60)} minutes")

# # (139779.741, 452306.005, 139786.186, 452312.363)
# # POLYGON ((139779.741 452308.591, 139783.566 452306.005, 139786.186 452309.82, 139782.348 452312.363, 139779.741 452308.591))
# with open("/home/bdukai/software/copc-experiment/one.txt", "w") as fo:
#     for point in points:
#         fo.write(f"{point.x} {point.y} {point.z}\n")
