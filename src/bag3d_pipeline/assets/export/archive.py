import csv
import zipfile
from zipfile import ZipFile

from dagster import asset, Output, AssetKey

from bag3d_pipeline.core import bag3d_export_dir


@asset(
    non_argument_deps={
        AssetKey(("export", "reconstruction_output_multitiles_nl"))
    },
    required_resource_keys={"file_store", "gdal"}
)
def geopackage_nl(context):
    """GeoPackage of the whole Netherlands, containing all 3D BAG layers.
    """
    path_export_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    path_tiles_dir = path_export_dir.joinpath("tiles")
    path_nl = path_export_dir.joinpath("3dbag_nl.gpkg")

    # Remove existing
    path_nl.unlink(missing_ok=True)

    with path_export_dir.joinpath("quadtree.tsv").open("r") as fo:
        csvreader = csv.reader(fo, delimiter="\t")
        # skip header, which is [id, level, nr_items, leaf, wkt]
        next(csvreader)
        leaf_ids = [row[0] for row in csvreader if row[3] == "true" and int(row[2]) > 0]
    # Find the first leaf that actually has exported data
    first_i_with_data = None
    for i, lid in enumerate(leaf_ids):
        path_first = create_path_layer(lid, path_tiles_dir)
        if path_first.exists():
            first_i_with_data = i
            break
    if first_i_with_data is None:
        raise ValueError("Did not find any .gpkg file")

    # Init the gpkg-s so that we can append to them later
    lid = leaf_ids[first_i_with_data]
    first_path_with_data = create_path_layer(lid, path_tiles_dir)
    cmd = [
        "OGR_SQLITE_SYNCHRONOUS=OFF",
        "{exe}",
        "-gt", "65536",
        "-lco", "SPATIAL_INDEX=NO",
        "-f", "GPKG",
        str(path_nl),
        str(first_path_with_data),
    ]
    cmd = " ".join(cmd)
    return_code, output = context.resources.gdal.execute("ogr2ogr", cmd)

    failed = []
    for lid in leaf_ids[first_i_with_data:]:
        path_with_data = create_path_layer(lid, path_tiles_dir)
        cmd = [
            "OGR_SQLITE_SYNCHRONOUS=OFF",
            "{exe}",
            "-gt", "65536",
            "-lco", "SPATIAL_INDEX=NO",
            "-append",
            "-f", "GPKG",
            str(path_nl),
            str(path_with_data),
        ]
        cmd = " ".join(cmd)
        try:
            return_code, output = context.resources.gdal.execute("ogr2ogr", cmd,
                                                             silent=True)
            if return_code != 0:
                failed.append((lid, output))
        except Exception as e:
            failed.append((lid, output))

    layers = ["pand", "lod12_2d", "lod12_3d", "lod13_2d", "lod13_3d",
              "lod22_2d", "lod22_3d",]
    for name_layer in layers:
        cmd = [
            "OGR_SQLITE_SYNCHRONOUS=OFF",
            "{exe}",
            str(path_nl),
            "-sql",
            f"\"SELECT CreateSpatialIndex('{name_layer}','geom')\""
        ]
        cmd = " ".join(cmd)
        context.resources.gdal.execute("ogrinfo", cmd)

    path_nl_zip = path_nl.with_suffix(".zip")
    with ZipFile(path_nl_zip, "w", compression=zipfile.ZIP_DEFLATED,
                 compresslevel=9) as myzip:
        myzip.write(path_nl)

    metadata = {}
    metadata[f"nr_failed"] = len(failed)
    metadata[f"ids_failed"] = [f[0] for f in failed]
    metadata["size uncompressed [Mb]"] = path_nl.stat().st_size * 1e-9
    metadata["size compressed [Mb]"] = path_nl_zip.stat().st_size * 1e-9
    path_nl.unlink(missing_ok=True)

    return Output(path_nl, metadata=metadata)


def create_path_layer(id_layer, path_tiles_dir):
    lid_in_filename = id_layer.replace("/", "-")
    name_lod12_2d = f"{lid_in_filename}.gpkg"
    path_lod12_2d = path_tiles_dir.joinpath(id_layer, name_lod12_2d)
    return path_lod12_2d


