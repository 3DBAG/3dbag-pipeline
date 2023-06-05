import csv
import concurrent.futures

from dagster import asset, Output, AssetKey

from bag3d_pipeline.core import bag3d_export_dir

@asset(
    non_argument_deps={
        AssetKey(("export", "reconstruction_output_multitiles_zuid_holland"))
    },
    required_resource_keys={"file_store", "gdal"}
)
def geopackage_nl(context):
    """GeoPackage of the whole Netherlands, containing all 3D BAG layers.
    The tiles are first compiled to a gpkg file per layer, each layer is compiled
    concurrently. For one layer, the tiles are appended sequentially.
    Finally, the layers are added to the main gpkg.
    """
    path_export_dir = bag3d_export_dir(context.resources.file_store.data_dir)
    path_tiles_dir = path_export_dir.joinpath("tiles")
    path_nl = path_export_dir.joinpath("3dbag_nl.gpkg")
    layers_3dbag = {
        "LoD12-2D": path_export_dir.joinpath("3dbag_nl_lod12_2d.gpkg"),
        "LoD12-3D": path_export_dir.joinpath("3dbag_nl_lod12_3d.gpkg"),
        "LoD13-2D": path_export_dir.joinpath("3dbag_nl_lod13_2d.gpkg"),
        "LoD13-3D": path_export_dir.joinpath("3dbag_nl_lod13_3d.gpkg"),
        "LoD22-2D": path_export_dir.joinpath("3dbag_nl_lod22_2d.gpkg"),
        "LoD22-3D": path_export_dir.joinpath("3dbag_nl_lod22_3d.gpkg")
    }

    # Remove existing
    path_nl.unlink(missing_ok=True)
    for p in layers_3dbag.values():
        p.unlink(missing_ok=True)

    with path_export_dir.joinpath("quadtree.tsv").open("r") as fo:
        csvreader = csv.reader(fo, delimiter="\t")
        # skip header, which is [id, level, nr_items, leaf, wkt]
        next(csvreader)
        leaf_ids = [row[0] for row in csvreader if row[3] == "true" and int(row[2]) > 0]
    # Find the first leaf that actually has exported data
    first_i_with_data = None
    for i, lid in enumerate(leaf_ids):
        path_lod12_2d = create_path_layer(lid, "LoD12-2D", path_tiles_dir)
        if path_lod12_2d.exists():
            first_i_with_data = i
            break

    # Init the gpkg-s so that we can append to them later
    for name_layer, path_layer in layers_3dbag.items():
        lid = leaf_ids[first_i_with_data]
        first_path_with_data = create_path_layer(lid, name_layer, path_tiles_dir)
        cmd = [
            "OGR_SQLITE_SYNCHRONOUS=OFF",
            "{exe}",
            "-gt", "65536",
            "-lco", "SPATIAL_INDEX=NO",
            "-f", "GPKG",
            str(path_layer),
            str(first_path_with_data),
            "-nln", name_layer,
            "geom"
        ]
        cmd = " ".join(cmd)
        return_code, output = context.resources.gdal.execute("ogr2ogr", cmd)

    def compile_layer(name_layer, path_layer):
        failed = []
        for lid in leaf_ids[first_i_with_data:]:
            path_with_data = create_path_layer(lid, name_layer, path_tiles_dir)
            cmd = [
                "OGR_SQLITE_SYNCHRONOUS=OFF",
                "{exe}",
                "-gt", "65536",
                "-append",
                "-f", "GPKG",
                str(path_layer),
                str(path_with_data),
                "-nln", name_layer,
                "geom"
            ]
            cmd = " ".join(cmd)
            return_code, output = context.resources.gdal.execute("ogr2ogr", cmd, silent=True)
            if return_code != 0:
                failed.append((lid, output))
        return failed

    metadata = {}
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {
            executor.submit(compile_layer, name_layer, path_layer): name_layer
            for name_layer, path_layer in layers_3dbag.items()
        }
        for future in concurrent.futures.as_completed(future_to_url):
            name_layer = future_to_url[future]
            try:
                failed = future.result()
                if len(failed) > 0:
                    metadata[f"nr_failed [{name_layer}]"] = len(failed)
                    metadata[f"ids_failed [{name_layer}]"] = [f[0] for f in failed]
                    context.log.error(
                        f"Layer {name_layer} failed to append {len(failed)} tiles."
                    )
            except Exception as exc:
                context.log.error(exc)

    for name_layer, path_layer in layers_3dbag.items():
        cmd = [
            "OGR_SQLITE_SYNCHRONOUS=OFF",
            "{exe}",
            "-gt", "65536",
            "-lco", "SPATIAL_INDEX=NO",
            "-append",
            "-f", "GPKG",
            str(path_nl),
            str(path_layer),
            name_layer
        ]
        cmd = " ".join(cmd)
        context.resources.gdal.execute("ogr2ogr", cmd)

    for name_layer, path_layer in layers_3dbag.items():
        cmd = [
            "OGR_SQLITE_SYNCHRONOUS=OFF",
            "{exe}",
            str(path_nl),
            "-sql",
            f"\"SELECT CreateSpatialIndex('{name_layer}','geom')\""
        ]
        cmd = " ".join(cmd)
        context.resources.gdal.execute("ogrinfo", cmd)

    return Output(path_nl, metadata=metadata)


def create_path_layer(id_layer, name_layer, path_tiles_dir):
    lid_in_filename = id_layer.replace("/", "-")
    name_lod12_2d = f"{lid_in_filename}-{name_layer}.gpkg"
    path_lod12_2d = path_tiles_dir.joinpath(id_layer, name_lod12_2d)
    return path_lod12_2d


