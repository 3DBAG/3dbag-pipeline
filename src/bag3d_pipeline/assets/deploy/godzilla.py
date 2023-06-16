"""Deploy 3D BAG to godzilla"""
import tarfile

from dagster import asset, AssetIn, Output
from fabric import Connection
from datetime import datetime

@asset(
    ins={
        "reconstruction_output_multitiles_nl": AssetIn(key_prefix="export"),
        "geopackage_nl": AssetIn(key_prefix="export"),
        "export_index": AssetIn(key_prefix="export"),
        "metadata": AssetIn(key_prefix="export"),
        "compressed_tiles": AssetIn(key_prefix="export"),
    },
)
def compressed_export_nl(context,
                         reconstruction_output_multitiles_nl,
                         geopackage_nl, export_index, metadata, compressed_tiles
                         ):
    """A .tar.gz compressed full directory tree of the exports"""
    export_dir = reconstruction_output_multitiles_nl
    output_tarfile = export_dir.parent / "export.tar.gz"
    with tarfile.open(output_tarfile, "w:gz") as tar:
        tar.add(export_dir, arcname="export")
    metadata_output = {"size [Gb]": output_tarfile.stat().st_size * 1e-9,
                       "path": str(output_tarfile)}
    return Output(output_tarfile, metadata=metadata_output)

@asset(
    ins={
        "reconstruction_output_multitiles_zuid_holland": AssetIn(key_prefix="export"),
        "geopackage_nl": AssetIn(key_prefix="export"),
        "export_index": AssetIn(key_prefix="export"),
        "metadata": AssetIn(key_prefix="export"),
    },
)
def compressed_export_zuid_holland(context,
                                   reconstruction_output_multitiles_zuid_holland,
                                   geopackage_nl, export_index, metadata
                                   ):
    """A .tar.gz compressed full directory tree of the exports"""
    export_dir = reconstruction_output_multitiles_zuid_holland
    output_tarfile = export_dir.parent / "export.tar.gz"
    with tarfile.open(output_tarfile, "w:gz") as tar:
        tar.add(export_dir, arcname="export")
    metadata_output = {"size [Gb]": output_tarfile.stat().st_size * 1e-9,
                       "path": str(output_tarfile)}
    return Output(output_tarfile, metadata=metadata_output)


@asset
def downloadable_godzilla(context, compressed_export_nl):
    """Downloadable files hosted on godzilla"""
    deploy_dir = "/data/3DBAGv3"
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        c.run(f"rm -f {deploy_dir}/export.tar.gz")
        c.run(f"rm -rf {deploy_dir}/export")
        c.put(compressed_export_zuid_holland, remote=deploy_dir)
        c.run(f"tar -C {deploy_dir} -xzvf {deploy_dir}/export.tar.gz")
    return deploy_dir


@asset
def webservice_godzilla(context, downloadable_godzilla):
    """Load the layers for WFS, WMS that are served from godzilla"""
    schema = "bag3d_tmp"
    old_schema = "bag3d_latest"
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        c.run(
            f"pgsql --dbname baseregisters --port 5432 --host localhost --user etl -c 'drop schema if exists {schema}; create schema {schema};'")
        
    deploy_dir = downloadable_godzilla
    cmd = [
        "PG_USE_COPY=YES",
        "OGR_TRUNCATE=YES",
        "ogr2ogr",
        "-gt", "65536",
        "-lco", "SPATIAL_INDEX=NONE",
        "-f", "PostgreSQL",
        f'PG:"dbname=baseregisters port=5432 host=localhost user=etl active_schema={schema}"',
        f"{deploy_dir}/export/3dbag_nl.gpkg"
    ]
    for layer in ["LoD12-2D", "LoD13-2D", "LoD22-2D"]:
        cmd.append(layer)
        cmd = " ".join(cmd)
        with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
            c.run(cmd)

    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        c.run(
            f"pgsql --dbname baseregisters --port 5432 --host localhost --user etl -c 'create index lod12_2d_geom_idx on {schema}.lod12_2d using gist (geom)'")
        c.run(
            f"pgsql --dbname baseregisters --port 5432 --host localhost --user etl -c 'create index lod13_2d_geom_idx on {schema}.lod13_2d using gist (geom)'")
        c.run(
            f"pgsql --dbname baseregisters --port 5432 --host localhost --user etl -c 'create index lod22_2d_geom_idx on {schema}.lod22_2d using gist (geom)'")

    cmd = [
        "PG_USE_COPY=YES",
        "OGR_TRUNCATE=YES",
        "ogr2ogr",
        "-gt", "65536",
        "-lco", "SPATIAL_INDEX=NONE",
        "-f", "PostgreSQL",
        f'PG:"dbname=baseregisters port=5432 host=localhost user=etl active_schema={schema}"',
        f"{deploy_dir}/export/export_index.csv",
        "-nln", "tile_index"
    ]
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        c.run(cmd)

    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        c.run(
            f"pgsql --dbname baseregisters --port 5432 --host localhost --user etl -c 'create index tile_index_geom_idx on {schema}.tile_index using gist (geom)'")

    extension = str(datetime.now().date())

    # with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
    #     c.run(
    #         f"pgsql --dbname baseregisters --port 5432 --host localhost --user etl -c 'ALTER SCHEMA {old_schema} name RENAME TO bag3d_{extension} ;'")
    #     c.run(
    #         f"pgsql --dbname baseregisters --port 5432 --host localhost --user etl -c 'ALTER SCHEMA {schema} name RENAME TO {old_schema} ;'")
        
    return f"{old_schema}.lod12_2d", f"{old_schema}.lod13_2d", f"{old_schema}.lod22_2d", f"{old_schema}.tile_index"
