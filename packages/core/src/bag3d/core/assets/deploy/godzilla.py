"""Deploy 3D BAG to godzilla"""
import tarfile
from datetime import datetime
from pathlib import Path
import json

from dagster import AssetIn, Output, asset, AssetKey
from fabric import Connection

from bag3d.common.utils.database import load_sql
from bag3d.common.types import PostgresTableIdentifier


@asset(
    ins={
        "reconstruction_output_multitiles_nl": AssetIn(key_prefix="export")
    },
    deps=[
        AssetKey(("export", "geopackage_nl")),
        AssetKey(("export", "export_index")),
        AssetKey(("export", "metadata")),
        AssetKey(("export", "compressed_tiles")),
        AssetKey(("export", "compressed_tiles_validation")),
    ],
)
def compressed_export_nl(context, reconstruction_output_multitiles_nl):
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
        "reconstruction_output_multitiles_zuid_holland":
            AssetIn(key_prefix="export"),
        "geopackage_nl": AssetIn(key_prefix="export"),
        "export_index": AssetIn(key_prefix="export"),
        "metadata": AssetIn(key_prefix="export"),
    },
)
def compressed_export_zuid_holland(
        context,
        reconstruction_output_multitiles_zuid_holland,
        geopackage_nl,
        export_index,
        metadata
):
    """A .tar.gz compressed full directory tree of the exports"""
    export_dir = reconstruction_output_multitiles_zuid_holland
    output_tarfile = export_dir.parent / "export.tar.gz"
    with tarfile.open(output_tarfile, "w:gz") as tar:
        tar.add(export_dir, arcname="export")
    metadata_output = {"size [Gb]": output_tarfile.stat().st_size * 1e-9,
                       "path": str(output_tarfile)}
    return Output(output_tarfile, metadata=metadata_output)


@asset(
    ins={
        "metadata": AssetIn(key_prefix="export")
    }
)
def downloadable_godzilla(context, compressed_export_nl: Path, metadata: Path):
    """Downloadable files hosted on godzilla.
    - Transfer the export.tar.gz archive to `godzilla:/data/3DBAGv3`
    - Uncompress the archive and add the current version to the directory name
    - Symlink to the 'export' to the current version
    - Add the current version to the tar.gz archive
    """
    deploy_dir = "/data/3DBAGv3"
    with metadata.open("r") as fo:
        metadata_json = json.load(fo)
        version = metadata_json["identificationInfo"]["citation"]["edition"]
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        c.put(compressed_export_nl, remote=deploy_dir)
        # delete symlink here, because the uncompressed tar archive is also 'export',
        # so we have a bit of downtime here, but that's ok
        c.run(f"rm {deploy_dir}/export")
        c.run(f"tar -C {deploy_dir} -xzvf {deploy_dir}/export.tar.gz")
        c.run(f"mv {deploy_dir}/export {deploy_dir}/export_{version}")
        # symlink to latest version so the fileserver picks up the data
        c.run(f"ln -s {deploy_dir}/export_{version} {deploy_dir}/export")
        # add version to the tar so that we can archive the data
        c.run(f"mv {deploy_dir}/export.tar.gz {deploy_dir}/export_{version}.tar.gz")
    return deploy_dir


@asset(
    required_resource_keys={"db_connection"}
)
def webservice_godzilla(context, downloadable_godzilla):
    """Load the layers for WFS, WMS that are served from godzilla"""
    schema = "webservice_dev"
    old_schema = "webservice_latest"
    # sql = f"drop schema if exists {schema} cascade; create schema {schema};"
    # with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
    #     context.log.debug(sql)
    #     c.run(
    #         f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'")

    deploy_dir = downloadable_godzilla

    # for layer in ["pand", "lod12_2d", "lod13_2d", "lod22_2d"]:
    #     cmd = " ".join([
    #         "PG_USE_COPY=YES",
    #         "OGR_TRUNCATE=YES",
    #         "ogr2ogr",
    #         "-gt", "65536",
    #         "-lco", "SPATIAL_INDEX=NONE",
    #         "-f", "PostgreSQL",
    #         f'PG:"dbname=baseregisters port=5432 host=localhost user=etl active_schema={schema}"',
    #         f"/vsizip/{deploy_dir}/export/3dbag_nl.gpkg.zip",
    #         layer,
    #         "-nln", layer + "_tmp"
    #     ])
    #     with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
    #         context.log.debug(cmd)
    #         c.run(cmd)


    pand_table = PostgresTableIdentifier(schema, "pand_tmp")
    lod12_2d_tmp = PostgresTableIdentifier(schema, "lod12_2d_tmp")
    lod13_2d_tmp = PostgresTableIdentifier(schema, "lod13_2d_tmp")
    lod22_2d_tmp = PostgresTableIdentifier(schema, "lod22_2d_tmp")
    lod12_2d = PostgresTableIdentifier(schema, "lod12_2d")
    lod13_2d = PostgresTableIdentifier(schema, "lod13_2d")
    lod22_2d = PostgresTableIdentifier(schema, "lod22_2d")

    # Create the LoD tables
    sql = load_sql(filename="webservice_lod.sql",
                   query_params={
                       'pand_table': pand_table,
                       'lod12_2d_tmp': lod12_2d_tmp,
                       'lod13_2d_tmp': lod13_2d_tmp,
                       'lod22_2d_tmp': lod22_2d_tmp,
                       'lod12_2d': lod12_2d,
                       'lod13_2d': lod13_2d,
                       'lod22_2d': lod22_2d})
    sql = context.resources.db_connection.print_query(sql)
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'")

    # # Create the intermediary export_index and validate_compressed_files tables so that they can be populated from the CSV files
    export_index = PostgresTableIdentifier(schema, "export_index")
    validate_compressed_files = PostgresTableIdentifier(schema, "validate_compressed_files")
    sql = load_sql(filename="webservice_tiles_intermediary.sql",
                   query_params={
                       "export_index": export_index,
                       "validate_compressed_files": validate_compressed_files
                   })
    sql = context.resources.db_connection.print_query(sql)
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'")
    # Load the CSV files into the intermediary tables
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        filepath = f"{deploy_dir}/export/export_index.csv"
        copy_cmd = "\copy " + str(export_index) + " FROM '" + filepath + "' DELIMITER ',' CSV HEADER "
        context.log.debug(f"{copy_cmd}")
        c.run(
            fr'psql --dbname baseregisters --port 5432 --host localhost --user etl -c "{copy_cmd}" ')
        filepath = f"{deploy_dir}/export/validate_compressed_files.csv"
        copy_cmd = "\copy " + str(validate_compressed_files) + " FROM '" + filepath + "' DELIMITER ',' CSV HEADER "
        context.log.debug(f"{copy_cmd}")
        c.run(
            fr'psql --dbname baseregisters --port 5432 --host localhost --user etl -c "{copy_cmd}" ')
    # Create the public 'tiles' table
    tiles = PostgresTableIdentifier(schema, "tiles")
    sql = load_sql(filename="webservice_tiles.sql",
                   query_params={
                       "new_table": tiles,
                       "export_index": export_index,
                       "validate_compressed_files": validate_compressed_files
                   })
    sql = context.resources.db_connection.print_query(sql)
    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        context.log.debug(sql)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{sql}'")

    # extension = str(datetime.now().date())
    #alter_to_archive = f"ALTER SCHEMA {old_schema} RENAME TO bag3d_{extension};"
    #alter_to_old = f"ALTER SCHEMA {schema} RENAME TO {old_schema};"
    grant_usage = f"GRANT USAGE ON SCHEMA {schema} TO bag_geoserver;"
    grant_select = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO bag_geoserver;"

    with Connection(host="godzilla.bk.tudelft.nl", user="dagster") as c:
        #context.log.debug(alter_to_archive)
        # c.run(
        #     f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{alter_to_archive}'")
        # context.log.debug(alter_to_old)
        # context.log.debug(alter_to_old)
        # c.run(
        #     f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{alter_to_old}'")
        context.log.debug(grant_usage)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{grant_usage}'")
        context.log.debug(grant_select)
        c.run(
            f"psql --dbname baseregisters --port 5432 --host localhost --user etl -c '{grant_select}'")

    return f"{old_schema}.lod12_2d", f"{old_schema}.lod13_2d", f"{old_schema}.lod22_2d", f"{old_schema}.tile_index"
