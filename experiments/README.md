# Various experiments and benchmarks

## EPT vs. flat index

See copc-experiment on gilfoyle.

## Snapshot source data into GeoPackage

We can snapshot the versions of the source data and archive it in `GPKG.tar.gz` files.
The compressed GPKG files are directly read by QGIS.
Postgres can connect to them as well with a [Foreign Data Wrapper](https://github.com/pramsey/pgsql-ogr-fdw).

That is, we archive the source versions where the reconstruction succeeded, and we can access the archives later in case we need to dig through previous versions.

For example:

```postgresql
CREATE EXTENSION ogr_fdw;

CREATE SERVER top10nlfdw_gpkg
	FOREIGN DATA WRAPPER ogr_fdw
	OPTIONS (
		datasource 'top10nl.gpkg',
		format 'GPKG' );

CREATE SCHEMA top10nlfdw;

IMPORT FOREIGN SCHEMA ogr_all
	FROM SERVER top10nlfdw_gpkg
	INTO top10nlfdw;
```