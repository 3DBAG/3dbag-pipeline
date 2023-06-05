# Various expeperiments and benchmarks

## EPT vs. flat index

Files with the test area: `gilfoyle:/data/pointcloud/AHN4/ept_test`

The [COPC](https://copc.io) files were generated with [untwine](https://github.com/hobuinc/untwine).

```bash
untwine \
  --files=/data/pointcloud/AHN4/ept_test/source \
  --output_dir=/data/pointcloud/AHN4/ept_test/copc \
  --cube=false \
  --single_file=true
```

```bash
/opt/bin/pdal translate "/data/pointcloud/AHN4/ept_test/source/C_31HZ2.LAZ" "/data/pointcloud/AHN4/ept_test/copc/c_31hz2.copc.laz"
```

Create the 200m tiles with `pdal tile`. 
Normally, we would use `las2las` with a pre-built tile index, but `pdal tile` is simpler and good enough for this.

```bash
docker run --rm -v "/data/pointcloud/AHN4/ept_test":/tmp pdal/pdal:sha-cfa827b6 \
pdal tile \
  --input "/tmp/source/*" \
  --output "/tmp/tiles_200m/t_#.laz" \
  --length 200
```

Create a tile index.

```bash
docker run --rm -v "/data/pointcloud/AHN4/ept_test/tiles_200m":/tmp pdal/pdal:sha-cfa827b6 \
pdal tindex create \
  --tindex "/tmp/tindex.geojson" \
  --filespec "/tmp/*.laz" \
  --fast_boundary \
  --ogrdriver GeoJSON \
  --t_srs "EPSG:28992" \
  --write_absolute_path \
  --lyr_name "pdal"
```

## Data

BAG for the AHN tile 31hz2.

```bash
ogr2ogr -spat 135000 450000 140000 456249 -f GeoJSON "buildings.geojson" PG:'dbname=baseregisters host=localhost port=5432 user=bdukai' "lvbag.pandactueelbestaand"
```