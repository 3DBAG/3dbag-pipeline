# Data flow

The main pipeline stages pass data through the file system via **stage directories** (`stages/<stage_name>/`), each enriching the per-building CityJSON feature files before the final export.

## Stage directory pipeline

```
stages/reconstruction/{tile_id}/objects/{pand_id}/reconstruct/{pand_id}.city.jsonl
        |
        |  party_walls.features_file_index    (opens/refreshes cjindex over stages/reconstruction/)
        |  party_walls.building_surfaces (pages FeatureRefs from index; reads DB: bag_adjacency)
        v
stages/party_walls/{tile_id}/{pand_id}.city.jsonl          <-- adds shared_walls geometry
        |
        |  floors_estimation.features_file_index  (opens/refreshes cjindex over stages/party_walls/)
        |  floors_estimation.bag3d_features       (pages FeatureRefs, extracts attributes into DB table)
        |  floors_estimation.save_cjfiles         (pages FeatureRefs, merges floor predictions back into files)
        v
stages/floors_estimation/{tile_id}/{pand_id}.city.jsonl    <-- adds b3_bouwlagen attribute
        |
        |  4x tyler export assets (multi, 3dtiles lod12/13/22)
        v
stages/export/{version}/
    +-- tiles/{tile_id}/  (.city.json, .gpkg, .obj, .mtl)  <-- tyler-multiformat
    +-- cesium3dtiles/lod{12,13,22}/                        <-- tyler
    +-- quadtree.tsv                                        <-- tyler output
    +-- sequence_header.json                                <-- generated before tyler
        |
        |  archive, compression, validation assets
        v
stages/export/{version}/
    +-- tiles/{tile_id}/*.gz, *-obj.zip                    <-- compressed_tiles
    +-- 3dbag_nl.gpkg, .gpkg.zip                           <-- geopackage_nl
    +-- export_index.csv                                   <-- export_index
    +-- reconstructed_features.csv                         <-- feature_evaluation
    +-- metadata.json                                      <-- metadata (from DB)
    +-- validate_compressed_files.csv                      <-- validate_compressed_files
```

## Stage details

### Reconstruction

The `reconstructed_building_models_nl` asset (partitioned by tile) runs roofer to produce per-building `.city.jsonl` files in a `z/x/y/objects/{pand_id}/reconstruct/` hierarchy under `stages/reconstruction/`.

### Party walls

`features_file_index` opens (or refreshes) a `cjindex` SQLite index over `stages/reconstruction/` and records the indexed feature count.  Stage handoff is still file-based; only **feature discovery** is backed by the index instead of directory walking.  `building_surfaces` pages over `FeatureRef` objects from the index, queries the `bag_adjacency` database table to determine adjacent buildings, and computes shared walls per building via multiprocessing. Each worker opens its own index instance.  `bag_adjacency` stores one row per directed pair `(identificatie, adjacent_identificatie)` for BAG polygons within 0.1 units, excluding self-pairs. Output files are written flat per tile to `stages/party_walls/{tile_id}/`.

### Floors estimation

`features_file_index` opens (or refreshes) a `cjindex` index over `stages/party_walls/` and records the indexed feature count. The ML sub-chain (`bag3d_features` -> `external_features` -> `all_features` -> `preprocessed_features` -> `inferenced_floors` -> `predictions_table`) operates in the database and pandas. `bag3d_features` and `save_cjfiles` page over `FeatureRef` objects from the index instead of consuming a `{id: path}` dict. Finally `save_cjfiles` reads the party_walls `.city.jsonl` files via the index and writes enriched copies with the `b3_bouwlagen` (floor count) attribute to `stages/floors_estimation/`.

### Export

Four tyler assets read from `stages/floors_estimation/` and write tiled output to `stages/export/{version}/`. Post-processing assets (compression, GeoPackage aggregation, validation) operate within the export directory.

`feature_evaluation` is a side branch that enumerates features from the `reconstruction_index` (backed by `cjindex`) instead of walking `stages/reconstruction/` directly. It produces a CSV summary of reconstruction quality. Stage outputs are unchanged in shape and location.

## Database as side channel

The `floors_estimation` workflow uses PostgreSQL as an intermediate store: `bag3d_features` extracts attributes from the `.city.jsonl` files into a database table, the ML pipeline runs entirely in DB/pandas, and `save_cjfiles` writes the predictions back into the files. The `building_surfaces` asset reads the row-based `bag_adjacency` table to determine which buildings are adjacent.

## Directory structure

```
stages/
+-- reconstruction/
|   +-- {z}/{x}/{y}/
|       +-- objects/
|       |   +-- {pand_id}/
|       |       +-- reconstruct/
|       |           +-- {pand_id}.city.jsonl
|       +-- roofer.toml
+-- party_walls/
|   +-- {tile_id}/
|       +-- {pand_id}.city.jsonl
+-- floors_estimation/
|   +-- {tile_id}/
|       +-- {pand_id}.city.jsonl
+-- export/
    +-- {version}/
        +-- tiles/
        |   +-- {tile_id}/
        |       +-- {tile_id}.city.json[.gz]
        |       +-- {tile_id}.gpkg[.gz]
        |       +-- {tile_id}.obj
        |       +-- {tile_id}.mtl
        |       +-- {tile_id}-obj.zip
        +-- cesium3dtiles/
        |   +-- lod12/
        |   +-- lod13/
        |   +-- lod22/
        +-- quadtree.tsv
        +-- sequence_header.json
        +-- export_index.csv
        +-- reconstructed_features.csv
        +-- metadata.json
        +-- 3dbag_nl.gpkg
        +-- 3dbag_nl.gpkg.zip
        +-- validate_compressed_files.csv
```
