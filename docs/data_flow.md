# Data flow

The main pipeline stages pass data through the file system via **stage directories** (`stages/<stage_name>/`). Reconstruction, party-wall, and floor-estimation stages exchange tile-level CityJSONSeq files; individual features are discovered through a `cityjson-index` SQLite index.

## Stage directory pipeline

```
stages/reconstruction/{z}/{x}/{y}/<tile>.city.jsonl
        |
        |  party_walls.building_surfaces (opens/refreshes the cityjson-index index; pages PackageRefs with keyset cursors; reads package CityModels and DB: bag_adjacency)
        v
stages/party_walls/{tile_id}/{tile_leaf}.city.jsonl          <-- adds shared_walls geometry
        |
        |  floors_estimation.bag3d_features       (opens/refreshes the cityjson-index index; pages PackageRefs with keyset cursors; extracts attributes into DB table)
        |  floors_estimation.save_cjfiles         (opens/refreshes the cityjson-index index; pages PackageRefs with keyset cursors; merges floor predictions back into files)
        v
stages/floors_estimation/{tile_id}/{tile_leaf}.city.jsonl    <-- adds b3_bouwlagen attribute
        |
        |  4x tyler export assets (multi, 3dtiles lod12/13/22)
        v
stages/export/{version}/
    +-- t/{tile_id}.<suffix>  (.city.json, .gpkg, .obj, .mtl)
    +-- ogc3dtiles/lod{12,13,22}/                        <-- Tyler
    +-- debug/quadtree.tsv                               <-- merged Tyler output
    +-- sequence_header.json                             <-- generated before Tyler
        |
        |  archive, compression, validation assets
        v
stages/export/{version}/
    +-- t/{tile_id}.*.gz, *-obj.zip                    <-- compressed_tiles
    +-- 3dbag_nl.gpkg, .gpkg.zip                           <-- geopackage_nl
    +-- export_index.csv                                   <-- export_index
    +-- reconstructed_features.csv                         <-- feature_evaluation
    +-- metadata.json                                      <-- metadata (from DB)
    +-- validate_compressed_files.csv                      <-- validate_compressed_files
```

## Stage details

### Reconstruction

The `reconstructed_building_models_nl` asset (partitioned by tile) runs Roofer and writes tile-level CityJSONSeq data below `stages/reconstruction/{z}/{x}/{y}/`. The index exposes individual package references without requiring consumers to know the source file layout.

### Party walls

`building_surfaces` opens (or refreshes) a `cityjson-index` SQLite index over `stages/reconstruction/` directly. Stage handoff remains file-based, while feature discovery and reads use paged `PackageRef` objects. The asset queries the `bag_adjacency` database table, computes shared walls per building via multiprocessing, and writes one strict CityJSONSeq file per tile to `stages/party_walls/{tile_id}/`. Each worker opens its own index instance.

### Floors estimation

`bag3d_features` and `save_cjfiles` each open (or refresh) a `cityjson-index` index over `stages/party_walls/` directly. The ML sub-chain (`bag3d_features` -> `external_features` -> `all_features` -> `preprocessed_features` -> `inferenced_floors` -> `predictions_table`) operates in the database and pandas. Both assets page over `PackageRef` objects instead of consuming a `{id: path}` mapping. `save_cjfiles` reads package CityModels from the CityJSONSeq files, merges `b3_bouwlagen`, and writes strict CityJSONSeq output to `stages/floors_estimation/`.

### Export

Four tyler assets read from `stages/floors_estimation/` and write tiled output to `stages/export/{version}/`. Post-processing assets (compression, GeoPackage aggregation, validation) operate within the export directory.

`feature_evaluation` is a side branch that enumerates features from the `reconstruction_index` (backed by `cityjson-index`) instead of walking `stages/reconstruction/` directly. It produces a CSV summary of reconstruction quality. Stage outputs are unchanged in shape and location.

## Database as side channel

The `floors_estimation` workflow uses PostgreSQL as an intermediate store: `bag3d_features` extracts attributes from the `.city.jsonl` files into a database table, the ML pipeline runs entirely in DB/pandas, and `save_cjfiles` writes the predictions back into the files. The `building_surfaces` asset reads the row-based `bag_adjacency` table to determine which buildings are adjacent.

## Directory structure

```
stages/
+-- reconstruction/
|   +-- {z}/{x}/{y}/
|       +-- {tile}.city.jsonl
|       +-- roofer.toml
+-- party_walls/
|   +-- {tile_id}/
|       +-- {tile_leaf}.city.jsonl
+-- floors_estimation/
|   +-- {tile_id}/
|       +-- {tile_leaf}.city.jsonl
+-- export/
    +-- {version}/
        +-- t/
        |   +-- {tile_id}.<suffix>
        |       +-- {tile_id}.city.json[.gz]
        |       +-- {tile_id}.gpkg[.gz]
        |       +-- {tile_id}.obj
        |       +-- {tile_id}.mtl
        |       +-- {tile_id}-obj.zip
        +-- ogc3dtiles/
        |   +-- lod12/
        |   +-- lod13/
        |   +-- lod22/
        +-- debug/quadtree.tsv
        +-- sequence_header.json
        +-- export_index.csv
        +-- reconstructed_features.csv
        +-- metadata.json
        +-- 3dbag_nl.gpkg
        +-- 3dbag_nl.gpkg.zip
        +-- validate_compressed_files.csv
```
