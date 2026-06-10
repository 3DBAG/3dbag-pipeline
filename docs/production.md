# Running the pipeline

The following section describes how to run the 3dbag-pipeline to produce the core of 3DBAG, excluding the extension packages (*floors_estimation* and *party_walls*).

Sourcing the inputs and generating the 3DBAG data requires a lot of storage.
As an indication, running the whole pipeline for a single AHN tile requires about 70GB space and 1-2 hours processing, depending on the machine.

The `core` package consists of several jobs that need to be executed in a certain order:

1. **`bgt`** — Load the latest BGT Pand layer.
2. **`ahn_tile_index`** — Download the AHN tile index (bladwijzer) and checksum files, and create the metadata tables for AHN 3, 4 and 5. Must be run before the AHN download jobs.
3. **`ahn3`** / **`ahn4`** / **`ahn5`** — Download the LAZ files and record metadata for each AHN version. These jobs are partitioned by tile and can be run independently. The `ahn_checksum_sensor` automates triggering these jobs when PDOK updates the upstream checksum files.
4. **`ahn_metadata_index`** — Create indices on the AHN metadata tables. Run after all AHN tile downloads are complete.
5. **`source_input`** — Download and stage BAG and TOP10NL source data, then prepare the input for reconstruction (tiling, intermediary processing).
6. **`nl_reconstruct`** — Run the crop and reconstruct steps for the Netherlands (partitioned by tile).
7. **`nl_export`** — Run the tyler export and 3D Tiles steps. Use **`nl_export_after_floors`** instead if the *floors_estimation* package has been run first — it includes the GeoPackage and 3D Tiles outputs.
8. **`nl_deploy`** — Deploy the Netherlands data to the production servers.
9.  **`nl_release`** — Perform the final steps for the 3DBAG release (publish data and webservices).

For debugging reconstruction, **`nl_reconstruct_debug`** runs the same steps as `nl_reconstruct` with additional debug output.

