# Running the pipeline

The following section describes how to run the 3dbag-pipeline to produce the core of 3DBAG, excluding the extension packages (*floors_estimation* and *party_walls*).
Note that the 3dbag-pipeline is under active development and this section will change in the coming months.

Sourcing the inputs and generating the 3DBAG data requires a lot of storage.
As an indication, running the whole pipeline for a single AHN tile, with AHN versions 3 and 4, requires about 70GB space and 1-2 hours processing, depending on the machine.

The `core` package consists of several jobs and they need to be executed in a certain order:

1. ahn_tile_index
2. ahn3
3. ahn4
4. regular_grid_200m, laz_tiles_ahn3_200m, laz_tiles_ahn4_200m assets
5. source_input
6. nl_reconstruct
7. nl_export

