set dotenv-load := true
datadir := "tests" / "data"
inputdir := datadir / "input"

# Create the directories for storing the input and output data
prepare:
    mkdir -p {{inputdir}}
    mkdir -p "tests/data/output/3DBAG"
    mkdir -p "packages/party_walls/tests/data/output/3DBAG"
    mkdir -p "packages/core/tests/data/output/3DBAG"

# Download the test data, area is POLYGON((153568 462867, 153559 463931, 155658 463916, 155651 462867, 153568 462867))
download: prepare
    #!/usr/bin/env bash
    set -euxo pipefail
    # Download the reconstructed features
    tiles_reconstruction=("9/560/624" "10/564/626" "10/566/626" "10/564/624" "10/566/624")
    for tile_id in "${tiles_reconstruction[@]}" ;
    do
        tiledir="$SERVER_RECONSTRUCTION_DIR/$tile_id"
        inputdir_tile="{{inputdir}}/3DBAG/crop_reconstruct/$tile_id"
        mkdir -p "$inputdir_tile"
        rsync --ignore-existing --exclude "objects/*/crop" --exclude "objects/*/config_.toml" --exclude "crop.toml" --exclude "features.txt" --exclude -av $SERVER_NAME:"$tiledir/*" "$inputdir_tile/"
    done
    # Download exported tiles
    tiles_exported=("9/560/624" "10/564/626" "10/566/626" "10/564/624" "10/566/624")
    for tile_id in "${tiles_exported[@]}" ;
    do
        tiledir="$SERVER_3DBAG_DIR/export/tiles/$tile_id"
        inputdir_tile="{{inputdir}}/export/3DBAG/export/tiles/$tile_id"
        mkdir -p "$inputdir_tile"
        rsync --ignore-existing -av $SERVER_NAME:"$tiledir/*" "$inputdir_tile/"

        # Uncompressing files to restore state before compression
        inputdir_tile_uncompressed="{{inputdir}}/export_uncompressed/3DBAG/export/tiles/$tile_id"
        mkdir -p "$inputdir_tile_uncompressed"
        for fullfile in "$inputdir_tile"/* ;
        do
            filename=$(basename -- "$fullfile")
            extension="${filename##*.}"
            filename="${filename%.*}"
            if [ $extension = 'gz' ]; then
                gunzip --keep -c $fullfile > "$inputdir_tile_uncompressed/$filename"
            elif [ $extension = 'zip' ]; then
                unzip -o $fullfile -d "$inputdir_tile_uncompressed"
            else
                ln -sf $fullfile "$inputdir_tile_uncompressed/"
            fi
        done
    done
    # Download loose files
    rsync --ignore-existing -av $SERVER_NAME:"$SERVER_3DBAG_DIR/export/quadtree.tsv" "{{inputdir}}/export/3DBAG/export/"
    ln -sfr "{{inputdir}}/export/quadtree.tsv" "{{inputdir}}/export_uncompressed/3DBAG/export/quadtree.tsv"
    # Link to packages
    ln -fsnr {{inputdir}} "packages/party_walls/tests/data"
    ln -fsnr {{inputdir}} "packages/core/tests/data"

# Delete all data files from the packages
clean-links:
    rm -rf "packages/core/tests/data"
    rm -rf "packages/party_walls/tests/data"

# Delete all data files
clean: clean-links
    rm -rf {{datadir}}
