set dotenv-load := true
datadir := "tests/data"
inputdir := datadir / "input"

# Create the directories for storing the input and output data
prepare:
    mkdir -p "tests/data/output"
    mkdir -p {{inputdir}}
    mkdir -p "packages/party_walls/tests/data/output"
    mkdir -p "packages/core/tests/data/output"


# Download the test data
download: prepare
    #!/usr/bin/env bash
    set -euxo pipefail
    # Download tiles
    tiles_exported=("9/560/624" "10/564/626" "10/566/626" "10/564/624" "10/566/624")
    for tile_id in "${tiles_exported[@]}"
    do
        tiledir="$SERVER_3DBAG_DIR/export/tiles/$tile_id"
        inputdir_tile="{{inputdir}}/export/tiles/$tile_id"
        mkdir -p "$inputdir_tile"
        rsync --ignore-existing -av $SERVER_NAME:"$tiledir/*" "$inputdir_tile/"

        # Uncompressing files to restore state before compression
        inputdir_tile_uncompressed="{{inputdir}}/export_uncompressed/tiles/$tile_id"
        mkdir -p "$inputdir_tile_uncompressed"
        for fullfile in "$inputdir_tile"/* ;
        do
            filename=$(basename -- "$fullfile")
            extension="${filename##*.}"
            filename="${filename%.*}"
            if [ $extension = 'gz' ]; then
                gunzip --keep -c $fullfile > "$inputdir_tile_uncompressed/$filename"
            elif [ $extension = 'zip' ]; then
                unzip $fullfile -d "$inputdir_tile_uncompressed"
            else
                ln -sf $fullfile "$inputdir_tile_uncompressed/"
            fi
        done
    done
    # Download loose files
    rsync --ignore-existing -av $SERVER_NAME:"$SERVER_3DBAG_DIR/export/quadtree.tsv" "{{inputdir}}/export/"
    ln -sfr "{{inputdir}}/export/quadtree.tsv" "{{inputdir}}/export_uncompressed/quadtree.tsv"
    # Link to packages
    ln -fsnr {{inputdir}} "packages/party_walls/tests/data"
    ln -fsnr {{inputdir}} "packages/core/tests/data"

clean-links:
    rm -rf "packages/core/tests/data"
    rm -rf "packages/party_walls/tests/data"

# Delete all data files
clean: clean-links
    rm -rf {{datadir}}
