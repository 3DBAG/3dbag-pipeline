#!/bin/bash

# Build a base image that is used for storing the 3D BAG sample data.
# Need to run from 3dbag-pipeline/docker

# We start with the postgis image.
docker pull "postgis/postgis:14-3.3"

# But the original postgres image adds the postgres data directory as a volume, which
# means that when a container of this image is committed with the data, the volume is
# dropped from the container, thus loosing the database.
# E.g. see https://stackoverflow.com/a/52015975
# Original postgres image volume is 'null: /var/lib/postgresql/data'
./docker-copyedit/docker-copyedit.py FROM "postgis/postgis:14-3.3" INTO "postgis/postgis:14-3.3-novolume" REMOVE ALL VOLUMES
rm ./load.tmp

cd "3dbag-sample-data" || return 1
docker build -t "balazsdukai/3dbag-sample-data:base" .
docker tag "balazsdukai/3dbag-sample-data:base" "balazsdukai/3dbag-sample-data:latest"
