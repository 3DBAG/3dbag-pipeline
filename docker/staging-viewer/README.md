# Staging viewer

On the Docker host, run this command from the pipeline repository after
`b3d deploy` has started staging. You can start the viewer while the pipeline
is still running.

```sh
./docker/staging-viewer/up.sh
```

Run the same command for each new staging deployment or to pick up changes
from [3dbag-viewer main](https://github.com/3DBAG/3dbag-viewer/tree/main).
It resolves the current Git commit, builds that revision with the local
manifest template, and reads the release name, tile directory, pipeline volume
and export image from the running export container. The web server mounts the
pipeline volume read-only. The index worker publishes a separate FlatGeobuf
file per release and refreshes it when the export index or checksums change.

From your workstation, open an SSH tunnel to the Docker host.

```sh
ssh -N -L 8086:127.0.0.1:8086 gilfoyle.bk.tudelft.nl
```

Open the [3D viewer](http://localhost:8086/en/viewer) or the [download map](http://localhost:8086/en/download).
The three LoDs and static downloads become available as staging writes them.
Select a tile and click **CJLoupe** beside its CityJSON download to inspect it
through the same tunnel. The map backgrounds use the viewer's usual external providers.

By default, `up.sh` uses `b3d-stg-export` and binds to `127.0.0.1:8086`.
Set `EXPORT_CONTAINER` or `VIEWER_PORT` when running the command to change
the deployment it serves or the loopback port. The exporter must use the b3d
named-volume layout at `/data/volume`.

After startup, `up.sh` saves the resolved configuration in an ignored
`.env` file. From this directory, run `docker compose ps` for status or
`docker compose logs index` for index generation. Before removing a staging
volume, run `docker compose down` here to release the viewer's mounts.
