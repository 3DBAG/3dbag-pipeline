#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"

# b3d names code-location containers after the Compose project and location.
container=${EXPORT_CONTAINER:-b3d-stg-export}
release=$(docker exec "$container" printenv BAG3D_RELEASE_VERSION)
tiles=$(docker exec "$container" sh -c 'echo "${BAG3D_TILES_DIR:-tiles}"')
volume=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data/volume"}}{{.Name}}{{end}}{{end}}' "$container")
image=$(docker inspect --format '{{.Config.Image}}' "$container")
for component in "$release" "$tiles"; do
    if [[ ! $component =~ ^[a-zA-Z0-9._~-]+$ || $component == . || $component == .. ]]; then
        echo "Invalid release or tile directory in $container: $component" >&2
        exit 1
    fi
done
if [[ -z $volume ]]; then
    echo "$container must mount its pipeline volume at /data/volume" >&2
    exit 1
fi

# Resolve once so the build context and image label identify the same revision.
revision=$(git ls-remote --exit-code https://github.com/3DBAG/3dbag-viewer.git refs/heads/main | cut -f1)
port=${VIEWER_PORT:-8086}
if [[ ! $port =~ ^[1-9][0-9]{0,4}$ ]] || (( port > 65535 )); then
    echo "VIEWER_PORT must be a TCP port between 1 and 65535" >&2
    exit 1
fi

# Compose prioritizes shell variables over the settings discovered for this run.
unset RELEASE_VERSION TILES_DIR PIPELINE_VOLUME EXPORT_IMAGE VIEWER_REVISION
env_file=$(mktemp .env_viewer.XXXXXX)
trap 'rm -f -- "$env_file"' EXIT
cat > "$env_file" <<EOF
VIEWER_REVISION=$revision
RELEASE_VERSION=$release
TILES_DIR=$tiles
PIPELINE_VOLUME=$volume
EXPORT_IMAGE=$image
VIEWER_PORT=$port
EOF
echo "Deploying viewer ${revision:0:12} for $release from $volume"
docker compose --env-file "$env_file" up -d --build
mv -- "$env_file" .env
