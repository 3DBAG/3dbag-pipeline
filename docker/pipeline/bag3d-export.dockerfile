#FROM 3dgi/3dbag-pipeline-tools:2026.06.15 AS develop
FROM 3dbag-pipeline-tools:tmp-build AS develop
ARG VERSION=develop
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-export"
LABEL org.opencontainers.image.description="The export workflow package of the 3dbag-pipeline. Image for building the pipeline packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

WORKDIR $BAG3D_PIPELINE_LOCATION

ENV UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV
ENV BAG3D_MANIFEST_PATH=$BAG3D_PIPELINE_LOCATION/3dbag-manifest.json

COPY ./3dbag-manifest.json $BAG3D_PIPELINE_LOCATION/

# Install only third-party dependencies (layer cached by lock/pyproject content)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/export/uv.lock,target=$BAG3D_PIPELINE_LOCATION/packages/export/uv.lock \
    --mount=type=bind,source=./packages/export/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/export/pyproject.toml \
    --mount=type=bind,source=./packages/common/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/common/pyproject.toml \
    uv sync \
    --frozen \
    --all-extras \
    --no-install-project \
    --no-install-package bag3d-common \
    --project $BAG3D_PIPELINE_LOCATION/packages/export \
    --python $VIRTUAL_ENV/bin/python

COPY packages/common $BAG3D_PIPELINE_LOCATION/packages/common
COPY packages/export $BAG3D_PIPELINE_LOCATION/packages/export

# Install the workflow package and the bag3d-common package in editable mode
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
    --frozen \
    --all-extras \
    --project $BAG3D_PIPELINE_LOCATION/packages/export \
    --python $VIRTUAL_ENV/bin/python

# Run dagster gRPC server on port 4003
EXPOSE 4003

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4003", "-m", "bag3d.export.code_location"]
