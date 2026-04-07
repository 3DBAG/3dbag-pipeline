FROM 3dgi/3dbag-pipeline-tools:2026.04.01 AS develop
ARG VERSION=develop
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-core"
LABEL org.opencontainers.image.description="The core workflow package of the 3dbag-pipeline. Image for building the pipeline packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

WORKDIR $BAG3D_PIPELINE_LOCATION

ENV UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV

# Install only third-party dependencies (layer cached by lock/pyproject content)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/core/uv.lock,target=$BAG3D_PIPELINE_LOCATION/packages/core/uv.lock \
    --mount=type=bind,source=./packages/core/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/core/pyproject.toml \
    --mount=type=bind,source=./packages/common/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/common/pyproject.toml \
    --mount=type=bind,source=./docker/vendor/cjlib-0.3.0-py3-none-any.whl,target=$BAG3D_PIPELINE_LOCATION/docker/vendor/cjlib-0.3.0-py3-none-any.whl \
    --mount=type=bind,source=./docker/vendor/cjindex-0.3.0-py3-none-any.whl,target=$BAG3D_PIPELINE_LOCATION/docker/vendor/cjindex-0.3.0-py3-none-any.whl \
    uv sync \
    --frozen \
    --all-extras \
    --no-install-project \
    --no-install-package bag3d-common \
    --project $BAG3D_PIPELINE_LOCATION/packages/core \
    --python $VIRTUAL_ENV/bin/python

COPY packages/common $BAG3D_PIPELINE_LOCATION/packages/common
COPY packages/core $BAG3D_PIPELINE_LOCATION/packages/core
COPY docker/vendor $BAG3D_PIPELINE_LOCATION/docker/vendor

# Install the workflow package and the bag3d-common package in editable mode
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
    --frozen \
    --all-extras \
    --project $BAG3D_PIPELINE_LOCATION/packages/core \
    --python $VIRTUAL_ENV/bin/python

# Run dagster gRPC server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "bag3d.core.code_location"]
