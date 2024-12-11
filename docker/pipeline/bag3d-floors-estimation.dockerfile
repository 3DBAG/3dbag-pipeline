FROM 3dgi/3dbag-pipeline-tools:2024.12.06 AS develop
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-floors-estimation"
LABEL org.opencontainers.image.description="The floors_estimation workflow package of the 3dbag-pipeline. Image for building the pipeline packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

WORKDIR $BAG3D_PIPELINE_LOCATION

ENV UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV

# Install only dependencies except the bag3d-common package
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/floors_estimation/uv.lock,target=$BAG3D_PIPELINE_LOCATION/packages/floors_estimation/uv.lock \
    --mount=type=bind,source=./packages/floors_estimation/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/floors_estimation/pyproject.toml \
    uv sync \
    --frozen \
    --all-extras \
    --no-install-project \
    --no-install-package bag3d-common\
    --project $BAG3D_PIPELINE_LOCATION/packages/floors_estimation \
    --python $VIRTUAL_ENV/bin/python

COPY . $BAG3D_PIPELINE_LOCATION
COPY ./docker/.env $BAG3D_PIPELINE_LOCATION/.env

# Install the workflow package and the bag3d-common package in editable mode
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
    --frozen \
    --all-extras \
    --project $BAG3D_PIPELINE_LOCATION/packages/floors_estimation \
    --python $VIRTUAL_ENV/bin/python

# Run dagster gRPC server on port 4001
EXPOSE 4001

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4001", "-m", "bag3d.floors_estimation.code_location", "--inject-env-vars-from-instance"]
