FROM 3dgi/3dbag-pipeline-tools:2024.12.14 AS develop
ARG VERSION=develop
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-party-walls"
LABEL org.opencontainers.image.description="The party_walls workflow package of the 3dbag-pipeline. Image for building the pipeline packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

RUN rm -rf $VIRTUAL_ENV
RUN uv venv --python 3.11 $VIRTUAL_ENV
ENV UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV
# Install packages into the virtual environment
COPY docker/tools/requirements.txt .
RUN --mount=type=cache,mode=0755,target=/root/.cache/uv uv pip install -r requirements.txt
RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get -y update && \
    apt-get install -y libgdal-dev && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*;

WORKDIR $BAG3D_PIPELINE_LOCATION


# Install only dependencies except the bag3d-common package
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./packages/party_walls/uv.lock,target=$BAG3D_PIPELINE_LOCATION/packages/party_walls/uv.lock \
    --mount=type=bind,source=./packages/party_walls/pyproject.toml,target=$BAG3D_PIPELINE_LOCATION/packages/party_walls/pyproject.toml \
    uv sync \
    --frozen \
    --all-extras \
    --no-install-project \
    --no-install-package bag3d-common\
    --project $BAG3D_PIPELINE_LOCATION/packages/party_walls \
    --python $VIRTUAL_ENV/bin/python

COPY . $BAG3D_PIPELINE_LOCATION

# Install the workflow package and the bag3d-common package in editable mode
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
    --frozen \
    --all-extras \
    --project $BAG3D_PIPELINE_LOCATION/packages/party_walls \
    --python $VIRTUAL_ENV/bin/python

# Run dagster gRPC server on port 4002
EXPOSE 4002

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4002", "-m", "bag3d.party_walls.code_location"]
