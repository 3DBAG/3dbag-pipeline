FROM 3dgi/3dbag-pipeline-tools:2024.10.23 AS develop
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-party-walls"
LABEL org.opencontainers.image.description="The party_walls workflow package of the 3dbag-pipeline. Image for building the pipeline packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

RUN rm -rf $VIRTUAL_ENV
RUN python3.11 -m venv $VIRTUAL_ENV
RUN python -m pip install --upgrade setuptools wheel pip
COPY docker/tools/requirements.txt .
RUN python -m pip install -r requirements.txt
RUN apt-get install -y libgdal-dev

WORKDIR $BAG3D_PIPELINE_LOCATION
COPY . $BAG3D_PIPELINE_LOCATION
COPY ./docker/.env $BAG3D_PIPELINE_LOCATION/.env

# Install the workflow package
RUN python -m pip install --no-cache-dir $BAG3D_PIPELINE_LOCATION/packages/party_walls/.[dev]

# Clean up the image
RUN rm -rf $HOME/.cache/*; \
    rustup self uninstall; \
    apt-get -y remove \
      clang make ninja-build gcc g++ cmake git wget \
      autoconf-archive autoconf libtool curl software-properties-common llvm-18; \
    python -m pip cache purge; \
    apt-get -y clean; \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*; \
    rm -rf /opt/3dbag-pipeline/.git; \
    rm -rf /opt/3dbag-pipeline/packages/common/.*; \
    rm -rf /opt/3dbag-pipeline/packages/party_walls/.*;

# Run dagster gRPC server on port 4002
EXPOSE 4002

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4002", "-m", "bag3d.party_walls.code_location", "--inject-env-vars-from-instance"]
