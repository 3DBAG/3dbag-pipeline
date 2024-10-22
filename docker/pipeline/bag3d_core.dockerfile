FROM 3dgi/3dbag-pipeline-tools:2024.10.20 AS develop
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-core"
LABEL org.opencontainers.image.description="The core workflow package of the 3dbag-pipeline. Image for building the pipeline packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

WORKDIR $BAG3D_PIPELINE_LOCATION
COPY . $BAG3D_PIPELINE_LOCATION
COPY ./docker/.env $BAG3D_PIPELINE_LOCATION/.env

# Install the workflow package
RUN python -m pip install $BAG3D_PIPELINE_LOCATION/packages/core/.[dev]

# Clean up the image
RUN rm -rf $HOME/.cache/*; \
    rustup self uninstall; \
    sudo rustup self uninstall; \
    apt-get -y remove --purge \
      clang make ninja-build gcc g++ cmake git wget python3.11 python3.11-venv \
      autoconf-archive autoconf libtool curl software-properties-common llvm-18; \
    apt-get -y autoremove; \
    python -m pip cache purge; \
    apt-get -y clean; \
    rm -rf /opt/3dbag-pipeline/.git; \
    rm -rf /opt/3dbag-pipeline/packages/common/.*; \
    rm -rf /opt/3dbag-pipeline/packages/core/.*;

# Run dagster gRPC server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "bag3d.core.code_location", "--inject-env-vars-from-instance"]


FROM ubuntu:24.04 AS production
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-core"
LABEL org.opencontainers.image.description="The core workflow package of the 3dbag-pipeline. Minimized image for production."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

WORKDIR $BAG3D_PIPELINE_LOCATION
# Activate the virtual environment
ENV VIRTUAL_ENV=$BAG3D_PIPELINE_LOCATION/venv
ENV PATH=$BAG3D_PIPELINE_LOCATION/venv/bin:$PATH

COPY --from=develop / /

# Run dagster gRPC server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "bag3d.core.code_location", "--inject-env-vars-from-instance"]
