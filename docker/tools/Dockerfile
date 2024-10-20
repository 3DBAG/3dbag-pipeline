FROM ubuntu:24.04 AS tools
ARG VERSION=latest
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-tools"
LABEL org.opencontainers.image.description="Builder image for building the 3dbag-pipeline workflow images, also containing all the external tools that are required by the pipeline (roofer, GDAL etc.). Plus, it contains the dagster packages and the virtual environment for running a dagster grpc server for the workflow packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

SHELL ["/bin/bash", "-c"]

RUN apt-get -y update && \
    apt-get -y install \
      make ninja-build gcc g++ cmake git wget libgeos-dev libgeos++-dev sqlite3 \
      libsqlite3-dev libtiff-dev openssh-client openssl libpq5 libpq-dev libpqxx-dev \
      libxinerama-dev libxcursor-dev xorg-dev libglu1-mesa-dev pkg-config \
      autoconf-archive autoconf libtool curl software-properties-common \
      nlohmann-json3-dev unzip zip bzip2 tar libproj-dev clang bison flex \
      python3-venv python3-pip

# Python 3.11 is currently required by the party_walls package
RUN add-apt-repository -y ppa:deadsnakes/ppa ;\
    apt-get -y update ; \
    apt-get -y install python3.11 python3.11-venv

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Download public key for github.com
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN mkdir -p $BAG3D_PIPELINE_LOCATION/tools
COPY ./tools-build.sh $BAG3D_PIPELINE_LOCATION
COPY ./tools-test.sh $BAG3D_PIPELINE_LOCATION


FROM tools AS build
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

WORKDIR $BAG3D_PIPELINE_LOCATION/tools
RUN bash $BAG3D_PIPELINE_LOCATION/tools-build.sh --build-all --jobs $JOBS --clean



# We only need this until we replace the old geoflow with roofer
FROM build AS geoflow-old
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/bin/geof $BAG3D_PIPELINE_LOCATION/tools/bin/geof
COPY --from=3dgi/geoflow-bundle-builder:2024.08.09 /export /export
COPY --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/src/FileGDB/lib/. /usr/local/src/FileGDB/lib
COPY --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/libfgdbunixrtl.so /usr/local/lib/libfgdbunixrtl.so
COPY --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/LASlib /export/usr/local/lib/
COPY --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/gdalplugins /export/usr/local/lib/
COPY --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/geoflow-plugins/. $BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/plugins
ENV GF_PLUGIN_FOLDER=$BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/plugins

RUN echo $BAG3D_PIPELINE_LOCATION/tools/lib >> /etc/ld.so.conf.d/3dbag-pipeline-tools.conf; \
    echo /export/usr/local/lib >> /etc/ld.so.conf.d/3dbag-pipeline-tools.conf; \
    echo /export/lib >> /etc/ld.so.conf.d/3dbag-pipeline-tools.conf; \
    ldconfig

RUN bash $BAG3D_PIPELINE_LOCATION/tools-test.sh --dir $BAG3D_PIPELINE_LOCATION/tools



FROM geoflow-old AS dagster
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

# Create a virtual environment, otherwise python complains
RUN python3.12 -m venv $BAG3D_PIPELINE_LOCATION/venv
# Activate the virtual environment
ENV VIRTUAL_ENV=$BAG3D_PIPELINE_LOCATION/venv
ENV PATH=$BAG3D_PIPELINE_LOCATION/venv/bin:$PATH
# Install packages into the virtual environment
COPY docker/tools/requirements.txt .
RUN python -m pip install --upgrade setuptools wheel pip
RUN python -m pip install -r requirements.txt

# Set DAGSTER_HOME, because the dagster grpc server needs it
ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME && \
    touch $DAGSTER_HOME/dagster.yaml

WORKDIR $BAG3D_PIPELINE_LOCATION
