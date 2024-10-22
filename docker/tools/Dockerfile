FROM ubuntu:24.04 AS dependencies
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

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get -y update && \
    apt-get -y install \
      make ninja-build gcc g++ cmake git wget \
      libxinerama-dev libxcursor-dev xorg-dev libglu1-mesa-dev pkg-config \
      autoconf-archive autoconf libtool curl software-properties-common \
      unzip zip bzip2 tar

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get -y install libgeos-dev libgeos++-dev sqlite3 \
    libsqlite3-dev libtiff-dev libproj-dev libproj25 libpq5 libpq-dev libpqxx-dev \
    libgeotiff-dev nlohmann-json3-dev python3-venv python3-pip openssh-client openssl \
    libxml++2.6-dev

# Python 3.11 is currently required by the party_walls package
RUN add-apt-repository -y ppa:deadsnakes/ppa ;\
    apt-get -y update ; \
    apt-get -y install python3.11 python3.11-venv

RUN mkdir -p $BAG3D_PIPELINE_LOCATION/tools
COPY --link ./tools-build.sh $BAG3D_PIPELINE_LOCATION
COPY --link ./tools-test.sh $BAG3D_PIPELINE_LOCATION


FROM dependencies AS tools
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

WORKDIR $BAG3D_PIPELINE_LOCATION/tools
RUN bash $BAG3D_PIPELINE_LOCATION/tools-build.sh \
    --dir $BAG3D_PIPELINE_LOCATION/tools \
    --build-gdal \
    --build-lastools \
    --build-pdal \
    --jobs $JOBS \
    --clean

ENV GDAL_DATA=$BAG3D_PIPELINE_LOCATION/tools/share/gdal
ENV PROJ_DATA=/usr/share/proj
RUN echo $BAG3D_PIPELINE_LOCATION/tools/lib >> /etc/ld.so.conf.d/3dbag-pipeline-tools.conf


FROM tools AS tyler
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha7 /usr/src/tyler/resources/geof /usr/src/tyler/resources/geof
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha7 /usr/local/share/proj /usr/local/share/proj
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha7 /lib/ /tyler/lib/
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha7 /lib64/ /tyler/lib64/
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha7 /usr/ /tyler/usr/
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha7 /usr/local/bin/tyler $BAG3D_PIPELINE_LOCATION/tools/bin/tyler

COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /lib/ /tyler/lib/
COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /lib64/ /tyler/lib64/
COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /usr/ /tyler/usr/
COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /usr/local/bin/tyler-db $BAG3D_PIPELINE_LOCATION/tools/bin/tyler-db

RUN echo /tyler/usr/local/lib >> /etc/ld.so.conf.d/tyler.conf; \
    echo /tyler/lib >> /etc/ld.so.conf.d/tyler.conf; \
    echo /tyler/lib64 >> /etc/ld.so.conf.d/tyler.conf; \
    ldconfig


FROM tyler AS roofer
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --link --from=3dgi/roofer:develop /opt/roofer/bin/. $BAG3D_PIPELINE_LOCATION/tools/bin/

# We only need this until we replace the old geoflow with roofer
FROM roofer AS geoflow-old
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --link --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/bin/geof $BAG3D_PIPELINE_LOCATION/tools/bin/geof
COPY --link --from=3dgi/geoflow-bundle-builder:2024.08.09 /export /export
COPY --link --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/src/FileGDB/lib/. /usr/local/src/FileGDB/lib
COPY --link --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/libfgdbunixrtl.so /usr/local/lib/libfgdbunixrtl.so
COPY --link --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/LASlib /export/usr/local/lib/
COPY --link --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/gdalplugins /export/usr/local/lib/
COPY --link --from=3dgi/geoflow-bundle-builder:2024.08.09 /usr/local/lib/geoflow-plugins/. $BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/plugins
ENV GF_PLUGIN_FOLDER=$BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/plugins

RUN echo /export/usr/local/lib >> /etc/ld.so.conf.d/geoflow.conf; \
    echo /export/lib >> /etc/ld.so.conf.d/geoflow.conf; \
    ldconfig


FROM geoflow-old AS dagster
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

# Create a virtual environment, otherwise python complains
RUN python3.12 -m venv $BAG3D_PIPELINE_LOCATION/venv
# Activate the virtual environment
ENV VIRTUAL_ENV=$BAG3D_PIPELINE_LOCATION/venv
ENV PATH=$BAG3D_PIPELINE_LOCATION/venv/bin:$PATH
# Install packages into the virtual environment
COPY --link docker/tools/requirements.txt .
RUN python -m pip install --upgrade setuptools wheel pip
RUN python -m pip install -r requirements.txt

# Set DAGSTER_HOME, because the dagster grpc server needs it
ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME && \
    touch $DAGSTER_HOME/dagster.yaml

WORKDIR $BAG3D_PIPELINE_LOCATION


FROM dagster AS test-tools

RUN bash $BAG3D_PIPELINE_LOCATION/tools-test.sh --dir $BAG3D_PIPELINE_LOCATION/tools
