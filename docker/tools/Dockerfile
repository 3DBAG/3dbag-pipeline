# These required image references are injected from 3dbag-manifest.json.
# `scratch` keeps the Dockerfile valid for static checks when they are omitted.
ARG UV_IMAGE=scratch
ARG TYLER_IMAGE=scratch
ARG TYLER_DB_IMAGE=scratch
ARG ROOFER_IMAGE=scratch
ARG CJVAL_IMAGE=scratch

FROM ${UV_IMAGE} AS uv-source
FROM ${TYLER_IMAGE} AS tyler-source
FROM ${TYLER_DB_IMAGE} AS tyler-db-source
FROM ${ROOFER_IMAGE} AS roofer-source
FROM ${CJVAL_IMAGE} AS cjval-source

FROM ubuntu:24.04 AS dependencies

ARG VERSION=latest
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

ARG GDAL_VERSION
ARG PROJ_VERSION
ARG GEOS_VERSION
ARG GEOTIFF_VERSION
ARG LASTOOLS_VERSION
ARG PDAL_VERSION
ARG VAL3DITY_VERSION
ARG CJIO_VERSION

ENV GDAL_VERSION=$GDAL_VERSION
ENV PROJ_VERSION=$PROJ_VERSION
ENV GEOS_VERSION=$GEOS_VERSION
ENV GEOTIFF_VERSION=$GEOTIFF_VERSION
ENV LASTOOLS_VERSION=$LASTOOLS_VERSION
ENV PDAL_VERSION=$PDAL_VERSION
ENV VAL3DITY_VERSION=$VAL3DITY_VERSION
ENV CJIO_VERSION=$CJIO_VERSION

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-tools"
LABEL org.opencontainers.image.description="Builder image for building the 3dbag-pipeline workflow images, also containing all the external tools that are required by the pipeline (roofer, GDAL etc.). Plus, it contains the dagster packages and the virtual environment for running a dagster grpc server for the workflow packages."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"


RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get -y update && \
    apt-get -y install \
      bash make ninja-build gcc g++ cmake git wget \
      libxinerama-dev libxcursor-dev xorg-dev libglu1-mesa-dev pkg-config \
      autoconf-archive autoconf libtool curl unzip zip bzip2 tar

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get -y install libgeos-dev libgeos++-dev sqlite3 \
    libsqlite3-dev libtiff-dev libproj-dev libproj25 libpq5 libpq-dev libpqxx-dev \
    libgeotiff-dev nlohmann-json3-dev openssh-client openssl ca-certificates \
    libxml++2.6-dev parallel libeigen3-dev libcgal-dev build-essential libboost-all-dev\
    libspatialite-dev libspatialite8t64 spatialite-bin \
    pipx

COPY --from=uv-source /uv /bin/uv
RUN uv python install 3.12

RUN mkdir -p $BAG3D_PIPELINE_LOCATION/tools
COPY ./tools-build.sh $BAG3D_PIPELINE_LOCATION
COPY ./tools-test.sh $BAG3D_PIPELINE_LOCATION


FROM dependencies AS tools
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

WORKDIR $BAG3D_PIPELINE_LOCATION/tools
RUN bash $BAG3D_PIPELINE_LOCATION/tools-build.sh \
    --dir $BAG3D_PIPELINE_LOCATION/tools \
    --build-gdal \
    --build-lastools \
    --build-pdal \
    --build-val3dity \
    --build-cjio \
    --build-proj \
    --jobs $JOBS \
    --clean

ENV GDAL_DATA=$BAG3D_PIPELINE_LOCATION/tools/share/gdal
ENV PROJ_DATA=$BAG3D_PIPELINE_LOCATION/tools/share/proj
RUN echo $BAG3D_PIPELINE_LOCATION/tools/lib >> /etc/ld.so.conf.d/3dbag-pipeline-tools.conf

FROM tools AS tyler
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --from=tyler-source /lib/ /tyler/lib/
COPY --from=tyler-source /lib64/ /tyler/lib64/
COPY --from=tyler-source /usr/ /tyler/usr/
COPY --from=tyler-source /usr/local/bin/tyler $BAG3D_PIPELINE_LOCATION/tools/bin/tyler
# Include Dutch transformation grids
COPY --from=tyler-source /usr/local/share/proj/nl_nsgi_nlgeo2018.tif $PROJ_DATA/nl_nsgi_nlgeo2018.tif
COPY --from=tyler-source /usr/local/share/proj/nl_nsgi_rdtrans2018.tif $PROJ_DATA/nl_nsgi_rdtrans2018.tif

COPY --from=tyler-db-source /lib/ /tyler/lib/
COPY --from=tyler-db-source /lib64/ /tyler/lib64/
COPY --from=tyler-db-source /usr/ /tyler/usr/
COPY --from=tyler-db-source /usr/local/bin/tyler-db $BAG3D_PIPELINE_LOCATION/tools/bin/tyler-db

RUN echo /tyler/usr/local/lib >> /etc/ld.so.conf.d/tyler.conf; \
    echo /tyler/lib >> /etc/ld.so.conf.d/tyler.conf; \
    echo /tyler/lib64 >> /etc/ld.so.conf.d/tyler.conf; \
    ldconfig

FROM tyler AS roofer
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --from=roofer-source /opt/roofer/bin/. $BAG3D_PIPELINE_LOCATION/tools/bin/
COPY --from=roofer-source /opt/roofer/share/proj $BAG3D_PIPELINE_LOCATION/tools/share/proj
COPY --from=roofer-source /opt/roofer/share/gdal $BAG3D_PIPELINE_LOCATION/tools/share/gdal

FROM roofer AS dagster

ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

# Activate the virtual environment
ENV VIRTUAL_ENV=$BAG3D_PIPELINE_LOCATION/venv
ENV PATH=$VIRTUAL_ENV/bin:$PATH
# Set DAGSTER_HOME, because the dagster grpc server needs it
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME && \
    touch $DAGSTER_HOME/dagster.yaml

# Create a virtual environment, otherwise python complains
RUN uv venv --python 3.12 $VIRTUAL_ENV
# Install packages into the virtual environment
COPY docker/tools/requirements.txt .
RUN --mount=type=cache,mode=0755,target=/root/.cache/uv uv pip install -r requirements.txt


FROM dagster AS cjval

ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --from=cjval-source /usr/local/bin/cjval $BAG3D_PIPELINE_LOCATION/tools/bin/cjval

FROM cjval AS test-tools
# Clean up the image
RUN rm -rf $HOME/.cache/*; \
    apt-get -y remove \
      make ninja-build gcc g++ cmake wget autoconf-archive autoconf libtool curl; \
    apt-get -y clean; \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*;

RUN bash $BAG3D_PIPELINE_LOCATION/tools-test.sh --dir $BAG3D_PIPELINE_LOCATION/tools
