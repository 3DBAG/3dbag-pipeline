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
      autoconf-archive autoconf libtool curl unzip zip bzip2 tar

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get -y install libgeos-dev libgeos++-dev sqlite3 \
    libsqlite3-dev libtiff-dev libproj-dev libproj25 libpq5 libpq-dev libpqxx-dev \
    libgeotiff-dev nlohmann-json3-dev openssh-client openssl ca-certificates \
    libxml++2.6-dev parallel

COPY --link --from=ghcr.io/astral-sh/uv:0.4.26 /uv /uvx /bin/
# Python 3.11 is currently required by the party_walls package
RUN uv python install 3.11 3.12

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

COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha8 /usr/src/tyler/resources/geof /usr/src/tyler/resources/geof
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha8 /usr/local/share/proj /usr/local/share/proj
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha8 /lib/ /tyler/lib/
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha8 /lib64/ /tyler/lib64/
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha8 /usr/ /tyler/usr/
COPY --link --from=3dgi/tyler-multiformat:0.4.0-alpha8 /usr/local/bin/tyler $BAG3D_PIPELINE_LOCATION/tools/bin/tyler

COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /lib/ /tyler/lib/
COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /lib64/ /tyler/lib64/
COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /usr/ /tyler/usr/
COPY --link --from=3dgi/tyler-db:0.3.4-db-alpha5 /usr/local/bin/tyler-db $BAG3D_PIPELINE_LOCATION/tools/bin/tyler-db

RUN echo /tyler/usr/local/lib >> /etc/ld.so.conf.d/tyler.conf; \
    echo /tyler/lib >> /etc/ld.so.conf.d/tyler.conf; \
    echo /tyler/lib64 >> /etc/ld.so.conf.d/tyler.conf; \
    ldconfig

RUN mkdir -p $BAG3D_PIPELINE_LOCATION/tools/share/tyler/resources/geof && \
    wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/createGLB.json -O $BAG3D_PIPELINE_LOCATION/tools/share/tyler/resources/geof/createGLB.json && \
    wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/createMulti.json -O $BAG3D_PIPELINE_LOCATION/tools/share/tyler/resources/geof/createMulti.json && \
    wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/metadata.json -O $BAG3D_PIPELINE_LOCATION/tools/share/tyler/resources/geof/metadata.json && \
    wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/process_feature.json -O $BAG3D_PIPELINE_LOCATION/tools/share/tyler/resources/geof/process_feature.json && \
    wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/process_feature_multi.json -O $BAG3D_PIPELINE_LOCATION/tools/share/tyler/resources/geof/process_feature_multi.json


FROM tyler AS roofer
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --link --from=3dgi/roofer:develop /opt/roofer/bin/. $BAG3D_PIPELINE_LOCATION/tools/bin/
COPY --link --from=3dgi/roofer:develop /opt/roofer/share/proj $BAG3D_PIPELINE_LOCATION/tools/share/proj
COPY --link --from=3dgi/roofer:develop /opt/roofer/share/gdal $BAG3D_PIPELINE_LOCATION/tools/share/gdal
ENV GDAL_DATA=$BAG3D_PIPELINE_LOCATION/tools/share/gdal
ENV PROJ_DATA=$BAG3D_PIPELINE_LOCATION/tools/share/proj

# We only need this until we replace the old geoflow with roofer
FROM roofer AS geoflow-old
ARG JOBS=2
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

COPY --link --from=3dgi/geoflow-bundle-builder:2024.12.13 /usr/local/bin/geof $BAG3D_PIPELINE_LOCATION/tools/bin/geof
COPY --link --from=3dgi/geoflow-bundle-builder:2024.12.13 /export /export
COPY --link --from=3dgi/geoflow-bundle-builder:2024.12.13 /usr/local/src/FileGDB/lib/. /usr/local/src/FileGDB/lib
COPY --link --from=3dgi/geoflow-bundle-builder:2024.12.13 /usr/local/lib/libfgdbunixrtl.so /usr/local/lib/libfgdbunixrtl.so
COPY --link --from=3dgi/geoflow-bundle-builder:2024.12.13 /usr/local/lib/LASlib /export/usr/local/lib/
COPY --link --from=3dgi/geoflow-bundle-builder:2024.12.13 /usr/local/lib/gdalplugins /export/usr/local/lib/
COPY --link --from=3dgi/geoflow-bundle-builder:2024.12.13 /usr/local/lib/geoflow-plugins/. $BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/plugins
ENV GF_PLUGIN_FOLDER=$BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/plugins

RUN echo /export/usr/local/lib >> /etc/ld.so.conf.d/geoflow.conf; \
    echo /export/lib >> /etc/ld.so.conf.d/geoflow.conf; \
    ldconfig

RUN mkdir -p $BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/flowcharts && \
    wget --no-verbose https://raw.githubusercontent.com/geoflow3d/gfc-brecon/79ab70bc7b08aee37a1ceca7e3bb4db18c0f2778/stream/reconstruct_bag.json -O $BAG3D_PIPELINE_LOCATION/tools/share/geoflow-bundle/flowcharts/reconstruct_bag.json


FROM geoflow-old AS dagster
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

# Activate the virtual environment
ENV VIRTUAL_ENV=$BAG3D_PIPELINE_LOCATION/venv
ENV PATH=$VIRTUAL_ENV/bin:$PATH
# Set DAGSTER_HOME, because the dagster grpc server needs it
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME && \
    touch $DAGSTER_HOME/dagster.yaml
#RUN useradd -ms /bin/sh -u 1001 bag3d && \
#    chown -R bag3d /opt/3dbag-pipeline && \
#    chown -R bag3d $DAGSTER_HOME
#USER bag3d

# Create a virtual environment, otherwise python complains
RUN uv venv --python 3.12 $VIRTUAL_ENV
# Install packages into the virtual environment
COPY --link docker/tools/requirements.txt .
RUN --mount=type=cache,mode=0755,target=/root/.cache/uv uv pip install -r requirements.txt



FROM dagster AS test-tools

# Clean up the image
RUN rm -rf $HOME/.cache/*; \
    apt-get -y remove \
      make ninja-build gcc g++ cmake wget autoconf-archive autoconf libtool curl; \
    apt-get -y clean; \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*;

RUN bash $BAG3D_PIPELINE_LOCATION/tools-test.sh --dir $BAG3D_PIPELINE_LOCATION/tools
