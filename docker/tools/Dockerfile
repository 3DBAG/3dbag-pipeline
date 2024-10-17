FROM ubuntu:24.04 AS tools
ARG VERSION
ARG JOBS
ARG 3DBAG_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"

SHELL ["/bin/bash", "-c"]

RUN apt-get -y update && \
    apt-get -y install \
      make ninja-build gcc g++ cmake git wget libgeos-dev libgeos++-dev sqlite3 \
      libsqlite3-dev libtiff-dev openssh-client \
      libxinerama-dev libxcursor-dev xorg-dev libglu1-mesa-dev pkg-config \
      autoconf-archive curl software-properties-common nlohmann-json3-dev unzip zip \
      bzip2 tar libproj-dev clang \
      python3-venv python3-pip

RUN add-apt-repository -y ppa:deadsnakes/ppa ;\
    apt-get -y update ; \
    apt-get -y install python3.11 python3.11-venv python3.11-pip

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Download public key for github.com
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

COPY ./tools-build.sh $3DBAG_PIPELINE_LOCATION
COPY ./tools-test.sh $3DBAG_PIPELINE_LOCATION
WORKDIR $3DBAG_PIPELINE_LOCATION/tools
RUN bash $3DBAG_PIPELINE_LOCATION/tools-build.sh --build-all --jobs $JOBS --clean

RUN bash $3DBAG_PIPELINE_LOCATION/tools-test.sh --dir $3DBAG_PIPELINE_LOCATION/tools

#FROM tools AS dagster
#
#RUN python3.12 -m venv $3DBAG_PIPELINE_LOCATION/venv
#ENV VIRTUAL_ENV=$3DBAG_PIPELINE_LOCATION/venv
#ENV PATH=$3DBAG_PIPELINE_LOCATION/venv/bin:$PATH
#RUN python3 -m pip install \
#    dagster \
#    dagster-postgres \
#    dagster-docker \
#
#WORKDIR $3DBAG_PIPELINE_LOCATION