FROM ubuntu:24.04 AS tools
ARG VERSION
ARG JOBS

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"

SHELL ["/bin/bash", "-c"]

RUN apt-get -y update && \
    apt-get -y install \
      make ninja-build gcc g++ cmake git wget libgeos-dev libgeos++-dev sqlite3 \
      libsqlite3-dev libtiff-dev openssh-client \
      libxinerama-dev libxcursor-dev xorg-dev libglu1-mesa-dev pkg-config \
      autoconf-archive curl software-properties-common nlohmann-json3-dev unzip zip \
      bzip2 tar libproj-dev clang

RUN add-apt-repository -y ppa:deadsnakes/ppa ;\
    apt-get -y update ; \
    apt-get -y install python3.11 python3.11-venv

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Download public key for github.com
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true


WORKDIR /3dbag-pipeline
COPY ./build-tools.sh .
WORKDIR /3dbag-pipeline/.build-3dbag-pipeline
RUN bash /3dbag-pipeline/build-tools.sh --build-all --jobs $JOBS --clean

WORKDIR /3dbag-pipeline

FROM tools
COPY --from=tools /3dbag-pipeline/.build-3dbag-pipeline /3dbag-pipeline/.build-3dbag-pipeline

WORKDIR /3dbag-pipeline
COPY . /3dbag-pipeline
COPY ./docker/tools/.env /3dbag-pipeline/.env

WORKDIR /3dbag-pipeline
RUN make venvs

WORKDIR /3dbag-pipeline
