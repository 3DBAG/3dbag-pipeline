FROM 3dgi/3dbag-pipeline-tools:latest1
ARG BAG3D_PIPELINE_LOCATION=/opt/3dbag-pipeline

LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DBAG"
LABEL org.opencontainers.image.title="3dbag-pipeline-party-walls"
LABEL org.opencontainers.image.description="The party_walls workflow package of the 3dbag-pipeline."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="(MIT OR Apache-2.0)"

RUN python3.11 -m venv /venv_3dbag_pipeline
ENV VIRTUAL_ENV=/venv_3dbag_pipeline
ENV PATH=/venv_3dbag_pipeline/bin:$PATH
RUN python -m pip install --upgrade setuptools wheel pip
COPY docker/tools/requirements.txt .
RUN python -m pip install -r requirements.txt
RUN apt-get install -y libgdal-dev

WORKDIR $BAG3D_PIPELINE_LOCATION
COPY . $BAG3D_PIPELINE_LOCATION
COPY ./docker/.env $BAG3D_PIPELINE_LOCATION/.env

# Install the workflow package
RUN python -m pip install --no-cache-dir $BAG3D_PIPELINE_LOCATION/packages/party_walls/.[dev]

# Run dagster gRPC server on port 4002
EXPOSE 4002

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4002", "-m", "bag3d.party_walls.code_location", "--inject-env-vars-from-instance"]
