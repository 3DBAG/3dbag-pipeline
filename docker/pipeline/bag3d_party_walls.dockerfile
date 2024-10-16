FROM 3dgi/3dbag-pipeline-tools:2024.09.24

RUN python3.11 -m venv /venv_3dbag_pipeline
ENV VIRTUAL_ENV=/venv_3dbag_pipeline
ENV PATH=/venv_3dbag_pipeline/bin:$PATH
RUN python3 -m pip install --upgrade setuptools wheel pip
RUN python3 -m pip install \
    dagster \
    dagster-postgres \
    dagster-docker

WORKDIR /3dbag-pipeline
COPY . /3dbag-pipeline
COPY ./docker/.env /3dbag-pipeline/.env

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME

RUN apt-get install -y libgdal-dev
RUN python3 -m pip install --no-cache-dir /3dbag-pipeline/packages/party_walls

# Run dagster gRPC server on port 4002

EXPOSE 4002

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4002", "-m", "bag3d.party_walls.code_location", "--inject-env-vars-from-instance"]
