FROM 3dgi/3dbag-pipeline-tools:2024.09.24

RUN apt-get install -y python3-pip
RUN python3.11 -m pip install \
    dagster \
    dagster-postgres \
    dagster-docker

WORKDIR /3dbag-pipeline
COPY . /3dbag-pipeline
COPY ./docker/.env /3dbag-pipeline/.env

WORKDIR /3dbag-pipeline
RUN make venvs

WORKDIR /3dbag-pipeline

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "packages/core/src/bag3d/core/code_location.py"]