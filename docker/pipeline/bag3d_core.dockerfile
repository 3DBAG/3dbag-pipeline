FROM 3dgi/3dbag-pipeline-tools:2024.09.24

RUN apt-get install -y python3-venv python3-pip
RUN python3 -m pip install --upgrade setuptools wheel
RUN python3 -m pip install \
    dagster \
    dagster-postgres \
    dagster-docker

WORKDIR /3dbag-pipeline
COPY . /3dbag-pipeline
COPY ./docker/.env /3dbag-pipeline/.env

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME


RUN python3 -m pip install /3dbag-pipeline/packages/core

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "bag3d.core.code_location", "--inject-env-vars-from-instance"]
