Activate the virtualenv of the core package.

```shell
cd 3dbag-pipeline
python -m pip install packages/core
```
Dagster loads the env vars from the `.env` file. For some reason it does not see the env vars if I source the `.env` manually and I run dagster from a subdirectory without `.env`.

```shell
 dagster api grpc -h 0.0.0.0 -p 4000 -m bag3d.core.code_location --inject-env-vars-from-instance
```

Activate the virtual environment of the dagster webserver in a separate shell.

```shell
cd 3dbag-pipeline
DAGSTER_HOME=3dbag-pipeline/experiments/dagster_grpc dagster-webserver -w experiments/dagster_grpc/workspace.yaml
```