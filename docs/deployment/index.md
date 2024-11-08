# Deployment

Deploying the 3dbag-pipeline is complex, because of its many components and dependencies.
Deploying it locally involves compiling several dependencies from source, ensuring that they are correctly linked, installing the 3dbag-pipeline code in virtual environments, configuring the dagster instance and optionally, setting it up as a service.
We have tested the local setup on **Ubuntu** 22.04, 24.04 only, and we don't have plans to explore any other operating systems or versions.
The required **Python** versions for the 3dbag-pipeline code are **3.11** for the `party_walls` package and **3.12** for the other packages.
Instead of the local deployment, we highly recommend that you use Docker for deploying the pipeline.

### Docker

We provide a sample docker compose file for deploying the 3dbag-pipeline in a multi-container setup.
You can read more about the [docker-based deployment here](docker.md).

### Local

In order to deploy the 3dbag-pipeline locally, as a service, you need to make sure that all the required tools are installed.
We don't recommend doing this.
However, the [local development setup](../development/code.md/#local-setup) and the [dagster deployment guide](https://docs.dagster.io/deployment/guides/service) can give you some hints on how to deploy the 3dbag-pipeline as a service.

## Resources

The 3DBAG pipeline is a heavy process that requires a well configured PostgreSQL database. Some instructions for configuring your database can be found here in the following links:

[Resource Consumption](https://www.postgresql.org/docs/10/runtime-config-resource.html)
[Write Ahead Log](https://www.postgresql.org/docs/12/runtime-config-wal.html)
[WAL Configuration](https://www.postgresql.org/docs/12/wal-configuration.html)

Indicatively, here are some specifications of our database setup:

```
shared_buffers = 24GB
max_parallel_workers = 24
max_connections = 150
effective_cache_size = 4GB
effective_io_concurrency = 100
maintenance_work_mem = 2GB
```