# 3DBAG pipeline

Repository of the 3DBAG production pipeline, shortly known as `3dbag-pipeline`.
The `3dbag-pipeline` is built with the [Dagster](https://dagster.io) data orchestration tool, therefore some familiarity with Dagster is recommended.

![dagster](images/dagster.png)
*Screenshot of the 3dbag-pipeline in the dagster UI.*

Dagster, and thus the 3dbag-pipeline is written in Python.
However, almost all data processing is done by external tools, and the 3dbag-pipline orchestrates and configures the operation of these tools.

## Documentation

You will find the complete documentation at [https://innovation.3dbag.nl/3dbag-pipeline](https://innovation.3dbag.nl/3dbag-pipeline).

## Deployment

Deploying the 3dbag-pipeline is complex, because of its many components and dependencies.
Only the Linux OS is supported (tested on **Ubuntu** 22.04, 24.04), and we don't have plans to support any other OS.
The required **Python** versions are **3.11 and 3.12**.

### Docker

We provide a sample docker compose file for deploying the 3dbag-pipeline in a multi-container setup.
You can read more about the [docker-based deployment here](docker).

### Local

In order to deploy the 3dbag-pipeline locally, as a service, you need to make sure that all the required tools are installed.
We don't recommend doing this.
However, the [local development setup](development/code/#local-setup) and the [dagster deployment guide](https://docs.dagster.io/deployment/guides/service) can give you some hints on how to deploy the 3dbag-pipeline as a service.

## Packages

The 3dbag-pipeline are organized into several packages.
The packages are organized into a `common` package and a number of workflow packages.
The `common` package contains the resources, functions and type definitions that are used by the 3DBAG packages that define the data processing workflows.
The workflow packages contain the assets, jobs, sensors etc. that define a data processing workflow for a part of the complete 3DBAG.

The reason for this package organization is that workflow packages have widely different dependencies, and installing them into the same environment bound to lead to dependency conflicts.
Additionally, this organization makes it easier to install and test the workflow packages in isolation.

- [`common`](index_common.md): The common package used by the workflow packages.
- [`core`](index_core.md): Workflow for producing the core of the 3D BAG data set.
- [`party_walls`](index_party_walls.md): Workflow for calculating the party walls.
- [`floors-estimation`](index_floors_estimation.md): Workflow for estimating the number of floors.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
