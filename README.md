# 3D BAG production pipeline

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

First, install your Dagster repository as a Python package. By using the --editable flag, pip will install your repository in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagit web server with the `$DAGSTER_HOME` set to `tests/dagster_home`.
This is the directory where dagster stores its data (logs, run history etc.) when running locally from this repository.

```bash
export DAGSTER_HOME=<absolute path to tests/dagster_home>
# execute from repository root, because the path in workspace.yaml is relative to it
dagit --workspace <absolute path to tests/dagster_home/workspace.yaml>
```

In a separate shell, start the Dagster daemon so that sensors work too.

```bash
export DAGSTER_HOME=<absolute path to tests/dagster_home>
# execute from repository root, because the path in workspace.yaml is relative to it
dagster-daemon run --workspace <absolute path to tests/dagster_home/workspace.yaml>
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `bag3d_pipeline/assets/`. The assets are automatically loaded into the Dagster repository as you define them.
Except, if you create a new sub-package under `assets`.
In this case you'll need to add the new asset-sub-package manually.

## Development


### Adding new Python dependencies

You can specify new Python dependencies in `pyproject.toml`.

### Unit testing

Tests are in the `tests` directory, and you can run tests using `pytest`:

```bash
pytest tests
```

The tests use the sample data from the `3dbag-sample-data` docker image.
A container is created from this image and bind-mounted on a new temporary directory, which is populated with the sample data from the image.
The tests then either use the data from this temporary directory or from the database in the container.
See the `tests/conftest.py` on how this is set up.

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, start the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process in the same folder as your `workspace.yaml` file, but in a different shell or terminal.

The `$DAGSTER_HOME` environment variable must be set to a directory for the daemon to work. Note: using directories within /tmp may cause issues. See [Dagster Instance default local behavior](https://docs.dagster.io/deployment/dagster-instance#default-local-behavior) for more details.

In this repository the `$DAGSTER_HOME` is in `tests/dagster_home`.

```bash
export DAGSTER_HOME=<absolute path to tests/dagster_home>
dagster-daemon run
```

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## GraphQL API

Dagster has a GraphQL API and it is served alongside the Dagit web server at `/graphql` (eg `http://localhost:3000/graphql`).
One can do basically everything that is doable in the Dagit UI.
Retrieve data on assets, runs etc., but also launch runs.

This query to get the asset materializations metadata and asset dependencies (lineage):

```
{
  assetNodes(
    group: {
      groupName: "source"
      repositoryName: "bag3d"
      repositoryLocationName: "dev_3dbag_pipeline_py38_venv"
    }
    pipeline: {
      pipelineName: "source_and_input_sample"
      repositoryName: "bag3d"
      repositoryLocationName: "dev_3dbag_pipeline_py38_venv"
    }
    # assetKeys: { path: ["top10nl", "stage_top10nl_gebouw"] }
    loadMaterializations: true
  ) {
    assetKey {
      path
    }
    dependencies {
      asset {
        assetKey{path}
      }
    }
    assetMaterializations(limit: 1) {
      runId
      assetLineage {
        assetKey {
          path
        }
        partitions
      }
      metadataEntries {
        label
        description
        __typename
        ... on TextMetadataEntry {
          text
        }
        __typename
        ... on IntMetadataEntry {
          intValue
        }
      }
    }
  }
}
```

## Source datasets

### BAG

**Definitions**

BAG – Basisregistratie Adressen en Gebouwen. Deze bevat alle BAG gegevens zoals ingewonnen door de BAG bronhouders, conform de [Official BAG specifications (BAG Catalogus 2018)](https://www.geobasisregistraties.nl/documenten/publicatie/2018/03/12/catalogus-2018).

LVBAG – De Landelijke Voorziening BAG, die de gegevens van de BAG overneemt naar en landelijk beschikbaar stelt.

**Links**

[The BAG Extract 2.0 documentation and links](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract)

[BAG object history documentation](https://www.kadaster.nl/-/specificatie-bag-historiemodel)

[Official BAG specifications (BAG Catalogus 2018)](https://www.geobasisregistraties.nl/documenten/publicatie/2018/03/12/catalogus-2018)

[BAG-API GitHub repo](https://github.com/lvbag/BAG-API)

[Official BAG viewer](https://bagviewer.kadaster.nl/lvbag/bag-viewer/)

[BAG quality dashboard](https://www.kadaster.nl/zakelijk/registraties/basisregistraties/bag/bag-voor-afnemers/bag-kwaliteitsdashboard-voor-afnemers)

*What is the difference between the LVBAG and the BAG?*

As far as I understand, the LVBAG is a central database of the BAG that is maintained by Kadaster and where the municipalities send their BAG data.
That is because each municipality is responsible for their own BAG data, but they have to contribute to the LVBAG. [link](https://www.kadaster.nl/zakelijk/registraties/basisregistraties/bag)

The *BAG Extract* is a periodic extract from the LVBAG, created by the Kadaster.

The BAG is distributed in two manners, through the extracts (Extractlevering or Standard Levering) and the mutations (Mutatielevering).
There are daily and monthly extracts and mutations, per municipality and for the whole country.
To access any of these requires a subscription, except the monthly national extract.
The monthly national extract is available for free, without a subscription.
The monthly extracts and mutations are release on the 8th of each month.

Technically, we could keep our BAG database up-to-date by processing the monthly mutations (Maandmutaties heel Nederland).
But the mutations are only available through a subscription.

Therefore, we need to drop and recreate our BAG tables from the national extract each time we update the data.
In fact, this is one of the recommended methods in the [Functioneele beschrijving mutatiebestaanded](https://www.kadaster.nl/-/functionele-beschrijving-mutatiebestanden) documentation: *"Het actualiseren van de lokaal ingerichte database kan door middel van het maandelijks inladen van een volledig BAG 2.0 Extract of door het verwerken van mutatiebestanden."*

We can reconstruct the BAG input at any give time (Ts) by selecting on `begingeldigheid <= Ts <= eindgeldigheid`.

The `oorspronkelijkbouwjaar` is not an indicator of a change in the geometry.

## Deployment

The deployment configurations are managed with the `DAGSTER_DEPLOYMENT` environment variable.
Possible values are in `bag3d_pipeline.repository`.

RESOURCES_LOCAL/PYTEST/PROD ?