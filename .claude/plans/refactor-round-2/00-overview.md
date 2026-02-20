# Big refactoring round 2

The goals of this refactoring round are:

1. improve developer efficiency when developing the 3dbag-pipeline,
  - so that iteration is faster,
  - the cognitive load is lower so that
    - the code is easier to understand and overview,
    - picking up development after a long period is easier,
2. improve the reliability of the the 3dbag-pipeline when producing and deploying a new 3DBAG release so that the production does not halt due to functional bugs that could have been detected and fixed with comprehensive testing,
3. streamline the production and deployment process by reducing the fragmentation of the production and deployment setup, and by automating as much as possible

After the refactoring, we will have a setup that is,
- modular,
- independently and easily testable so that developing and testing assets in isolation is possible,
- end-to-end tested all the way to releasing the 3DBAG, 

The refactoring impacts two repositories:

- 3dbag-pipeline (contains the pipeline code and docker setup for running the pipeline)
- 3dbag-admin (contains the setup for deploying the pipeline on our server for testing and production, and the setup for publishing the 3DBAG)

## Improve the docker-based development workflow

Related:

- https://github.com/3DBAG/3dbag-pipeline/issues/206
- https://github.com/3DBAG/3dbag-pipeline/issues/402

## Refactor tests to reduce dependency on test data

We rely on samples of real inputs and intermediate outputs in our unit and integration tests.
The reasons behind this are:

- we cannot effectively mock the real spatial data in a way that would allow us to actually execute the tools in tests  that are fundamental to the pipeline (roofer, tyler, cjval etc.),
- the schema of the input changes (or can change) over time, or there can be errors in the published input data, and we want to detect and react to these changes before going into production.

Therefore, we tried to include everything into our pytest setup (unit and integration tests), with the idea that we can catch almost all errors with the pytest setup.
But this setup requires that we continuously have to maintain the samples of real inputs and intermediate outputs as the 3dbag-pipeline evolves, and in practice this proved to be very error prone, obscure and time consuming.

However, now that we have multi-tier deployment and testing, controlled by the `DAGSTER_DEPLOYMENT` variable (`pytest`, `user`, `pc`, `production`), we can actually separate the different tiers of testing based on what they ought to do and make the iterative development, testing more efficient.

So then testing tiers become, in order of increasing complexity and duration:

- lint
  - format check (ruff)
  - syntax check (ruff)
  - code style check (ruff)
  - type check (mypy)
- pytest: Functional testing of assets, jobs etc. with mocked inputs (mostly or only), run via pytest. Used during  iterative development and in CI. Fast to run. Detects errors in the use of different API-s.
- user: End-to-end testing with real input, with one specific AHN tile. Run on our server (gilfoyle). Takes about one hour to complete. Detects errors in assumptions about the input and schema drift.
- pc: End-to-end testing with real input, with a large area that can surface edge cases in the inputs that we would run into in production. Takes multiple hours to run on our server (gilfoyle).

## CI/CD

### Production-candidate

The `pc-*` branch deploys on gilfoyle on push, after all checks succeed.
Production-candidate test are triggered manually from GitHub with `workflow_dispatch`.
These runs are very long running, I don't know if that could be an issue with GitHub.

### User tests

Any branch can be deployed on gilfoyle and the `user` test initiated manually with `workflow_dispatch`.


## Dockerized publication deployment

Similarly to the dockerized 3dbag-pipline setup, a dockerized publication setup would help us to test the publication and release flow, which is currently untested.
It also allows us to hook it up to CI, as integration tests.

The services we need:

- Caddy (file server and reverse proxy)
  - 3DTiles
  - format downloads
  - metadata
  - tile index
  - Website
  - docs
- Geoserver
  - WFS
  - WMS
- 3dbag-api
- PostgreSQL

## Staging deployment

A login-protected (BasicAuth) staging area where we can test a release fully, with website, docs, webservices and all.
A login implies some password protection on the webservices too, which I'm not sure how to solve.
It would be hosted at `https://dev.3dbag.nl`.

## Improve the documentation

Bring the documentation at `https://innovation.3dbag.nl/3dbag-pipeline/` to a level where it can really serve as the re-starting point when we pick up development after a long period.

