# Changelog
All notable changes to the 3dbag-pipeline are documented in this file.
For the changes in the 3DBAG data set, see the [3DBAG release notes](https://docs.3dbag.nl/en/overview/release_notes/).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [2024.12.16]

Release that produced the 3DBAG data set version 2024.12.16.

### Added
- Documentation for deploying the 3dbag-pipeline, contributor guidelines and the project layout.
- Docker-based deployment.
- CI pipeline for testing and docker image builds.

### Changed
- Major refactoring of the project structure for easier maintenance and deployment.

### Docker images

The docker images for this release:
- `3dgi/3dbag-pipeline-dagster:2024.12.16`
- `3dgi/3dbag-pipeline-core:2024.12.16`
- `3dgi/3dbag-pipeline-floors-estimation:2024.12.16`
- `3dgi/3dbag-pipeline-party-walls:2024.12.16`
