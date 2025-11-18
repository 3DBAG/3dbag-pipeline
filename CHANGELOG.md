# Changelog
All notable changes to the 3dbag-pipeline are documented in this file.
For the changes in the 3DBAG data set, see the [3DBAG release notes](https://docs.3dbag.nl/en/overview/release_notes/).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [2025.11.18]

Minor update.

- **[#324](https://github.com/3DBAG/3dbag-pipeline/pull/354)** - Add lasinfo exe and update lastools to v2.0.4

### Other Updates
- Docker image updates and tool version bumps

### Docker images

The docker images for this release:
- `3dgi/3dbag-pipeline-dagster:2025.11.18`
- `3dgi/3dbag-pipeline-core:2025.11.18`
- `3dgi/3dbag-pipeline-floors-estimation:2025.11.18`
- `3dgi/3dbag-pipeline-party-walls:2025.11.18`

## [2025.09.03]

Release that produced the 3DBAG data set version 2025.09.03.

- **[#324](https://github.com/3DBAG/3dbag-pipeline/pull/324)** - Fix dagster concurrency: Refactored AHN LAZ file download concurrency management using Dagster's tag-based concurrency limits
- **[#316](https://github.com/3DBAG/3dbag-pipeline/pull/316)** - Updated Docker base images and improved projection data handling
- **[#247](https://github.com/3DBAG/3dbag-pipeline/pull/247)** - Deployment and release improvements with Docker-based deployment
- **[#205](https://github.com/3DBAG/3dbag-pipeline/pull/205)** - Added connection and transfer to podzilla server
- GH Action docker startup failure.
- **[#255](https://github.com/3DBAG/3dbag-pipeline/pull/255)** - Remove AHN 200m tiles from pipeline: Major rework to use whole indexed AHN tiles instead of 200m tiles for roofer
- **[#293](https://github.com/3DBAG/3dbag-pipeline/pull/293)** - Updated AHN5 checksums using Het Waterschapshuis index
- **[#273](https://github.com/3DBAG/3dbag-pipeline/pull/273)** - Fixed AHN5 PDAL issue by storing filename even when PDAL fails
- **[#278](https://github.com/3DBAG/3dbag-pipeline/pull/278)** - Fixed metadata storage for tiles without hash matched
- **[#297](https://github.com/3DBAG/3dbag-pipeline/pull/297)** - Refactored and optimized lasindex behavior
- **[#211](https://github.com/3DBAG/3dbag-pipeline/pull/211)** - Added global attribute specifications and validation using 3dbag-specs
- **[#203](https://github.com/3DBAG/3dbag-pipeline/pull/203)** - Added GPKG validation with GEOS
- **[#193](https://github.com/3DBAG/3dbag-pipeline/pull/193)** - Filter "Pand ten onrechte opgevoerd" from BAG
- **[#221](https://github.com/3DBAG/3dbag-pipeline/pull/221)** - Automated 3D Tiles generation with tyler
- **[#231](https://github.com/3DBAG/3dbag-pipeline/pull/231)** - Test and deploy 3dtiles, fix roofer config and validation
- **[#295](https://github.com/3DBAG/3dbag-pipeline/pull/295)** - Improved failing behavior for AHN assets
- **[#261](https://github.com/3DBAG/3dbag-pipeline/pull/261)** - Fixed partition mismatch between AHN and reconstruction assets
- **[#257](https://github.com/3DBAG/3dbag-pipeline/pull/257)** - Handle requests.exceptions.ChunkedEncodingError
- **[#200](https://github.com/3DBAG/3dbag-pipeline/pull/200)** - Allow different thread count per job
- **[#192](https://github.com/3DBAG/3dbag-pipeline/pull/192)** - Fix exe version reporting
- **[#174](https://github.com/3DBAG/3dbag-pipeline/pull/174)** - Updated copyrights and licenses
- **[#197](https://github.com/3DBAG/3dbag-pipeline/pull/197)** - Test that code locations load successfully
- **[#269](https://github.com/3DBAG/3dbag-pipeline/pull/269)** - Updated test data
- **[#148](https://github.com/3DBAG/3dbag-pipeline/pull/148)** - Added reference date configuration for BAG

### Other Updates
- Multiple Docker image updates and tool version bumps
- Formatting, logging improvements, and minor fixes
- Documentation updates
- Dependency upgrades
- Small bug fixes and typo corrections
- Configuration adjustments

### Docker images

The docker images for this release:
- `3dgi/3dbag-pipeline-dagster:2025.09.03`
- `3dgi/3dbag-pipeline-core:2025.09.03`
- `3dgi/3dbag-pipeline-floors-estimation:2025.09.03`
- `3dgi/3dbag-pipeline-party-walls:2025.09.03`

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
