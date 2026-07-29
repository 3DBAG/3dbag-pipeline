# ADR 0005: Manifest-Owned Versioning and Docker Image Resolution

**Status:** Accepted **Date:** 2026-07-29

## Context

The repository previously carried the same version in the manifest, Python package metadata and lockfiles, Dockerfiles, Compose configuration, and CI. The old checker could report drift, but Docker
and release workflows could still choose a different version. Docker also resolves a FROM instruction before it can read a JSON file copied into a build stage.

## Decision

3dbag-manifest.json is the sole write authority for pipeline, tool, and 3DBAG Docker image versions. scripts/manifest_versions.py is the only supported translation layer between the manifest and its
consumers.

### Manifest contract

| Field                                                                                    | Meaning                                                                                       |
|------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| version                                                                                  | Pipeline release version in YYYY.MM.DD format; also the tag for all released pipeline images. |
| images.tools                                                                             | Repository and independently managed tag of the tools base image.                             |
| images.core, images.floors-estimation, images.party-walls, images.export, images.dagster | Published image repositories; their tags derive from version.                                 |
| tools.<name>.version                                                                     | Version of a tool installed in the tools image.                                               |
| tools.<docker-tool>.image                                                                | Docker repository and immutable SHA-256 digest.                                               |
| pipeline                                                                                 | Pipeline stage/job metadata; not version-resolution input.                                    |

Docker-delivered tools (uv, tyler, tyler-db, roofer, and cjval) require a digest. The resolver creates repository:version@digest references. Source-built tools provide version build arguments. The
tools image has a separate tag because it can be published before a pipeline release.

### Resolution flow

    3dbag-manifest.json
            |
            +-- validate/check --> schema and generated-metadata verification
            +-- sync -----------> pyproject.toml and uv.lock files
            +-- env ------------> Make, Compose, and downstream Docker build arguments
            +-- build-args -----> docker/tools/Dockerfile arguments
            +-- value ----------> release and publishing workflow tags

Dockerfiles receive ARG values before FROM and contain no tool-image or tools-base version. Make resolves the manifest environment before invoking Compose. Direct Compose users can run:

    eval "$(python3 scripts/manifest_versions.py env --format shell)"

BAG3D_DOCKER_IMAGE_TAG is no longer supported as an override.

### CI and release behavior

- The tools-image workflow validates the manifest, passes build-args to Buildx, and publishes the repository/tag in images.tools.
- The pipeline-image workflow accepts only vYYYY.MM.DD tags, requires the tag to match manifest.version, and publishes core, floors-estimation, party-walls, export, and Dagster images with that
  version.
- The release workflow runs sync and check, then creates the matching Git tag and release.
- make lint runs check, so derived metadata drift fails locally and in CI.

## Operational procedure

### Update a source-built tool

1. Edit its tools entry in the manifest.
2. Run make sync_versions.
3. Run python3 scripts/manifest_versions.py check and manually build the tools image.

### Update a Docker-delivered tool

1. Update version, image.repository, and image.digest together.
2. If a new tools image is needed, set a new images.tools.version.
3. Publish the tools image. Later pipeline releases use that base-image tag from the manifest.

### Release the pipeline

1. Edit only manifest.version and update the changelog.
2. Run make sync_versions; this updates all package metadata and local-package lockfile entries.
3. Run python3 scripts/manifest_versions.py check.
4. Start the release workflow. It creates v<manifest.version>, which triggers publication of all pipeline images with the same version.

## Consequences

- Every version edit has one authoritative location and a mechanically checked propagation path.
- External Docker dependencies are reproducible because a readable tag is paired with a digest.
- Local Compose, CI, image labels, Python distributions, lockfiles, and Git releases agree on one pipeline version.
- The tools image may advance separately, but its selected tag is explicitly recorded.
- Edits to derived package versions or lockfiles are overwritten by sync and rejected by check.

## Rejected Alternatives

### Keep Dockerfile versions and only check for drift

Rejected because checking does not make Docker consume the checked value.

### Read the manifest inside Docker builds

Rejected because Docker resolves FROM before build-stage files are available.

### Keep arbitrary local image-tag overrides

Rejected because they produce deployments that cannot be reconstructed from repository state.

## Related Files

| File                                        | Role                                                                            |
|---------------------------------------------|---------------------------------------------------------------------------------|
| 3dbag-manifest.json                         | Authoritative versions, image repositories/digests, and pipeline stage metadata |
| scripts/manifest_versions.py                | Validation, synchronization, and consumer-specific output                       |
| makefile                                    | Manifest-derived Compose environment and sync/check integration                 |
| docker/tools/Dockerfile                     | Immutable external-tool build arguments                                         |
| docker/compose.yaml                         | Manifest-derived pipeline image names and tools base reference                  |
| .github/workflows/build-docker-develop.yaml | Release-tagged pipeline image publishing                                        |
| .github/workflows/build-docker-tools.yaml   | Independently versioned tools image publishing                                  |
| .github/workflows/release.yaml              | Metadata synchronization and manifest-matching Git release                      |
