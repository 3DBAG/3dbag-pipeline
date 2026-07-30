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

### Docker image digests

An image digest is the registry-published SHA-256 content identifier for an OCI image manifest (or, for a multi-platform image, its manifest index). It is calculated by the registry/client as the
SHA-256 hash of that OCI manifest content; it is not computed by this repository, is not a hash of the Dockerfile, and is not a Git commit ID. When updating a Docker-delivered tool, obtain the digest
for the intended published image from the registry, for example with `docker buildx imagetools inspect <repository>:<tag>`, and record it in `image.digest`.

The tag in `repository:version` is readable but can be moved by the publisher. Appending `@sha256:...` makes Docker pull the exact manifest identified by that digest, even if the tag later points
elsewhere. The tag and digest are therefore updated together: the tag explains the intended release/branch name, and the digest provides reproducibility.

### Git references and commit pinning

The manifest does not currently model Git commit-ish values. Its top-level `repository` fields are descriptive metadata, and `tools.<name>.version` is passed either as an image tag or as the version
argument expected by the existing source-build script; neither field is interpreted as a Git branch, tag, or commit by `manifest_versions.py`.

For Docker-delivered tools, a branch-like image tag such as roofer's `develop` is still reproducible because the accompanying OCI digest locks the exact published image content. It does **not**
prove or record the Git commit from which that image was built. For source-built tools, the present system has no Git-ref-to-commit lock: reproducibility depends on the release URL or any Git
reference already hard-coded in the source build script. A branch named as a manifest `version` would not be resolved and pinned to a commit automatically. Adding that capability would require a
separate manifest field for an immutable commit SHA and build-script support to clone or download that exact revision.

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

### Resolver commands

Every command first loads and validates the manifest, so malformed versions, missing image fields, and non-SHA-256 Docker digests fail before values are consumed.

| Command                          | Why it is needed                                                                                                                                                                                                                                                                      |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `validate`                       | Checks the manifest itself without inspecting generated files. Use it after editing manifest-owned values to catch invalid structure, pipeline/tools-image version formats, and Docker digest syntax.                                                                                 |
| `check`                          | Runs validation and confirms that derived Python package metadata and local-package entries in `uv.lock` still match `version`. It is used by `make lint` and CI to prevent committing or publishing version drift.                                                                   |
| `sync`                           | Writes the manifest pipeline version to the repository's `pyproject.toml` files and updates local-package versions in `uv.lock` using uv's normalized PEP 440 form. Run it after changing `version`; it does not resolve or upgrade third-party dependencies.                         |
| `env`                            | Produces the complete tools and pipeline image references derived from the manifest. `make` consumes its default format, Compose receives the exported values, `--format shell` supports direct Compose use, and CI uses `--format build-args` for downstream pipeline Docker builds. |
| `build-args`                     | Produces the tools-image build arguments: immutable `repository:version@digest` references for Docker-delivered tools and version arguments for source-built tools. This is the only path used by Make and CI to supply those values before Docker evaluates `FROM`.                  |
| `value pipeline` / `value tools` | Returns the one version needed where a scalar is required: respectively the release-tagged pipeline image version or independently versioned tools-image tag. Release and publishing workflows use this instead of parsing JSON themselves.                                           |

### CI and release behavior

- The tools-image workflow runs on changes to its build context merged to `develop`, or on manual dispatch. It validates synchronized manifest metadata, passes manifest-derived build arguments to
  Buildx, and publishes the complete `BAG3D_TOOLS_IMAGE` reference. Release tags do not rebuild the tools image.
- The pipeline-image workflow accepts only vYYYY.MM.DD tags, requires the tag to match manifest.version, and publishes core, floors-estimation, party-walls, export, and Dagster images using their
  complete
  `BAG3D_*_IMAGE` resolver outputs.
- The release workflow runs sync and check, commits both package metadata and lockfile updates, then creates the matching Git tag and release.
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
