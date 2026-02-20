# ADR 0002: Independent Package Environments over uv Workspace

**Status:** Accepted
**Date:** 2026-02-20

## Context

The monorepo contains four Python packages with a root `pyproject.toml` that holds only
development tooling (ruff, mkdocs, bumpver). Since the root is not a real installable package
and the inter-package relationship is managed via local path sources, the question arises
whether converting to a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) would
simplify dependency management.

A uv workspace would provide a single unified `uv.lock` at the root and automatic resolution
of intra-monorepo dependencies, removing the need for explicit
`{ path = "../common", editable = true }` entries in each package's `[tool.uv.sources]`.

## Decision

Do not convert to a uv workspace. Keep each package under `packages/` as an independent uv
project with its own virtual environment and `uv.lock`.

## Rationale

A uv workspace resolves all member packages into a **single shared virtual environment**.
This is incompatible with the deliberate dependency isolation between packages:

| Package | Conflicting constraints |
|---------|------------------------|
| `bag3d-party-walls` | `numpy~=1.24.4`, `pandas~=2.1.0` |
| `bag3d-floors-estimation` | `pandas~=2.2.3` (requires numpy ≥1.26) |

These packages are separate precisely to avoid such conflicts (noted in the architecture
documentation). Merging them into a single environment would force one set of versions on all
packages, breaking the isolation that the monorepo structure was designed to provide.

Additionally, each package is deployed in its own Docker container with only its own
dependencies installed. Independent lock files ensure each container gets a reproducible,
minimal environment.

## Consequences

- Each package retains its own `uv.lock` and virtual environment.
- The `bag3d-common` path dependency is declared explicitly in each dependent package's
  `[tool.uv.sources]` — a small amount of boilerplate but the correct trade-off.
- Docker builds remain straightforward: each image installs only the relevant package.
- If uv introduces support for per-member isolated environments within a workspace in a
  future release, this decision should be re-evaluated.

## Related Files

| File | Role |
|------|------|
| `packages/common/pyproject.toml` | Shared base package |
| `packages/core/pyproject.toml` | Core workflow, depends on common |
| `packages/floors_estimation/pyproject.toml` | ML workflow, heavy sklearn/pandas deps |
| `packages/party_walls/pyproject.toml` | Geometry workflow, old numpy/pandas pins |
