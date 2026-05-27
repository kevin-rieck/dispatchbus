# Prepare `dispatchbus` Alpha Release `0.1.0a1`

## Goal

Prepare the repository for the first public alpha release of `dispatchbus` as version `0.1.0a1`, create a dedicated release-prep commit, and create the annotated git tag `v0.1.0a1`.

## Scope

This release-prep work includes:

- keeping `dispatchbus` as the released package name
- changing package versioning from `0.1.0` to `0.1.0a1`
- marking the release clearly as alpha in package metadata and docs
- adding in-repo release notes/changelog content for `0.1.0a1`
- verifying test, type-check, and build readiness locally
- creating one release-prep commit
- creating annotated tag `v0.1.0a1`

This release-prep work does not include:

- publishing to PyPI or TestPyPI
- broad repository restructuring
- resolving the separate `dispatchr` package tree during this release pass unless it blocks release readiness
- introducing new product functionality

## Approach options considered

### Option 1: Minimal alpha release prep (recommended)

Update versioning, alpha signaling, release notes, and verification artifacts without expanding scope into larger codebase cleanup.

**Pros**
- Matches the requested outcome
- Low-risk first public prerelease
- Keeps focus on release readiness

**Cons**
- Leaves broader repository cleanup for later

### Option 2: Alpha release plus packaging cleanup

Prepare the release and also resolve more of the mixed `dispatchbus`/`dispatchr` repository state before tagging.

**Pros**
- Cleaner repository state at release time

**Cons**
- Higher scope and higher release risk
- Can delay the alpha with non-essential cleanup

### Option 3: Docs-only alpha framing

Keep code changes minimal and rely mostly on documentation, release notes, and tagging.

**Pros**
- Smallest possible change set

**Cons**
- Risks shipping avoidable metadata or packaging rough edges

## Recommended design

Use Option 1.

Treat this as release-engineering work for an early prerelease. The repository should truthfully communicate that `dispatchbus` is usable enough for an alpha, but still unstable and subject to change.

## Planned changes

### Packaging metadata

Update `pyproject.toml` so the published distribution version is `0.1.0a1`.

Also ensure the metadata clearly describes alpha maturity, including an appropriate trove classifier such as:

- `Development Status :: 3 - Alpha`

The package name remains `dispatchbus`, and the wheel target remains `src/dispatchbus`.

### Documentation

Update `README.md` near the top with a brief alpha notice so readers immediately understand the maturity level and expected API instability.

The README should continue to present `dispatchbus` as the canonical package name in install commands, imports, and examples.

### Release notes / changelog

Add a repository-tracked release notes artifact for `0.1.0a1`.

The content should cover:

- what the alpha release is
- key highlights already present in the package
- known limitations
- an explicit warning that breaking changes may still occur
- a short verification summary from release prep

A top-level `CHANGELOG.md` is the best fit for this first release because it is conventional, easy to discover, and can later accumulate additional entries.

### Git operations

After verification passes:

- create one release-prep commit containing the release metadata/docs changes
- create an annotated tag `v0.1.0a1`

The tag message should make clear that this is the first alpha release of `dispatchbus`.

## Data flow and runtime behavior

This work should not intentionally change runtime behavior.

Expected effects are limited to:

- version metadata exposed by the package build artifacts
- release maturity messaging in docs and changelog material
- repository history gaining a release-prep commit and tag

Core bus semantics, outbox behavior, middleware behavior, sync bridge behavior, and public import paths should remain unchanged.

## Error handling and release caveats

Because this is an alpha release:

- API compatibility should not be implied beyond this prerelease snapshot
- the changelog and README should explicitly warn that future releases may include breaking changes
- if verification uncovers a packaging or metadata issue, it should be fixed before tagging
- if verification uncovers unrelated product bugs, only release-blocking issues should be addressed in this pass

## Testing and verification

Before creating the release commit and tag, run:

- `uv run pytest`
- `uv run pyright`
- build the distribution artifacts

Build verification should confirm that:

- the package builds successfully
- the resulting version is `0.1.0a1`
- release-facing metadata still points to `dispatchbus`

If formatting is needed after edits, run Ruff formatting before final verification.

## Risks

- Metadata may still imply a stable release if alpha signaling is incomplete
- Release notes may omit important caveats for early users
- Build artifacts may expose a version mismatch if version changes are only partially applied
- Existing repository clutter such as parallel `dispatchr` content could confuse future cleanup, though it should not block this alpha if packaging stays correct

## Success criteria

The work is successful when:

- `pyproject.toml` publishes `dispatchbus` version `0.1.0a1`
- alpha maturity is visible in package metadata and README
- a changelog or release notes entry for `0.1.0a1` exists in the repo
- `uv run pytest` passes
- `uv run pyright` passes
- distribution artifacts build successfully
- a release-prep commit exists
- annotated tag `v0.1.0a1` exists
