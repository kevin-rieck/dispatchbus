# Rename `dispatchr` to `dispatchbus`

## Goal

Rename the project from `dispatchr` to `dispatchbus` everywhere in this repository as a clean break, including code-facing, packaging-facing, CLI-facing, documentation-facing, and repository-facing references.

## Scope

The rename applies to:

- Python package/import path
- Distribution/package metadata in `pyproject.toml`
- Console script entry points
- README and usage examples
- Repository URLs and references
- Tests and internal imports
- Any remaining textual references that are intended to describe this project

The rename does not include a backwards-compatibility shim. `dispatchr` should stop being the active package/import name after this change.

## Approach options considered

### Option 1: Full source/package rename (recommended)

Rename the source package directory from `src/dispatchr` to `src/dispatchbus` and update all metadata, imports, docs, and references to match.

**Pros**
- Consistent naming everywhere
- Matches the desired clean break
- Avoids long-term alias maintenance

**Cons**
- Requires touching code, packaging, docs, and tests together
- Is a breaking change for existing users

### Option 2: Compatibility alias transition

Publish as `dispatchbus` while preserving `dispatchr` imports temporarily.

**Pros**
- Easier migration path for existing users

**Cons**
- Conflicts with the requested clean break
- Adds temporary maintenance and ambiguity

### Option 3: Branding-only rename

Rename docs and package metadata, but keep the import path as `dispatchr`.

**Pros**
- Lowest code churn

**Cons**
- Not a true rename everywhere
- Leaves a confusing mismatch between package name and import path

## Recommended design

Use Option 1 and perform a repository-wide rename to `dispatchbus`.

## Planned changes

### Package and source layout

- Rename `src/dispatchr` to `src/dispatchbus`
- Update internal imports from `dispatchr...` to `dispatchbus...`
- Ensure wheel packaging points at `src/dispatchbus`

### Packaging and CLI

Update `pyproject.toml` to rename:

- `[project].name` from `dispatchr` to `dispatchbus`
- repository URLs from `dispatchr` to `dispatchbus`
- console script name from `dispatchr` to `dispatchbus`
- script target module from `dispatchr.__main__` to `dispatchbus.__main__`
- optional dependency install examples and any package-name references in docs

### Documentation and examples

Update repository-facing and user-facing references, including:

- project title and description text where appropriate
- install commands such as `uv add dispatchbus` and `pip install dispatchbus`
- optional extra examples such as `dispatchbus[sqlite]`
- import examples such as `from dispatchbus import ...`
- any GitHub links that include the old repository name

### Tests and tooling alignment

Update tests and any configuration that references the old package name so that:

- imports target `dispatchbus`
- source roots and packaging targets remain correct
- project checks continue to pass under the renamed package

## Data flow and runtime behavior

This is a naming and packaging refactor only. Runtime semantics should remain unchanged:

- public APIs keep the same structure
- behavior of command dispatch, event publication, middleware, observability, and outbox support should not change
- only names and references change

## Error handling and migration expectations

Because this is a clean break:

- old imports like `import dispatchr` are expected to fail after the rename
- old CLI invocations like `dispatchr` are expected to fail after the rename
- documentation should clearly show only the new names to avoid mixed guidance

## Testing and verification

After the rename:

- run formatting if edits require it
- run `uv run pytest`
- run `uv run pyright`
- optionally run a repository search for any remaining `dispatchr` references and review whether any should remain

## Risks

- Missing a textual or import reference can leave the project partially renamed
- Entry-point or packaging metadata may break if source-path updates are incomplete
- Some historical docs may still mention `dispatchr`; these should be reviewed and updated if still relevant to current users

## Success criteria

The work is successful when:

- the source package is `dispatchbus`
- project metadata publishes as `dispatchbus`
- CLI entry points use `dispatchbus`
- docs and examples consistently use `dispatchbus`
- tests and type checks pass
- no active repository references to `dispatchr` remain except where intentionally retained in historical context
