# Pyright Setup Design

## Goal
Set up Pyright as the project's static type checker with a minimal initial configuration.

## Scope
- Add `pyright` to the development dependency group in `pyproject.toml`
- Add a minimal `[tool.pyright]` configuration in `pyproject.toml`
- Update `README.md` to document running Pyright via `uv run pyright`

## Approach
Use the existing single-file project configuration pattern in `pyproject.toml` rather than introducing a separate `pyrightconfig.json`.

## Configuration
- Set Python version to 3.12 to match the project requirement
- Point Pyright at the `src` directory
- Keep type-checking strictness minimal for the initial setup

## Files to Change
- `pyproject.toml`
- `README.md`

## Error Handling
No runtime behavior changes are expected. The main risk is configuration mismatch; this is mitigated by matching the existing Python version and source layout.

## Testing and Verification
- Sync dev dependencies with `uv sync --dev`
- Run `uv run pyright`
- Run `uv run ruff check .`

## Trade-offs
This setup favors quick adoption and low friction over strict early enforcement. It can be tightened later once baseline typing issues are known.
