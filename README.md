# dispatchr

Minimal Python project scaffolded for modern tooling:

- `uv` for environment management, dependency installation, locking, and builds
- `ruff` for linting and formatting

## Quick start

```powershell
uv sync --dev
uv run ruff check .
uv run ruff format .
uv run dispatchr
uv build
```
