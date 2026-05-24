format:
    uv run ruff format .

lint:
    uv run ruff check .

typecheck:
    uv run pyright

test:
    uv run pytest

build:
    uv build

check:
    uv run ruff format --check .
    uv run ruff check .
    uv run pyright
    uv run pytest
    uv build

all:
    just format
    just check
