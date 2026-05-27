# Dispatchbus Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the project from `dispatchr` to `dispatchbus` everywhere in the repository with a clean break.

**Architecture:** This is a package-rename refactor with no intended runtime behavior changes. The work should proceed by first updating tests and package metadata expectations, then renaming the source package and internal imports, then updating docs and repository references, and finally verifying no active `dispatchr` references remain.

**Tech Stack:** Python 3.12, uv, pytest, pyright, ruff, Hatchling

---

## File map

### Source files to move or modify
- Move: `src/dispatchr/` → `src/dispatchbus/`
- Modify after move:
  - `src/dispatchbus/__init__.py`
  - `src/dispatchbus/__main__.py`
  - `src/dispatchbus/bus.py`
  - `src/dispatchbus/command_dispatch.py`
  - `src/dispatchbus/context.py`
  - `src/dispatchbus/debug.py`
  - `src/dispatchbus/event_publisher.py`
  - `src/dispatchbus/exceptions.py`
  - `src/dispatchbus/lifecycle.py`
  - `src/dispatchbus/messages.py`
  - `src/dispatchbus/middleware.py`
  - `src/dispatchbus/observability.py`
  - `src/dispatchbus/registry.py`
  - `src/dispatchbus/runtime.py`
  - `src/dispatchbus/sync_bridge.py`
  - `src/dispatchbus/outbox/__init__.py`
  - `src/dispatchbus/outbox/models.py`
  - `src/dispatchbus/outbox/processor.py`
  - `src/dispatchbus/outbox/serializers.py`
  - `src/dispatchbus/outbox/sqlite.py`

### Project metadata and docs
- Modify:
  - `pyproject.toml`
  - `README.md`
  - `docs/roadmap.md`
  - `docs/specs/2026-05-25-transactional-outbox-design.md`
  - `docs/specs/2026-05-25-enterprise-roadmap-design.md`
  - `docs/plans/2026-05-25-transactional-outbox-plan.md`
  - `docs/plans/2026-05-25-idempotency-plan.md`

### Tests
- Modify:
  - `tests/unit/test___init__.py`
  - `tests/unit/test_sync_bridge.py`
  - `tests/unit/test_registry.py`
  - `tests/unit/test_outbox_serializers.py`
  - `tests/unit/test_outbox_protocols.py`
  - `tests/unit/test_outbox_processor.py`
  - `tests/unit/test_observability.py`
  - `tests/unit/test_messages.py`
  - `tests/unit/test_lifecycle.py`
  - `tests/unit/test_event_publisher.py`
  - `tests/unit/test_debug.py`
  - `tests/unit/test_command_dispatch.py`
  - `tests/unit/test_bus.py`
  - `tests/integration/test_outbox_sqlite_int.py`
  - `tests/integration/test_bus_int.py`

### Verification targets
- Search for remaining `dispatchr` references with `rg -n "dispatchr" .`
- Run `uv run pytest`
- Run `uv run pyright`

### Task 1: Update metadata and test expectations for the new name

**Files:**
- Modify: `pyproject.toml`
- Modify: `tests/unit/test___init__.py`
- Modify: `README.md`

- [ ] **Step 1: Write the failing tests/expectations for exported package naming and metadata references**

```python
# tests/unit/test___init__.py
# Update any imports like:
from dispatchbus import MessageBus

# Keep existing public API assertions unchanged aside from import path.
```

```toml
# pyproject.toml
[project]
name = "dispatchbus"

[project.urls]
Homepage = "https://github.com/kevin-rieck/dispatchbus"
Repository = "https://github.com/kevin-rieck/dispatchbus.git"
Issues = "https://github.com/kevin-rieck/dispatchbus/issues"

[project.scripts]
dispatchbus = "dispatchbus.__main__:main"

[tool.hatch.build.targets.wheel]
packages = ["src/dispatchbus"]
```

```markdown
# README.md snippets to update
# dispatchbus
`dispatchbus` is an in-memory Python message bus...
uv add dispatchbus
pip install dispatchbus
pip install 'dispatchbus[sqlite]'
from dispatchbus import CommandBase, EventBase, MessageBus
from dispatchbus.outbox import JSONSerializer, OutboxProcessor, OutboxRetentionPolicy, OutboxWorker
from dispatchbus.outbox import SQLiteOutboxStorage
```

- [ ] **Step 2: Run the targeted test to verify imports fail before the package move**

Run: `uv run pytest tests/unit/test___init__.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dispatchbus'`

- [ ] **Step 3: Apply the metadata and README changes**

```toml
# pyproject.toml exact target state
[build-system]
requires = ["hatchling>=1.25.0"]
build-backend = "hatchling.build"

[project]
name = "dispatchbus"
version = "0.1.0"
description = "Dispatchbus Python project."
readme = "README.md"
requires-python = ">=3.12"
authors = [
  { name = "Kevin Rieck" },
]
license = "Apache-2.0"
classifiers = [
  "Programming Language :: Python :: 3",
  "Programming Language :: Python :: 3.12",
  "License :: OSI Approved :: Apache Software License",
  "Operating System :: OS Independent",
]
dependencies = []

[project.urls]
Homepage = "https://github.com/kevin-rieck/dispatchbus"
Repository = "https://github.com/kevin-rieck/dispatchbus.git"
Issues = "https://github.com/kevin-rieck/dispatchbus/issues"

[project.scripts]
dispatchbus = "dispatchbus.__main__:main"

[dependency-groups]
dev = [
  "pytest>=9.0.0",
  "pytest-asyncio>=1.3.0",
  "ruff>=0.11.0",
  "pyright>=1.1.0",
  "rich>=15.0.0",
]

[tool.ruff]
line-length = 100
target-version = "py312"
src = ["src"]

[tool.ruff.lint]
select = [
  "E",
  "F",
  "I",
  "B",
  "UP",
]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
line-ending = "lf"

[tool.pyright]
pythonVersion = "3.12"
include = ["src"]

[tool.hatch.build.targets.wheel]
packages = ["src/dispatchbus"]

[project.optional-dependencies]
sqlite = ["aiosqlite>=0.20.0"]
```

- [ ] **Step 4: Re-run the targeted test**

Run: `uv run pytest tests/unit/test___init__.py -v`
Expected: still FAIL with `ModuleNotFoundError` until the source package move is completed

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml README.md tests/unit/test___init__.py
git commit -m "build: rename package metadata to dispatchbus"
```

### Task 2: Move the source package and update internal imports

**Files:**
- Move: `src/dispatchr/` → `src/dispatchbus/`
- Modify: all Python files under `src/dispatchbus/`

- [ ] **Step 1: Write the failing import expectation by updating a representative integration test import**

```python
# tests/integration/test_bus_int.py
from dispatchbus import MessageBus
```

- [ ] **Step 2: Run the representative test before the package move**

Run: `uv run pytest tests/integration/test_bus_int.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dispatchbus'`

- [ ] **Step 3: Move the package directory**

```bash
move src\dispatchr src\dispatchbus
```

- [ ] **Step 4: Update all internal imports and package docstrings**

```python
# src/dispatchbus/__init__.py
"""Dispatchbus package."""

from dispatchbus.bus import MessageBus
from dispatchbus.exceptions import (
    BusUsageError,
    DispatchbusError,
    DuplicateCommandHandlerError,
    EventPublicationError,
    HandlerRegistrationError,
    NoCommandHandlerError,
)
from dispatchbus.messages import (
    CommandBase,
    EventBase,
    MessageBase,
    MessageMetadata,
    derive_child_metadata,
    get_metadata,
    new_root_metadata,
)
from dispatchbus.observability import (
    DispatchFinished,
    DispatchStarted,
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
)
```

```python
# For every Python file under src/dispatchbus/
# replace import prefixes:
from dispatchr. -> from dispatchbus.
import dispatchr. -> import dispatchbus.
```

```python
# Rename the base package exception if defined as a public symbol
class DispatchbusError(Exception):
    ...
```

- [ ] **Step 5: Run the package API test**

Run: `uv run pytest tests/unit/test___init__.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/dispatchbus tests/integration/test_bus_int.py
git commit -m "refactor: rename source package to dispatchbus"
```

### Task 3: Update all remaining test imports and behavior checks

**Files:**
- Modify all files under `tests/unit/` and `tests/integration/`

- [ ] **Step 1: Update all test imports from `dispatchr` to `dispatchbus`**

```python
# Representative patterns to apply across tests
from dispatchbus import MessageBus
from dispatchbus.debug import DebugSubscriber
from dispatchbus.outbox import JSONSerializer, OutboxProcessor, OutboxWorker
from dispatchbus.outbox import SQLiteOutboxStorage
```

- [ ] **Step 2: Run the full test suite to surface any missed references**

Run: `uv run pytest -q`
Expected: FAIL with one or more remaining `dispatchr` references or renamed symbols

- [ ] **Step 3: Fix any renamed public symbol references if needed**

```python
# If tests reference the old base exception name, update to:
from dispatchbus.exceptions import DispatchbusError
```

- [ ] **Step 4: Re-run the full test suite**

Run: `uv run pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests
git commit -m "test: rename imports to dispatchbus"
```

### Task 4: Update repository-facing docs and historical docs

**Files:**
- Modify: `README.md`
- Modify: `docs/roadmap.md`
- Modify: `docs/specs/2026-05-25-transactional-outbox-design.md`
- Modify: `docs/specs/2026-05-25-enterprise-roadmap-design.md`
- Modify: `docs/plans/2026-05-25-transactional-outbox-plan.md`
- Modify: `docs/plans/2026-05-25-idempotency-plan.md`

- [ ] **Step 1: Update textual references and code examples from `dispatchr` to `dispatchbus`**

```markdown
# Apply these replacements wherever they refer to the current project:
- dispatchr -> dispatchbus
- `dispatchr` -> `dispatchbus`
- from dispatchr import ... -> from dispatchbus import ...
- dispatchr[sqlite] -> dispatchbus[sqlite]
- github.com/kevin-rieck/dispatchr -> github.com/kevin-rieck/dispatchbus
```

- [ ] **Step 2: Run a repository search limited to docs to confirm the remaining references**

Run: `rg -n "dispatchr" README.md docs`
Expected: either no matches, or only deliberately retained historical mention(s) that are reviewed and accepted

- [ ] **Step 3: Clean up any remaining active project references**

```markdown
# Ensure docs describe the package only as dispatchbus.
```

- [ ] **Step 4: Re-run the docs search**

Run: `rg -n "dispatchr" README.md docs`
Expected: no active project references remain

- [ ] **Step 5: Commit**

```bash
git add README.md docs
git commit -m "docs: rename project references to dispatchbus"
```

### Task 5: Final formatting, search, and verification

**Files:**
- Modify if needed: any files changed by formatting

- [ ] **Step 1: Run formatting**

Run: `uv run ruff format .`
Expected: files reformatted if needed

- [ ] **Step 2: Run a repository-wide search for the old name**

Run: `rg -n "dispatchr" .`
Expected: no matches, or only intentionally preserved historical context that has been reviewed

- [ ] **Step 3: Run tests**

Run: `uv run pytest`
Expected: PASS

- [ ] **Step 4: Run type checking**

Run: `uv run pyright`
Expected: `0 errors, 0 warnings, 0 informations`

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "chore: finalize dispatchbus rename"
```

## Self-review

- Spec coverage: package rename, metadata rename, CLI rename, docs/examples rename, repo URL rename, tests/imports rename, and verification are all covered.
- Placeholder scan: removed TBD/TODO language; each task has explicit files and commands.
- Type consistency: the plan assumes the package-level exception may also be renamed from `DispatchrError` to `DispatchbusError`; verify this during execution and update all references consistently.
