# dispatchbus 0.1.0a1 Alpha Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prepare the repository for the first public `dispatchbus` alpha release as version `0.1.0a1`, verify release readiness, then create a release-prep commit and annotated tag.

**Architecture:** This is release-engineering work only. Update package metadata and user-facing docs to signal alpha maturity, add a repo-tracked changelog entry for the first release, then verify the project with the existing `uv`/`pytest`/`pyright`/`hatchling` toolchain before creating the release commit and tag.

**Tech Stack:** Python 3.12, uv, Hatchling, Ruff, Pyright, pytest, git

---

### Task 1: Update package metadata for the alpha prerelease

**Files:**
- Modify: `pyproject.toml`
- Test: `pyproject.toml`

- [ ] **Step 1: Change the project version and add the alpha classifier**

Replace the current project metadata block content below:

```toml
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
```

with:

```toml
[project]
name = "dispatchbus"
version = "0.1.0a1"
description = "Dispatchbus Python project."
readme = "README.md"
requires-python = ">=3.12"
authors = [
  { name = "Kevin Rieck" },
]
license = "Apache-2.0"
classifiers = [
  "Development Status :: 3 - Alpha",
  "Programming Language :: Python :: 3",
  "Programming Language :: Python :: 3.12",
  "License :: OSI Approved :: Apache Software License",
  "Operating System :: OS Independent",
]
dependencies = []
```

- [ ] **Step 2: Verify the version string and classifier were updated exactly once**

Run:

```bash
rg -n 'version = "0\.1\.0a1"|Development Status :: 3 - Alpha' pyproject.toml
```

Expected output:

```text
pyproject.toml:<line>:version = "0.1.0a1"
pyproject.toml:<line>:  "Development Status :: 3 - Alpha",
```

- [ ] **Step 3: Commit the metadata update with the doc changes later in a single release-prep commit**

Do not commit yet. This task will be committed together with Tasks 2 and 3 after verification in Task 5.

### Task 2: Add an alpha notice to the README

**Files:**
- Modify: `README.md`
- Test: `README.md`

- [ ] **Step 1: Insert a short alpha notice directly under the opening description**

Add this block immediately after the first descriptive paragraph near the top of `README.md`:

```md
> **Alpha release:** `dispatchbus` is in early prerelease status. Expect rough edges and breaking changes before a stable release.
```

The top of the file should read like this after the edit:

```md
# dispatchbus

`dispatchbus` is an in-memory Python message bus for applications that want explicit command and event dispatch without bringing in a framework.

> **Alpha release:** `dispatchbus` is in early prerelease status. Expect rough edges and breaking changes before a stable release.

It supports:
```

- [ ] **Step 2: Verify the README now exposes the alpha caveat near the top**

Run:

```bash
rg -n "Alpha release|breaking changes before a stable release" README.md
```

Expected output:

```text
README.md:<line:>> **Alpha release:** `dispatchbus` is in early prerelease status. Expect rough edges and breaking changes before a stable release.
```

- [ ] **Step 3: Leave the rest of the README unchanged unless a later verification step proves another release-facing wording issue**

No additional README edits are needed for this release pass if the notice is present and existing install/import examples still use `dispatchbus`.

### Task 3: Add a first changelog entry for 0.1.0a1

**Files:**
- Create: `CHANGELOG.md`
- Test: `CHANGELOG.md`

- [ ] **Step 1: Create `CHANGELOG.md` with the first alpha release entry**

Write this complete file:

```md
# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0a1] - 2026-05-26

First public alpha release of `dispatchbus`.

### Highlights

- in-memory command and event dispatch with explicit handlers
- middleware support around send and publish pipelines
- observability subscribers and debug subscribers for dispatch lifecycle visibility
- async-first APIs with synchronous bridge methods
- optional SQLite outbox support via `dispatchbus[sqlite]`

### Alpha caveats

- this is an early prerelease and breaking changes may still occur
- the API surface should not yet be treated as stable
- the CLI entry point is currently a placeholder

### Known limitations

- in-memory only; no broker, queue, or transport integration
- exact-type handler lookup only; no inheritance-based dispatch
- one command handler per command type
- sync handlers and sync subscribers run in a thread pool

### Release verification

This release was prepared with local verification including:

- `uv run pytest`
- `uv run pyright`
- `uv build`
```

- [ ] **Step 2: Verify the changelog contains the expected release heading and alpha caveat**

Run:

```bash
rg -n '^## \[0\.1\.0a1\]|^### Alpha caveats|^### Release verification' CHANGELOG.md
```

Expected output:

```text
CHANGELOG.md:<line>:## [0.1.0a1] - 2026-05-26
CHANGELOG.md:<line>:### Alpha caveats
CHANGELOG.md:<line>:### Release verification
```

- [ ] **Step 3: Keep the changelog limited to the first release entry**

Do not add historical reconstruction entries. This file should begin with the single `0.1.0a1` release.

### Task 4: Run formatting and repository sanity checks

**Files:**
- Modify: none expected
- Test: `pyproject.toml`, `README.md`, `CHANGELOG.md`

- [ ] **Step 1: Run Ruff formatting to ensure the repo remains formatted**

Run:

```bash
uv run ruff format .
```

Expected output:

```text
<either "1 file reformatted" / similar or "N files left unchanged">
```

- [ ] **Step 2: Confirm the release-facing files contain the intended alpha version and wording**

Run:

```bash
rg -n '0\.1\.0a1|Development Status :: 3 - Alpha|Alpha release|First public alpha release of `dispatchbus`' pyproject.toml README.md CHANGELOG.md
```

Expected output:

```text
pyproject.toml:<line>:version = "0.1.0a1"
pyproject.toml:<line>:  "Development Status :: 3 - Alpha",
README.md:<line:>> **Alpha release:** `dispatchbus` is in early prerelease status. Expect rough edges and breaking changes before a stable release.
CHANGELOG.md:<line>:## [0.1.0a1] - 2026-05-26
CHANGELOG.md:<line>:First public alpha release of `dispatchbus`.
```

- [ ] **Step 3: Review the working tree before verification**

Run:

```bash
git status --short
```

Expected output:

```text
M README.md
M pyproject.toml
?? CHANGELOG.md
```

If additional tracked files changed, inspect them before continuing.

### Task 5: Verify tests, types, and build artifacts

**Files:**
- Modify: none expected
- Test: repository test suite and built artifacts under `dist/`

- [ ] **Step 1: Run the full test suite**

Run:

```bash
uv run pytest
```

Expected output:

```text
============================= test session starts =============================
...
============================== <N> passed in <time> ==============================
```

- [ ] **Step 2: Run static type checking**

Run:

```bash
uv run pyright
```

Expected output:

```text
0 errors, 0 warnings, 0 informations
```

- [ ] **Step 3: Build the distribution artifacts**

Run:

```bash
uv build
```

Expected output:

```text
Building source distribution...
Building wheel...
Successfully built dist/dispatchbus-0.1.0a1.tar.gz
Successfully built dist/dispatchbus-0.1.0a1-py3-none-any.whl
```

- [ ] **Step 4: Inspect the built artifact names and embedded metadata version**

Run:

```bash
uv run python - <<'PY'
from pathlib import Path
import tarfile
import zipfile

wheel = sorted(Path('dist').glob('dispatchbus-0.1.0a1-*.whl'))[-1]
sdist = sorted(Path('dist').glob('dispatchbus-0.1.0a1.tar.gz'))[-1]
print(wheel.name)
print(sdist.name)
with zipfile.ZipFile(wheel) as zf:
    metadata_name = next(name for name in zf.namelist() if name.endswith('METADATA'))
    metadata = zf.read(metadata_name).decode()
for line in metadata.splitlines():
    if line.startswith('Name: ') or line.startswith('Version: '):
        print(line)
with tarfile.open(sdist, 'r:gz') as tf:
    pkg_info_name = next(name for name in tf.getnames() if name.endswith('PKG-INFO'))
    pkg_info = tf.extractfile(pkg_info_name).read().decode()
for line in pkg_info.splitlines():
    if line.startswith('Name: ') or line.startswith('Version: '):
        print(line)
PY
```

Expected output:

```text
dispatchbus-0.1.0a1-py3-none-any.whl
dispatchbus-0.1.0a1.tar.gz
Name: dispatchbus
Version: 0.1.0a1
Name: dispatchbus
Version: 0.1.0a1
```

- [ ] **Step 5: Stop and fix any verification failure before creating the release commit**

Do not create the release commit or tag if pytest, pyright, or build inspection fails.

### Task 6: Create the release commit and annotated tag

**Files:**
- Modify: git history only
- Test: git log and tag list

- [ ] **Step 1: Stage only the release-prep files**

Run:

```bash
git add pyproject.toml README.md CHANGELOG.md
```

Expected output:

```text
<no output>
```

- [ ] **Step 2: Create the release-prep commit**

Run:

```bash
git commit -m "chore: prepare 0.1.0a1 alpha release"
```

Expected output:

```text
[main <sha>] chore: prepare 0.1.0a1 alpha release
 3 files changed, <N> insertions(+), <N> deletions(-)
 create mode 100644 CHANGELOG.md
```

- [ ] **Step 3: Create the annotated alpha tag**

Run:

```bash
git tag -a v0.1.0a1 -m "dispatchbus 0.1.0a1 alpha release"
```

Expected output:

```text
<no output>
```

- [ ] **Step 4: Verify the commit and tag exist**

Run:

```bash
git log --oneline -1 && git tag --list 'v0.1.0a1'
```

Expected output:

```text
<sha> chore: prepare 0.1.0a1 alpha release
v0.1.0a1
```

- [ ] **Step 5: Leave push and publication as a separate explicit action**

Do not push commits or tags and do not publish artifacts as part of this plan.
