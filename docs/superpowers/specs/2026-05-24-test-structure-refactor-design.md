# Test Structure Refactor Design

## Goal

Refactor the test suite so it mirrors the current module structure more clearly and avoids pytest collection ambiguity between unit and integration coverage.

## Summary

The repository has been refactored into a clearer module layout under `src/dispatchr/`, but the tests still live in two broad top-level files:

- `tests/test_registry.py`
- `tests/test_message_bus.py`

That layout no longer matches the code structure well. It also makes it harder to locate tests by module and to distinguish isolated unit coverage from cross-module integration coverage.

## Chosen Approach

Adopt a two-level test layout split by test type:

- `tests/unit/`
- `tests/integration/`

Within each directory, use module-aligned filenames:

- unit: `tests/unit/test_<module>.py`
- integration: `tests/integration/test_<module>_int.py`

Examples:

- `tests/unit/test_registry.py`
- `tests/unit/test_bus.py`
- `tests/integration/test_bus_int.py`

This keeps filenames unique for pytest collection while making the source-module relationship explicit.

## Alternatives Considered

### 1. Minimal rename only
Keep the current broad files and only move them into `tests/unit/` and `tests/integration/`.

- Pros: lowest change volume
- Cons: still weakly aligned to the refactored source layout

### 2. Strict module-aligned split
Chosen approach.

- Pros: best discoverability, mirrors `src/dispatchr/`, supports future growth
- Cons: requires breaking up the current large bus-oriented test file

### 3. Subsystem-aligned split
Keep larger test files organized by runtime subsystem rather than source module.

- Pros: less churn than a full module split
- Cons: does not fully reflect the repo’s new structure

## File Layout Rules

### Unit tests
Use `tests/unit/test_<module>.py` for isolated behavior that primarily exercises one module at a time.

Typical unit cases:

- registry registration and lookup behavior
- helper and metadata behavior for observability
- public export assertions for `__init__`
- validation behavior that does not require multi-module orchestration

### Integration tests
Use `tests/integration/test_<module>_int.py` for behavior that coordinates multiple modules or runtime boundaries.

Typical integration cases:

- `MessageBus` send and publish flows
- middleware application across dispatch operations
- lifecycle event publication to subscribers
- shutdown/drain semantics
- background event-loop and thread behavior
- sequential vs concurrent event execution guarantees

### Empty files
Do not create placeholder test files for every source module. Only create a module-aligned test file when it contains real coverage.

## Initial Mapping From Current Tests

### `tests/test_registry.py`
Move to:

- `tests/unit/test_registry.py`

This file is already a good fit for unit coverage.

### `tests/test_message_bus.py`
Split by concern into at least:

- `tests/unit/test_bus.py`
- `tests/unit/test_observability.py`
- `tests/unit/test___init__.py` if public export checks remain standalone
- `tests/integration/test_bus_int.py`

The integration file should retain the end-to-end bus behavior tests, especially where dispatch, runtime, middleware, subscribers, event propagation, async/sync bridging, and shutdown rules interact.

The unit files should collect smaller checks that are module-local and do not need full end-to-end orchestration.

## Migration Strategy

1. Create `tests/unit/` and `tests/integration/`.
2. Move the registry tests into `tests/unit/test_registry.py`.
3. Split the current bus-heavy test file by intent:
   - move module-local checks into unit files
   - keep cross-module runtime behavior in `tests/integration/test_bus_int.py`
4. Remove the old top-level test files once coverage has been migrated.
5. Run the full test suite to confirm pytest collection is clean and filenames do not collide.

## Classification Heuristic

When deciding whether a test is unit or integration:

- classify as unit if the main behavior under test belongs to one module and can be understood without exercising the broader runtime pipeline
- classify as integration if the value of the test comes from coordination across the registry, runtime, middleware, subscribers, sync bridge, event propagation, or shutdown machinery

This heuristic is more important than the raw number of functions touched internally.

## Expected Result

After the refactor:

- tests are easier to find by source module
- unit and integration coverage are clearly separated
- pytest collection remains unambiguous because integration files use the `_int` suffix
- future modules can gain matching test files without returning to broad catch-all test files

## Scope Boundaries

This refactor changes test organization and filenames, not production behavior. The goal is structural clarity, not rewriting assertions unless necessary to fit the new organization.
