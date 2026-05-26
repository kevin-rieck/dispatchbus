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
