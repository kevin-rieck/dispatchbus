# Message Bus Library Design

Date: 2026-05-14
Project: dispatchr
Topic: async-first in-process message bus library for event-driven Python applications

## Goal

Build an async-first Python message bus library for in-process event-driven applications, with a clean path to external transports later.

The library should:
- support both commands and events as first-class concepts
- provide an async-first API
- support sync handlers and sync callers through a safe bridge
- allow configurable concurrency policies
- stay focused on in-memory dispatch for v1
- preserve internal boundaries so external transports can be introduced later without redesigning the public API

The library should not include broker integration, persistence, retries, or delivery guarantees in v1.

## Scope

### In scope for v1
- command dispatch with exactly one handler
- event publication with zero or more handlers
- async handlers
- sync handlers executed via executor
- async-first public API
- sync wrapper API for blocking callers
- handler registration
- middleware pipeline
- lifecycle management for runtime resources
- explicit error types
- configurable concurrency behavior
- tests for core behavior

### Out of scope for v1
- Redis, Kafka, RabbitMQ, or other broker integrations
- persistent queues or outbox patterns
- message serialization formats
- retries, dead-letter queues, or acknowledgements
- distributed delivery semantics
- scheduled or delayed delivery
- cross-process discovery or routing

## Recommended approach

Use a minimal in-memory bus with domain semantics as the core architecture.

The public API should be centered on application intent:
- `send(command)` for commands
- `publish(event)` for events

This keeps the API clear and ergonomic while allowing internals to separate dispatch concerns from future transport concerns.

Alternative approaches considered but not chosen:
- a transport-oriented core from day one: too much complexity for v1
- an actor-style runtime core: useful for some workloads but less aligned with a clean application bus API

## Architecture

The system is a single-process async-first runtime composed of:
- message models
- a handler registry
- a dispatcher/runtime
- a middleware pipeline
- lifecycle management
- a sync bridge
- a focused error model

Commands and events are distinct concepts:
- commands target exactly one handler and may return a result
- events target zero or more subscribers and do not return per-subscriber values

The async runtime is the source of truth. Sync APIs are wrappers around the async runtime rather than an independent execution model.

## Components

### 1. Message models

Provide lightweight, user-friendly message types.

Design guidance:
- users should be able to use plain Python classes or dataclasses for commands and events
- the library should not require a heavy base class hierarchy for v1
- command and event identity should be based on Python type

Possible shape:
- marker protocols or marker base classes for `Command` and `Event`
- optional envelope or metadata support may be added later, but should not be required in v1

This keeps the library ergonomic and avoids unnecessary ceremony.

### 2. Handler registry

The registry is responsible for storing and resolving handlers.

Responsibilities:
- map a command type to exactly one handler
- map an event type to zero or more handlers
- validate registrations
- reject duplicate command handler registrations

Behavior:
- command lookup failure should raise a dedicated error during dispatch
- event lookup with no subscribers should resolve to an empty list
- registration should fail fast on invalid configuration

This unit should be independent from execution logic so it can be tested directly.

### 3. Dispatcher/runtime

The runtime resolves handlers and executes them.

Responsibilities:
- invoke async handlers directly
- invoke sync handlers via an executor
- coordinate event fan-out
- apply concurrency policy
- manage in-flight task tracking for shutdown

The runtime should not expose raw loop and thread management as the main user-facing model. Internally it may use event loop and executor primitives, but the external API should remain simple and domain-focused.

### 4. Middleware pipeline

Middleware wraps dispatch operations for cross-cutting concerns.

Initial goals:
- support both command dispatch and event publication
- allow logging, tracing, metrics, validation, and policy enforcement
- preserve execution order consistently

Design guidance:
- middleware should wrap the logical operation, not individual low-level executor steps
- middleware should be composable and deterministic in order

Retries should not be built into v1 middleware behavior by default because retry semantics differ between commands and events and would add ambiguity.

### 5. Lifecycle management and sync bridge

The runtime needs explicit lifecycle management.

Responsibilities:
- start runtime resources when needed
- stop gracefully
- drain in-flight work within a timeout
- tear down executor resources

The sync bridge should:
- allow blocking callers to invoke command dispatch and event publication safely
- submit work into the async runtime
- avoid making sync loop/thread internals part of the primary programming model

This preserves an async-first architecture while supporting mixed application environments.

### 6. Error model

Define focused, explicit exception types rather than leaking low-level runtime errors as the main contract.

Examples:
- `NoCommandHandlerError`
- `DuplicateCommandHandlerError`
- `HandlerRegistrationError`
- `EventPublicationError` with per-handler failure details

Low-level exceptions raised by user handlers should still be preserved and surfaced appropriately.

## Public API direction

The public API should feel small and intentional.

Likely shape:
- `register_command_handler(command_type, handler)`
- `register_event_handler(event_type, handler)`
- `await send(command)`
- `await publish(event)`
- sync wrapper methods for blocking code, such as `send_sync(...)` and optionally `publish_sync(...)`

API guidance:
- command handlers may return a result
- event handlers should return no meaningful result for the caller
- handler type may be either sync or async
- the bus should be usable without requiring users to manually manage queues

The library should emphasize domain operations rather than exposing message queue internals.

## Data flow

### Command flow

1. caller invokes `send(command)`
2. middleware chain runs
3. registry resolves exactly one handler for the command type
4. runtime executes the handler
   - async handler: await directly
   - sync handler: execute in executor and await result
5. return handler result to caller
6. if the handler raises, propagate the exception to the caller

### Event flow

1. caller invokes `publish(event)`
2. middleware chain runs
3. registry resolves zero or more handlers for the event type
4. runtime dispatches handlers according to configured concurrency policy
5. publication completes when all dispatched handlers complete
6. if one or more handlers fail, raise a structured aggregate publication error

## Concurrency model

Concurrency should be configurable, with safe defaults.

Requirements:
- support conservative execution defaults in v1
- allow future per-handler or per-message overrides
- keep ordering guarantees explicit rather than implied

The design must keep these concerns distinct:
- execution concurrency
- ordering guarantees
- error propagation

Recommended default posture:
- commands execute as single operations against their one resolved handler
- event subscribers may execute concurrently, subject to policy
- any ordering guarantee beyond simple registration order should be considered optional and explicitly configured

For v1, the public configuration does not need to be large, but the runtime should be structured so concurrency policies are not hardcoded into unrelated components.

## Error handling behavior

### Commands
- if no handler is registered, raise a dedicated command resolution error
- if multiple handlers are somehow configured for a command, treat it as an invalid state and raise a dedicated error
- if the handler fails, surface that exception to the caller

### Events
- if no subscribers are registered, publishing succeeds as a no-op
- if one or more subscribers fail, raise an aggregate publication error containing failure details for each failed handler
- successful subscribers should not be rolled back or hidden because event handlers are independent

### Registration
- invalid registrations should fail immediately
- duplicate command registrations should be rejected

### Shutdown
- stop should attempt graceful completion of in-flight work within a timeout
- remaining work may then be cancelled or torn down
- shutdown semantics should be documented clearly so applications know what guarantees exist

## Testing strategy

The initial test suite should cover:
- registry behavior and validation
- command dispatch to async handlers
- command dispatch to sync handlers via executor
- event fan-out to multiple subscribers
- aggregate event failure behavior
- middleware ordering and wrapping behavior
- no-handler and duplicate-handler error cases
- sync bridge behavior for blocking callers
- runtime lifecycle start/stop and graceful shutdown behavior
- configurable concurrency behavior at the level exposed by v1

Tests should focus on externally visible behavior and a few targeted internal invariants, especially around lifecycle and error aggregation.

## Evolution path

This design intentionally leaves room for future transport support.

Expected future direction:
- keep public semantics centered on `send` and `publish`
- introduce transport abstractions behind the dispatch layer when a real need arises
- add optional envelopes or serialization only when external transport work begins

The key constraint is that v1 should not prematurely adopt distributed-system semantics before those requirements exist.

## Open decisions intentionally deferred

These are deferred, not unspecified:
- exact middleware callable interface
- exact message marker implementation style: base classes vs protocols
- concrete concurrency policy configuration API
- whether lifecycle is explicit-only or can also auto-start lazily
- naming details for sync wrapper methods

These decisions can be finalized during implementation planning without changing the approved architectural direction.

## Summary

Dispatchr v1 should be an async-first, in-process Python message bus focused on clear application semantics:
- commands with exactly one handler
- events with zero or more subscribers
- sync and async handler support
- middleware
- configurable concurrency
- explicit lifecycle and error handling

The design aims to keep the public API simple while preserving internal seams for future transport-backed evolution.