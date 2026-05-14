# Observability Subscribers Design

Date: 2026-05-14
Project: dispatchr
Topic: lifecycle event subscribers for message bus observability

## Goal

Add first-class observability to `dispatchr` through lifecycle event subscribers.

The feature should:
- expose read-only lifecycle events for `send()` and `publish()` operations
- support logging, metrics, tracing, and debugging use cases
- preserve existing dispatch semantics exactly
- work with both sync and async handlers
- remain framework-agnostic and minimal

The feature should not introduce retry policy, flow control, or middleware redesign.

## Scope

### In scope
- typed lifecycle event payloads
- subscriber registration on `MessageBus`
- internal subscriber notification during command and event dispatch
- support for sync and async subscribers
- subscriber failure isolation
- tests for command, event, sequential, and concurrent flows

### Out of scope
- vendor-specific tracing integrations
- metrics backends
- context propagation APIs
- filtering subscriptions by message type
- middleware API changes
- retry or error policy changes

## Recommended approach

Use a simple subscriber API with typed lifecycle events.

This is the smallest design that makes observability a first-class feature without turning middleware into a policy engine. It matches the current minimal style of the library while creating a stable foundation for future integrations.

Alternative approaches considered but not chosen:
- richer context-aware middleware as the primary API: too large a shift for this feature
- typed observer classes or protocols as the primary API: more ceremony than needed for the current library style
- backend-specific logging or tracing integrations: too opinionated for a general-purpose core library

## Architecture

The feature should add three focused pieces:

1. lifecycle event types
2. subscriber dispatching support
3. message bus integration points

`MessageBus.send()` and `MessageBus.publish()` should emit lifecycle events around the existing runtime flow. Subscriber notifications must be passive and must never alter message dispatch behavior.

## Public API direction

The public API should stay small.

Likely shape:
- `MessageBus(subscribers=[...])`
- optionally `bus.add_subscriber(subscriber)` for incremental registration

A subscriber should be a callable that accepts a single lifecycle event object. Sync and async subscriber callables should both be supported.

Example shape:

```python
async def subscriber(event: object) -> None:
    ...

bus = MessageBus(subscribers=[subscriber])
```

The recommended API is a simple subscriber list rather than multiple event-specific registration methods.

## Event model

Define five lifecycle event types:
- `DispatchStarted`
- `DispatchFinished`
- `HandlerStarted`
- `HandlerFinished`
- `HandlerFailed`

These should be small typed dataclasses in a dedicated module.

### Common fields

All lifecycle events should include:
- `message`: the original message object
- `message_type`: the concrete Python type of the message
- `operation`: `"send"` or `"publish"`
- `timestamp`: the moment the lifecycle event was emitted

### Dispatch-level fields

`DispatchStarted` and `DispatchFinished` should include:
- `dispatch_id`: unique identifier shared by all related lifecycle events
- `handler_count`: number of resolved handlers involved in the operation

`DispatchFinished` should additionally include:
- `duration_ms`: elapsed dispatch duration
- `success`: whether the overall dispatch completed without raising

### Handler-level fields

`HandlerStarted`, `HandlerFinished`, and `HandlerFailed` should include:
- `dispatch_id`
- `handler`: the callable reference
- `handler_name`: stable string representation for logging and metrics

`HandlerFinished` and `HandlerFailed` should additionally include:
- `duration_ms`

`HandlerFailed` should additionally include:
- `error`: the raised exception

## Behavioral semantics

### Commands
- emit one dispatch lifecycle sequence
- emit one handler lifecycle sequence
- preserve current result and exception behavior exactly

### Events
- emit one dispatch lifecycle sequence per `publish()` call
- emit one handler lifecycle sequence per resolved event handler
- preserve current no-subscriber and aggregate-error behavior exactly

### No subscribers registered
- dispatch behavior should remain unchanged
- observability path should be a cheap no-op

## Subscriber execution and safety

Subscriber behavior should be defined explicitly:
- subscribers are best-effort observers
- subscriber failures must never fail `send()` or `publish()`
- subscriber exceptions should be swallowed internally
- subscribers should be invoked in registration order
- sync subscribers should run through the same sync-call support style used elsewhere in the runtime
- async subscribers should be awaited

For concurrent event handlers, lifecycle notifications may interleave across handlers. Ordering must remain consistent within a single handler lifecycle:
- `HandlerStarted`
- then either `HandlerFinished` or `HandlerFailed`

Dispatch-level ordering should remain:
- `DispatchStarted`
- zero or more handler lifecycle events
- `DispatchFinished`

## Implementation boundaries

### 1. Lifecycle event module

Add a dedicated module for typed event dataclasses.

Responsibilities:
- define lifecycle payload types
- keep payloads read-only
- avoid leaking internal runtime state beyond what observability needs

### 2. Subscriber notifier support

Add internal support for notifying subscribers.

Responsibilities:
- accept subscriber callables
- invoke sync and async subscribers correctly
- isolate subscriber errors
- keep notification order deterministic

This logic should be factored so dispatch code remains readable.

### 3. Message bus and runtime integration

Emit lifecycle events around existing command and event execution.

Responsibilities:
- create a `dispatch_id`
- measure dispatch and handler durations
- resolve handler identity strings consistently
- emit events for both command and event flows
- preserve current aggregate error and concurrency behavior

## Handler identity

Expose both:
- the original handler callable as `handler`
- a stable string as `handler_name`

`handler_name` should be suitable for logs and metrics and should not require consumers to inspect Python callable internals directly.

The exact formatting can be finalized during implementation planning, but it should be deterministic and human-readable.

## Error handling

This feature must not change existing message handling semantics.

That means:
- command handler exceptions still propagate to the caller
- event handler failures still contribute to `EventPublicationError`
- subscriber exceptions are isolated and ignored by dispatch semantics

If dispatch fails, `DispatchFinished(success=False, ...)` should still be emitted before the exception leaves the bus.

## Testing strategy

Add tests for:
- command lifecycle event emission in order
- event lifecycle event emission in order
- lifecycle payload contents: operation, message type, dispatch ID, timing, handler identity
- sync subscriber support
- async subscriber support
- subscriber failure isolation
- no-subscriber fast path behavior
- sequential publish ordering guarantees
- concurrent publish interleaving with correct per-handler ordering
- failure cases emitting `HandlerFailed` and unsuccessful `DispatchFinished`

Tests should verify externally visible observability behavior rather than internal implementation details.

## Evolution path

This design creates a base for future observability features without committing to larger abstractions yet.

Possible future additions built on this foundation:
- typed observer adapters
- filtering subscriptions
- tracing adapters
- metrics adapters
- richer dispatch context for instrumentation

Those should remain optional layers above this simple subscriber model.

## Summary

The next feature that best improves `dispatchr` as a general-purpose library is first-class observability through lifecycle event subscribers.

The design keeps the API minimal:
- simple subscriber registration
- five typed lifecycle events
- read-only payloads
- safe best-effort delivery

It improves real-world usefulness for logging, metrics, tracing, and debugging while preserving the library's current execution semantics and lightweight feel.
