# Bus Drain Lifecycle Design

Date: 2026-05-15
Project: dispatchr
Topic: graceful drain-and-close lifecycle for the message bus

## Goal

Add explicit lifecycle management to `MessageBus` so shutdown is predictable and safe.

The lifecycle should:
- reject new messages once shutdown begins
- allow already accepted work to finish
- preserve emitted follow-up event delivery for accepted work
- release runtime resources only after draining completes
- behave consistently for async and sync entry points

The lifecycle should not:
- silently drop externally submitted messages
- cancel accepted work by default
- expand the public API beyond the existing close methods unless necessary

## Scope

### In scope
- drain-aware behavior for `send()` and `publish()`
- matching drain-aware behavior for `send_sync()` and `publish_sync()`
- explicit internal lifecycle states
- graceful shutdown through `close()` and `aclose()`
- idempotent shutdown calls
- dedicated lifecycle exception for rejected messages
- tests covering draining, nested follow-up events, and sync bridge behavior

### Out of scope
- configurable shutdown timeouts
- forced cancellation APIs
- transport-level shutdown semantics
- lifecycle event subscriber additions specific to drain state
- registration freezing after shutdown

## Recommended approach

Use a three-state internal lifecycle:
- `open`
- `draining`
- `closed`

When shutdown starts, the bus transitions from `open` to `draining`. In that state, the bus rejects new external `send()` and `publish()` requests with a dedicated lifecycle exception while allowing already accepted operations to complete. Once all accepted work has completed and runtime resources have been released, the bus transitions to `closed`.

This is the smallest design that gives `dispatchr` a clear graceful shutdown contract without adding new public methods or dropping accepted work.

Alternative approaches considered but not chosen:
- immediate hard close: simpler but violates graceful completion
- separate `drain()` and `close()` public methods: more flexible but unnecessary complexity for the current library
- silently ignoring new messages during shutdown: easier operationally to miss and too ambiguous for callers

## Lifecycle contract

### States

#### `open`
- new `send()` and `publish()` requests are accepted
- accepted work contributes to the in-flight operation count

#### `draining`
- entered when `close()` or `aclose()` begins shutdown
- new externally submitted `send()` and `publish()` requests fail immediately
- already accepted operations continue running
- follow-up events emitted from already accepted operations are still processed

#### `closed`
- entered after all accepted work finishes and runtime resources are released
- all new externally submitted `send()` and `publish()` requests fail immediately
- repeated shutdown calls succeed as no-ops

## Error contract

Add a dedicated lifecycle exception under `DispatchrError`:
- `BusDrainingError`

Behavior:
- `send()`, `publish()`, `send_sync()`, and `publish_sync()` raise `BusDrainingError` once shutdown has begun
- calls made after full shutdown also raise `BusDrainingError`
- the exception message should be explicit and stable, for example: `message bus is draining and not accepting new messages`

The contract intentionally uses one exception for both `draining` and `closed` states because the operational meaning for callers is the same: the bus is no longer accepting new work.

## Behavioral semantics

### Accepted external work

An operation is considered accepted if it entered the bus while the state was `open`.

Accepted operations:
- may complete normally
- may fail with their existing domain exceptions
- may emit follow-up events through `EventContext`
- continue to participate in shutdown draining until fully complete

### Rejected external work

Any new external `send()` or `publish()` request that arrives after shutdown starts is rejected before dispatch begins.

That means:
- no handler lookup should occur for rejected work
- no middleware should execute for rejected work
- no subscribers should observe dispatch lifecycle events for rejected work

### Follow-up emitted events

Follow-up events emitted by handlers that were already accepted before drain started remain part of the accepted operation graph and must continue to be published during the drain period.

This preserves the current semantic expectation that a command or event handler that emits follow-up events completes its logical work before shutdown finishes.

## Implementation shape

Track draining at the `MessageBus` layer rather than only in `MessageRuntime`.

Responsibilities at the bus layer:
- own the lifecycle state
- decide whether external work is accepted or rejected
- count accepted in-flight operations
- coordinate waiters used by shutdown
- allow accepted nested follow-up dispatches to continue during drain

Recommended internal structure:
- a lifecycle state field
- an in-flight counter
- an async coordination primitive that lets `aclose()` wait until the counter reaches zero
- a thread-safe coordination primitive or bridge for `close()`

The runtime should remain responsible for:
- executing handlers
- managing executor-backed work
- tracking any runtime-level event handler tasks it creates internally
- releasing executor resources during `aclose()`

## Shutdown flow

### `aclose()`

1. if state is `closed`, return immediately
2. if state is `open`, transition to `draining`
3. wait for accepted in-flight work to reach zero
4. await `MessageRuntime.aclose()`
5. stop the background loop if one exists
6. transition to `closed`

### `close()`

`close()` should provide the same semantics for synchronous callers by submitting the async shutdown path into the background runtime when needed, waiting for completion, then tearing down the background loop.

### Idempotency

Repeated `close()` or `aclose()` calls:
- should not raise
- should not attempt duplicate resource teardown
- should observe the same final `closed` state

## Edge cases

### Shutdown without prior sync bridge use

If no background loop was created, shutdown should still drain and release runtime resources correctly.

### Shutdown during concurrent event fan-out

If drain begins while concurrent event handlers are running:
- in-progress handlers continue
- follow-up events emitted by those handlers continue
- new external publishes are rejected

### Shutdown while middleware or subscribers are active

If an accepted operation is already inside middleware or dispatch lifecycle notifications when drain begins, that operation continues normally as part of the accepted in-flight work.

## Testing strategy

Add tests for:
- `send()` rejecting new work after drain begins
- `publish()` rejecting new work after drain begins
- accepted work finishing successfully while shutdown waits
- emitted follow-up events from accepted work still running during drain
- sync bridge rejection after drain begins
- repeated `close()` and `aclose()` calls being harmless
- shutdown completing correctly whether or not a background loop was created

Tests should focus on externally visible lifecycle behavior rather than internal locking details.

## Summary

`dispatchr` should implement graceful shutdown by moving `MessageBus` through `open`, `draining`, and `closed` states.

The essential contract is:
- shutdown begins with a transition to `draining`
- new external messages fail immediately with `BusDrainingError`
- accepted work, including follow-up emitted events, is allowed to finish
- resources are released only after draining completes

This keeps shutdown semantics explicit, safe, and compatible with the library's current async-first design.
