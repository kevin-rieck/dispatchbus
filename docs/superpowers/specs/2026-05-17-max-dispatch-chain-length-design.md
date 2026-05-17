# Max Dispatch Chain Length For Follow-Up Events

## Goal

Add an optional limit to `MessageBus` that caps the number of follow-up events emitted from a single top-level dispatch chain. The limit applies only to events emitted through `context.emit(...)` and does not change the behavior of top-level `send()` or `publish()` calls.

## Scope

This design covers follow-up event chains that originate from:

- a command handler emitting events during `send()`
- an event handler emitting additional events during `publish()`

This design does not add limits for:

- top-level `send()` calls
- top-level `publish()` calls
- explicit nested public `bus.send()` or `bus.publish()` calls made from handler code
- registration-time graph analysis or cycle detection

## Current Behavior

- `send()` dispatches one command handler and publishes each emitted event afterward.
- `publish()` dispatches all handlers for an event and processes emitted follow-up events until no more remain.
- Follow-up event failures are accumulated into `EventPublicationError`.
- Deep acyclic follow-up chains are supported without recursion overflow.
- There is currently no built-in limit on how many emitted follow-up events a single top-level dispatch can trigger.

## Desired Semantics

The new limit counts only emitted follow-up events, not the original top-level operation.

Examples:

- `max_dispatch_chain_length=None`: unlimited, which preserves current behavior
- `max_dispatch_chain_length=1`: allow one emitted follow-up event after the original top-level `send()` or `publish()`
- `max_dispatch_chain_length=2`: allow two emitted follow-up events in that same top-level chain

This is a total follow-up budget for a top-level dispatch chain, not a per-branch depth limit.

## Proposed API

Add an optional constructor parameter:

`MessageBus(..., max_dispatch_chain_length: int | None = None)`

Rules:

- `None` means unlimited
- `0` means emitted follow-up events are disabled
- negative values are invalid and should raise `HandlerRegistrationError`

This keeps the feature opt-in and preserves the current default behavior.

## Recommended Approach

Track a per-top-level follow-up budget inside the bus and consume one unit before each emitted event is dispatched.

Why this approach fits:

- it matches the requested semantics exactly
- it applies centrally where emitted events are orchestrated today
- it works for both command-originated and event-originated follow-ups
- it can protect against both long linear chains and wide fan-out within one top-level dispatch

## Why Not A Depth Counter

A pure depth counter is the wrong model here.

Example:

- an event emits three sibling follow-up events

Each child has depth `1`, so a depth-only limit would allow all of them even if the intended budget were `1`. Because you asked for max dispatch chain length to count follow-up events after the original dispatch, the bus should count total emitted follow-up dispatches instead.

## Error Model

Add a dedicated exception such as `MaxDispatchChainLengthExceededError`.

Suggested meaning:

- the bus attempted to dispatch an emitted follow-up event
- doing so would exceed the configured `max_dispatch_chain_length`

Suggested message shape:

- `max dispatch chain length exceeded: allowed 1 follow-up event(s)`

Behavioral rules:

- the rejected emitted event is not dispatched
- the failure is collected the same way other follow-up publication failures are collected
- top-level `send()` and `publish()` surface the failure through `EventPublicationError`, preserving the existing aggregation model

## Architecture Changes

The change belongs in `src/dispatchr/bus.py`, where emitted follow-up events are already scheduled.

### Budget Ownership

Each top-level dispatch owns a single follow-up budget.

- a top-level `send()` seeds the budget for everything emitted from that command and its descendants
- a top-level `publish()` seeds the budget for everything emitted from that event and its descendants
- nested emitted follow-up events consume from the same shared budget

Explicit public nested calls remain separate operations and do not consume that budget.

### Internal Representation

Represent the budget using a small internal mutable state object that is created once per top-level dispatch chain and then passed through internal helper calls.

That state should track:

- configured limit
- consumed follow-up count

It should expose a method that atomically reserves one slot for an emitted event or raises `MaxDispatchChainLengthExceededError`.

### Follow-Up Dispatch Flow

For every emitted event produced by a handler outcome:

1. reserve one slot from the current top-level chain budget
2. if reservation succeeds, dispatch the emitted event normally
3. if reservation fails, do not dispatch the emitted event and collect the exception as a follow-up failure

This rule should be applied consistently for:

- events emitted by command handlers in `send()`
- events emitted by event handlers in sequential mode
- events emitted by event handlers in concurrent mode

## Concurrency Considerations

Concurrent event mode needs one shared budget per top-level chain, not one budget copy per task.

That means the accounting object should not rely only on copied context-local integers, because sibling tasks would otherwise reserve independently and oversubscribe the configured limit. Instead:

- top-level dispatch creates one shared internal budget object
- emitted follow-up dispatch paths receive a reference to that same object
- budget reservation is serialized with a lock that is safe for the async event loop

Given the current architecture, an `asyncio.Lock` owned by the top-level chain is sufficient because budget consumption happens from async dispatch code on one event loop.

## Testing Strategy

Use TDD with failing tests first.

Required coverage:

- default behavior remains unlimited and existing deep emitted chains still pass
- `max_dispatch_chain_length=0` rejects the first emitted follow-up event
- `max_dispatch_chain_length=1` allows exactly one emitted follow-up event after a top-level `send()`
- `max_dispatch_chain_length=1` allows exactly one emitted follow-up event after a top-level `publish()`
- when the budget is exceeded, the rejected emitted event is not handled
- sibling handler failures and max-chain failures are both aggregated into one `EventPublicationError`
- concurrent event mode does not oversubscribe the shared budget when multiple handlers emit follow-ups at the same time
- explicit nested public `bus.publish()` or `bus.send()` calls are unchanged by the feature

## File Impact

Expected primary changes:

- `src/dispatchr/bus.py` for budget creation, propagation, and reservation
- `src/dispatchr/exceptions.py` for the new exception
- `src/dispatchr/__init__.py` if the exception should be part of the public surface
- `tests/test_message_bus.py` for behavior coverage

## Risks And Mitigations

### Risk: copied state in concurrent follow-up tasks bypasses the limit

Mitigation:

- use one shared budget object per top-level dispatch chain
- add explicit concurrent emission tests at the boundary

### Risk: ambiguity about what counts toward the limit

Mitigation:

- document clearly that only emitted follow-up events count
- keep top-level calls and explicit nested public calls out of scope
- add tests covering both included and excluded cases

### Risk: accidental API breakage

Mitigation:

- make the new constructor argument optional with a default of `None`
- preserve existing method signatures and error aggregation behavior

## Implementation Constraints

- preserve current behavior by default
- preserve drain and close semantics
- keep support for deep emitted chains when no limit is configured
- run verification with `uv run pytest`, `uv run pyright`, and `uv run ruff format .`

## Recommended Next Step

Write an implementation plan that starts with failing tests for the budget boundary, then adds the shared follow-up budget to internal bus dispatch paths, and finally verifies sequential and concurrent behavior.
