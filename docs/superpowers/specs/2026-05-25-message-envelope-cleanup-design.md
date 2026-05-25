# Message Envelope Cleanup Design

## Goal

Remove the leaking payload-to-metadata sidecar and simplify message envelope ownership so runtime metadata lives only on runtime envelopes or legacy pre-stamped messages.

## Problem

The current implementation stores plain payload metadata in a process-global sidecar keyed by `id(payload)`. That design has three problems:

1. It retains strong references to every payload that enters the bus, causing memory leaks and unbounded growth.
2. It couples metadata lookup to object identity and lifetime, which is more fragile than the envelope-first design.
3. It expands the public behavior of `get_metadata(...)` beyond the runtime-envelope model by making previously dispatched plain payloads globally inspectable.

## Desired outcome

The runtime should have a single clear metadata ownership model:

- `RuntimeMessage` carries metadata for plain payloads inside dispatchr internals.
- Legacy pre-stamped messages continue to expose metadata through their own `metadata` property.
- Plain payload objects remain plain objects and do not gain persistent metadata visibility as a side effect of dispatch.

This should eliminate the leak, bound memory use, and reduce conceptual complexity.

## Approaches considered

### 1. Remove the sidecar entirely

Make `get_metadata(...)` succeed only for `RuntimeMessage` and legacy pre-stamped messages.

- Pros: fixes the leak completely, matches the envelope-first architecture, simplest implementation
- Cons: removes the current undocumented-but-implemented behavior where a plain payload may become inspectable after dispatch

### 2. Keep a weak or evicting sidecar

Retain post-dispatch lookup on plain payloads through weak references or bounded caches.

- Pros: preserves some current behavior
- Cons: more complexity, still identity-coupled, still diverges from the clean envelope model, weakref support is inconsistent across payload types

### 3. Mutate payloads to attach metadata

Stamp metadata directly onto payload instances.

- Pros: simple lookup story
- Cons: violates the plain-payload and immutable-model goals, conflicts with frozen dataclasses and similar libraries

## Recommendation

Adopt approach 1.

## Proposed design

### Metadata ownership

Metadata is readable from exactly two sources:

1. `RuntimeMessage.metadata`
2. `message.metadata` for legacy or explicitly pre-stamped message objects

No global payload registry is maintained.

### `get_metadata()` behavior

`get_metadata(message)` should behave as follows:

- if `message` is a `RuntimeMessage`, return `message.metadata`
- otherwise, if `message.metadata` exists and is a `MessageMetadata`, return it
- otherwise, raise `ValueError("Message metadata is unavailable for this object")`

This preserves support for runtime envelopes and legacy stamped messages while making the plain-payload boundary explicit.

### `as_runtime_message()` behavior

`as_runtime_message(message, parent=None)` should behave as follows:

- if `message` is already a `RuntimeMessage`, return it unchanged
- otherwise, if `get_metadata(message)` succeeds, preserve that metadata
- otherwise, create metadata:
  - use `new_root_metadata()` when `parent is None`
  - use `derive_child_metadata(parent)` when `parent` is provided
- return `RuntimeMessage(payload=message, metadata=resolved_metadata)`

It should not mutate the payload and should not register the payload in any global store.

### Error handling

The metadata probing path should avoid broad exception swallowing where possible. The implementation should only treat expected metadata-unavailable cases as absence and should not introduce unnecessary hidden global state.

### Runtime behavior

No runtime pipeline changes are required beyond using the simplified helpers:

- bus root boundaries still wrap commands and events into `RuntimeMessage`
- event emission still derives child metadata from parent metadata
- handlers still receive payload objects
- observability still exposes payload plus separate metadata fields

## Documentation changes

Update `README.md` to state clearly:

- plain payload models are the primary API
- metadata is runtime-owned for plain payloads
- `get_metadata(...)` works for runtime envelopes and legacy/pre-stamped messages
- `get_metadata(...)` raises for plain payload objects, including payloads that were previously dispatched without preserving their runtime envelope

Remove any wording that implies a plain payload becomes inspectable after dispatch.

## Testing strategy

Update tests to cover:

- `get_metadata(plain_payload)` raises before dispatch
- `get_metadata(plain_payload)` still raises after the payload has been dispatched if the caller only has the payload object
- `get_metadata(runtime_message)` returns envelope metadata
- `get_metadata(legacy_pre_stamped_message)` returns its metadata
- `as_runtime_message(plain_payload)` creates root metadata
- `as_runtime_message(plain_event, parent=...)` derives child correlation and causation
- `as_runtime_message(runtime_message)` returns the same object unchanged

Remove tests that validate the old sidecar lookup behavior.

## File impact

- Modify: `src/dispatchr/messages.py`
  - remove sidecar types/state
  - simplify `get_metadata()`
  - simplify `as_runtime_message()`
- Modify: `README.md`
  - document the cleaned metadata boundary
- Modify: `tests/unit/test_messages.py`
  - remove sidecar-specific tests
  - add explicit post-dispatch plain-payload failure coverage if needed
- Modify: any integration/unit tests that assume original payloads become metadata-readable after dispatch

## Risks

- This is a behavior break for callers that relied on `get_metadata(payload)` succeeding after dispatch on the original plain payload object.
- Some tests or docs may currently encode that behavior and will need updating.

These risks are acceptable because the current behavior is implemented through a leaking global sidecar and conflicts with the desired envelope-first architecture.

## Success criteria

- no global payload metadata sidecar remains
- plain payload dispatch does not retain payloads globally
- `get_metadata(...)` has a small, explicit contract
- docs match implementation
- tests no longer rely on post-dispatch side effects on plain payload objects
