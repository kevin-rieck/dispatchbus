# dispatchr

`dispatchr` is an in-memory Python message bus for applications that want explicit command and event dispatch without bringing in a framework.

It supports:

- one command handler per command
- zero or more event handlers per event
- async-first APIs with synchronous bridge methods
- middleware around command and event dispatch
- follow-up events emitted from handlers via a context object
- observability subscribers for dispatch and handler lifecycle events
- optional debug subscribers for human-friendly or key/value logging
- sequential or concurrent event handler execution

## Requirements

- Python 3.12+

## Installation

With `uv`:

```powershell
uv add dispatchr
```

With `pip`:

```powershell
pip install dispatchr
```

## Quick start

```python
from dataclasses import dataclass

from dispatchr import MessageBus


@dataclass(frozen=True)
class CreateUser:
    name: str


@dataclass(frozen=True)
class UserCreated:
    user_id: int


async def create_user(command: CreateUser, context) -> str:
    user_id = len(command.name)
    context.emit(UserCreated(user_id=user_id))
    return command.name.upper()


async def on_user_created(event: UserCreated) -> None:
    print(f"user created: {event.user_id}")


bus = MessageBus()
bus.register_command_handler(CreateUser, create_user)
bus.register_event_handler(UserCreated, on_user_created)

result = await bus.send(CreateUser(name="ada"))
print(result)  # ADA
```

## Core concepts

### Commands

Commands are sent with `await bus.send(command)`.

- A command must have exactly one registered handler.
- Registering a second command handler for the same message type raises `DuplicateCommandHandlerError`.
- Sending a command with no handler raises `NoCommandHandlerError`.

### Events

Events are published with `await bus.publish(event)`.

- An event may have zero, one, or many handlers.
- Publishing an event with no handlers is allowed.
- If one or more event handlers fail, `EventPublicationError` is raised and exposes the collected failures.

### Handler lookup

Handler lookup uses the exact runtime type of the message.

If you register a handler for `BaseEvent`, it will not automatically receive `DerivedEvent` instances unless you also register `DerivedEvent` explicitly.

## Handler shapes

Handlers and subscribers may be:

- `async def` callables, or
- synchronous callables that return a final value immediately.

A synchronous callable that returns a coroutine or other awaitable is rejected at runtime.

Handlers can optionally accept a `context` argument:

```python
async def handle(message, context) -> None:
    ...

async def handle(message, *, context) -> None:
    ...
```

The context lets a handler emit follow-up events:

```python
context.emit(SomeEvent(...))
```

## Event concurrency

Configure event execution with `event_concurrency`:

```python
bus = MessageBus(event_concurrency="sequential")
```

Supported modes:

- `"concurrent"` (default): event handlers start together and may finish in any order
- `"sequential"`: event handlers run in registration order

Semantics:

- command dispatch always targets a single handler
- sequential event mode preserves handler execution order for a given event
- concurrent event mode does not guarantee completion order
- follow-up events emitted by concurrent handlers may interleave

## Middleware

Middleware wraps dispatch and can intercept both `send()` and `publish()` pipelines.

```python
from typing import Any


async def logging_middleware(message: Any, next_call):
    print(f"before {type(message).__name__}")
    try:
        return await next_call(message)
    finally:
        print(f"after {type(message).__name__}")


bus = MessageBus(middleware=[logging_middleware])
```

Middleware receives the message and a `next_call` awaitable callback.

## Observability subscribers

Subscribers can observe dispatch lifecycle events emitted by the bus:

- `DispatchStarted`
- `DispatchFinished`
- `HandlerStarted`
- `HandlerFinished`
- `HandlerFailed`

```python
from dispatchr import DispatchFinished, DispatchStarted, HandlerFailed, MessageBus


async def audit(event: object) -> None:
    match event:
        case DispatchStarted(operation=operation, message_type=message_type):
            print(f"starting {operation} for {message_type.__name__}")
        case DispatchFinished(operation=operation, success=success, duration_ms=duration_ms):
            print(f"finished {operation}: success={success} duration_ms={duration_ms:.2f}")
        case HandlerFailed(handler_name=handler_name, error=error):
            print(f"handler failed: {handler_name}: {error}")


bus = MessageBus(subscribers=[audit])
```

Subscribers are isolated from dispatch: subscriber exceptions are ignored.

## Debug subscribers

`dispatchr.debug` includes ready-made subscribers for development-time logging:

- `DebugSubscriber.human(...)`
- `DebugSubscriber.key_value(...)`
- `debug_subscriber_human(...)`
- `debug_subscriber_key_value(...)`

```python
from dispatchr import MessageBus
from dispatchr.debug import DebugSubscriber


bus = MessageBus(
    subscribers=[DebugSubscriber.human(include_dispatch=True)],
)
```

These subscribers can write to:

- a text stream
- a `logging.Logger`
- a Rich console if `rich` is installed

## Async and sync APIs

Use the async methods from async code:

- `await bus.send(...)`
- `await bus.publish(...)`
- `await bus.aclose()`

Use the sync bridge methods from synchronous code:

- `bus.send_sync(...)`
- `bus.publish_sync(...)`
- `bus.close()`

Calling sync bridge methods inside an active event loop raises `BusUsageError`.

## Closing behavior

When closing begins, the bus stops accepting new top-level sends and publishes.
Already-running dispatch can still finish, including nested event publication triggered during that work.

## Public API

The package root currently exports:

- `MessageBus`
- `DispatchStarted`
- `DispatchFinished`
- `HandlerStarted`
- `HandlerFinished`
- `HandlerFailed`
- `DispatchrError`
- `HandlerRegistrationError`
- `DuplicateCommandHandlerError`
- `NoCommandHandlerError`
- `EventPublicationError`
- `BusUsageError`

## Current limitations

`dispatchr` is intentionally small today. Current limitations include:

- in-memory only; no broker, queue, or transport integration
- no persistence, retries, scheduling, or outbox support
- exact-type handler lookup only; no inheritance-based dispatch
- one command handler per command type
- sync handlers and sync subscribers run in a thread pool
- long-running blocking sync work can reduce throughput
- event ordering guarantees depend on the selected concurrency mode
- the `dispatchr` CLI entry point is currently just a placeholder

## Development

This repo uses `uv`, `ruff`, `pyright`, and `pytest`.

```powershell
uv sync --dev
just format
just check
```

Available local commands:

```powershell
just format
just lint
just typecheck
just test
just build
just check
just all
```
