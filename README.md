# dispatchr

Minimal Python project scaffolded for modern tooling:

- `uv` for environment management, dependency installation, locking, and builds
- `ruff` for linting and formatting
- `just` for local workflow commands

## Quick start

Install development dependencies:

```powershell
uv sync --dev
```

Install `just`:
- See https://github.com/casey/just#installation

Recommended workflow:

```powershell
just format
just check
```

Available commands:

```powershell
just format     # auto-format source files
just lint       # run Ruff lint checks
just typecheck  # run Pyright
just test       # run pytest
just build      # build sdist and wheel
just check      # format check, lint, typecheck, test, build
just all        # format, then run full check pipeline
```

## Message bus example

```python
from dataclasses import dataclass

from dispatchr import MessageBus


@dataclass(frozen=True)
class CreateUser:
    name: str


@dataclass(frozen=True)
class UserCreated:
    user_id: int


async def create_user(command: CreateUser) -> str:
    return command.name.upper()


async def on_user_created(event: UserCreated) -> None:
    print(event.user_id)


bus = MessageBus()
bus.register_command_handler(CreateUser, create_user)
bus.register_event_handler(UserCreated, on_user_created)
```

Handlers and subscribers must be either:

- `async def` callables, or
- synchronous callables that return a final value immediately.

A synchronous callable that returns a coroutine or other awaitable is rejected at runtime.

## Observability subscribers

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

## Sync vs async API usage

- Use `await bus.send(...)` and `await bus.publish(...)` from async code.
- Use `bus.send_sync(...)` and `bus.publish_sync(...)` only from synchronous code.
- Use `await bus.aclose()` from async code.
- Use `bus.close()` only from synchronous code.

## Dispatch semantics

- A command has exactly one handler.
- An event may have zero or more handlers.
- Handler lookup uses the exact runtime type of the message.
- In sequential event mode, handlers for a given event run in registration order.
- In concurrent event mode, handlers are started together and may finish in any order.
- Follow-up events emitted by different concurrent handlers may interleave.

## Operational notes

- Sync handlers and sync subscribers run in the runtime thread pool executor.
- Long-running blocking sync work can reduce throughput for other sync handlers.
- Prefer `async def` handlers for I/O-bound work when possible.
