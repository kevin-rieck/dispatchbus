# dispatchr

Minimal Python project scaffolded for modern tooling:

- `uv` for environment management, dependency installation, locking, and builds
- `ruff` for linting and formatting

## Quick start

```powershell
uv sync --dev
uv run ruff check .
uv run ruff format .
uv run dispatchr
uv build
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
