from collections.abc import Sequence
from typing import Any

from dispatchr.middleware import Middleware, compose_middleware
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import MessageRuntime


class MessageBus:
    def __init__(self, middleware: Sequence[Middleware] | None = None) -> None:
        self._registry = HandlerRegistry()
        self._runtime = MessageRuntime()
        self._middleware = list(middleware or [])

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    async def send(self, command: Any) -> Any:
        handler = self._registry.get_command_handler(type(command))

        async def final_handler(message: Any) -> Any:
            return await self._runtime.dispatch_command(handler, message)

        pipeline = compose_middleware(self._middleware, final_handler)
        return await pipeline(command)

    async def publish(self, event: Any) -> None:
        handlers = self._registry.get_event_handlers(type(event))

        async def final_handler(message: Any) -> None:
            await self._runtime.dispatch_event(handlers, message)

        pipeline = compose_middleware(self._middleware, final_handler)
        await pipeline(event)

    async def aclose(self) -> None:
        await self._runtime.aclose()
