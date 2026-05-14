from typing import Any

from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import MessageRuntime


class MessageBus:
    def __init__(self) -> None:
        self._registry = HandlerRegistry()
        self._runtime = MessageRuntime()

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    async def send(self, command: Any) -> Any:
        handler = self._registry.get_command_handler(type(command))
        return await self._runtime.dispatch_command(handler, command)

    async def publish(self, event: Any) -> None:
        handlers = self._registry.get_event_handlers(type(event))
        await self._runtime.dispatch_event(handlers, event)

    async def aclose(self) -> None:
        await self._runtime.aclose()
