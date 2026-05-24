from datetime import datetime
from time import perf_counter
from typing import Any

from dispatchr.exceptions import EventPublicationError
from dispatchr.middleware import Middleware, compose_middleware
from dispatchr.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import MessageRuntime


class CommandDispatcher:
    def __init__(
        self,
        *,
        registry: HandlerRegistry,
        runtime: MessageRuntime,
        middleware: list[Middleware],
        subscribers: list[Subscriber],
        publish_event,
    ) -> None:
        self._registry = registry
        self._runtime = runtime
        self._middleware = middleware
        self._subscribers = subscribers
        self._publish_event = publish_event

    async def send(self, command: Any) -> Any:
        handler = self._registry.get_command_handler(type(command))
        dispatch_id = new_dispatch_id()
        started = perf_counter()
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchStarted(
                message=command,
                message_type=type(command),
                operation="send",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=1,
            ),
        )

        async def final_handler(message: Any) -> Any:
            outcome = await self._runtime.dispatch_command(
                handler,
                message,
                dispatch_id=dispatch_id,
                subscribers=self._subscribers,
            )
            failures: list[Exception] = []
            for emitted_event in outcome.emitted_events:
                try:
                    await self._publish_event(emitted_event)
                except EventPublicationError as exc:
                    failures.extend(exc.failures)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise EventPublicationError(failures)
            return outcome.result

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            result = await pipeline(command)
        except Exception:
            await self._runtime.notify_subscribers(
                self._subscribers,
                DispatchFinished(
                    message=command,
                    message_type=type(command),
                    operation="send",
                    timestamp=datetime.now(),
                    dispatch_id=dispatch_id,
                    handler_count=1,
                    duration_ms=(perf_counter() - started) * 1000,
                    success=False,
                ),
            )
            raise

        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchFinished(
                message=command,
                message_type=type(command),
                operation="send",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=1,
                duration_ms=(perf_counter() - started) * 1000,
                success=True,
            ),
        )
        return result
