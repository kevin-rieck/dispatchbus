import asyncio
from collections import deque
from datetime import datetime
from typing import Any

from dispatchr.exceptions import EventPublicationError
from dispatchr.middleware import Middleware, compose_middleware
from dispatchr.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import MessageRuntime


class EventPublisher:
    def __init__(
        self,
        *,
        registry: HandlerRegistry,
        runtime: MessageRuntime,
        middleware: list[Middleware],
        subscribers: list[Subscriber],
    ) -> None:
        self._registry = registry
        self._runtime = runtime
        self._middleware = middleware
        self._subscribers = subscribers

    async def publish(self, event: Any) -> None:
        pending_events = deque([event])
        failures: list[Exception] = []

        while pending_events:
            current_event = pending_events.popleft()
            try:
                emitted_events, event_failures = await self._publish_one_event(current_event)
            except EventPublicationError as exc:
                failures.extend(exc.failures)
            except Exception as exc:
                failures.append(exc)
            else:
                pending_events.extend(emitted_events)
                failures.extend(event_failures)

        if failures:
            raise EventPublicationError(failures)

    async def _publish_one_event(self, event: Any) -> tuple[list[Any], list[Exception]]:
        handlers = self._registry.get_event_handlers(type(event))
        dispatch_id = new_dispatch_id()
        started = asyncio.get_running_loop().time()
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchStarted(
                message=event,
                message_type=type(event),
                operation="publish",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
            ),
        )

        failures: list[Exception] = []

        async def final_handler(message: Any) -> None:
            follow_up_tasks: list[asyncio.Task[None]] = []

            async def on_outcome(outcome: Any) -> None:
                for emitted_event in outcome.emitted_events:
                    follow_up_tasks.append(asyncio.create_task(self.publish(emitted_event)))

            dispatch_outcome = await self._runtime.dispatch_event(
                handlers,
                message,
                dispatch_id=dispatch_id,
                subscribers=self._subscribers,
                on_outcome=on_outcome,
            )
            failures.extend(dispatch_outcome.failures)
            if follow_up_tasks:
                nested_results = await asyncio.gather(*follow_up_tasks, return_exceptions=True)
                for result in nested_results:
                    if isinstance(result, EventPublicationError):
                        failures.extend(result.failures)
                    elif isinstance(result, Exception):
                        failures.append(result)
            if failures:
                raise EventPublicationError(failures)

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            await pipeline(event)
        except Exception:
            await self._runtime.notify_subscribers(
                self._subscribers,
                DispatchFinished(
                    message=event,
                    message_type=type(event),
                    operation="publish",
                    timestamp=datetime.now(),
                    dispatch_id=dispatch_id,
                    handler_count=len(handlers),
                    duration_ms=(asyncio.get_running_loop().time() - started) * 1000,
                    success=False,
                ),
            )
            raise

        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchFinished(
                message=event,
                message_type=type(event),
                operation="publish",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
                duration_ms=(asyncio.get_running_loop().time() - started) * 1000,
                success=True,
            ),
        )
        return [], []
