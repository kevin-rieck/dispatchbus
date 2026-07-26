import asyncio
from collections import deque
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from dispatchbus.exceptions import EventPublicationError
from dispatchbus.messages import as_runtime_message, message_type_of, payload_of
from dispatchbus.middleware import Middleware, compose_middleware
from dispatchbus.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchbus.registry import HandlerRegistry
from dispatchbus.runtime import MessageRuntime


class EventPublisher:
    def __init__(
        self,
        *,
        registry: HandlerRegistry,
        runtime: MessageRuntime,
        middleware: Sequence[Middleware],
        subscribers: Sequence[Subscriber],
    ) -> None:
        self._registry = registry
        self._runtime = runtime
        self._middleware = tuple(middleware)
        self._subscribers = list(subscribers)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)

    async def publish(self, event: Any) -> None:
        if self._runtime._event_concurrency == "sequential":
            pending_events = deque([as_runtime_message(event)])
            failures: list[Exception] = []

            while pending_events:
                current_event = pending_events.popleft()
                follow_ups: list[Any] = []
                try:
                    _, event_failures = await self._publish_one_event(
                        current_event, collect_follow_ups=follow_ups
                    )
                except EventPublicationError as exc:
                    failures.extend(exc.failures)
                    pending_events.extend(follow_ups)
                except Exception as exc:
                    failures.append(exc)
                else:
                    pending_events.extend(follow_ups)
                    failures.extend(event_failures)

            if failures:
                raise EventPublicationError(failures)
        else:
            await self._publish_one_event(event)

    async def _publish_one_event(
        self,
        event: Any,
        collect_follow_ups: list[Any] | None = None,
    ) -> tuple[list[Any], list[Exception]]:
        runtime_event = as_runtime_message(event)
        payload = payload_of(runtime_event)
        metadata = runtime_event.metadata
        handlers = self._registry.get_event_handlers(message_type_of(runtime_event))
        dispatch_id = new_dispatch_id()
        started = asyncio.get_running_loop().time()
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchStarted(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation="publish",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
            ),
        )

        failures: list[Exception] = []

        async def final_handler(message: Any) -> None:
            if collect_follow_ups is not None:
                follow_ups_list = collect_follow_ups

                async def on_outcome(outcome: Any) -> None:
                    for emitted_event in outcome.emitted_events:
                        follow_ups_list.append(emitted_event)

                dispatch_outcome = await self._runtime.dispatch_event(
                    handlers,
                    runtime_event,
                    dispatch_id=dispatch_id,
                    subscribers=self._subscribers,
                    on_outcome=on_outcome,
                )
                failures.extend(dispatch_outcome.failures)
                if failures:
                    raise EventPublicationError(failures)
            else:
                follow_up_tasks: list[asyncio.Task[None]] = []

                async def on_outcome(outcome: Any) -> None:
                    for emitted_event in outcome.emitted_events:
                        follow_up_tasks.append(asyncio.create_task(self.publish(emitted_event)))

                dispatch_outcome = await self._runtime.dispatch_event(
                    handlers,
                    runtime_event,
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
            await pipeline(payload)
        except Exception:
            await self._runtime.notify_subscribers(
                self._subscribers,
                DispatchFinished(
                    message=payload,
                    metadata=metadata,
                    message_type=type(payload),
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
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation="publish",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
                duration_ms=(asyncio.get_running_loop().time() - started) * 1000,
                success=True,
            ),
        )
        return [], []
