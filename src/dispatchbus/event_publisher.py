import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from typing import Any, TypeVar

from dispatchbus.exceptions import BusDrainingError, EventPublicationError
from dispatchbus.handler_runtime import HandlerOutcome, HandlerRuntime
from dispatchbus.lifecycle import BusLifecycle
from dispatchbus.messages import RuntimeMessage, as_runtime_message, message_type_of
from dispatchbus.observability import new_dispatch_id
from dispatchbus.registry import HandlerRegistry, RegisteredHandler

OutcomeCallback = Callable[[HandlerOutcome], Awaitable[None]]
RunDispatch = Callable[..., Awaitable[Any]]
TaskResult = TypeVar("TaskResult")


class EventPublisher:
    """Orchestrate event dispatch trees using state owned by a DispatchTree."""

    def __init__(
        self,
        *,
        registry: HandlerRegistry,
        handler_runtime: HandlerRuntime,
        lifecycle: BusLifecycle,
        event_concurrency: str,
        run_dispatch: RunDispatch,
    ) -> None:
        self._registry = registry
        self._handler_runtime = handler_runtime
        self._lifecycle = lifecycle
        self._run_dispatch = run_dispatch
        self._publish_events_impl: Callable[[Sequence[RuntimeMessage]], Awaitable[None]]
        self._dispatch_handlers_impl: Callable[..., Awaitable[list[Exception]]]
        if event_concurrency == "sequential":
            self._publish_events_impl = self._publish_events_sequential
            self._dispatch_handlers_impl = self._dispatch_handlers_sequential
        else:
            self._publish_events_impl = self._publish_events_concurrent
            self._dispatch_handlers_impl = self._dispatch_handlers_concurrent
        self._event_tasks: set[asyncio.Task[Any]] = set()

    async def publish(self, event: Any) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self.publish_events((as_runtime_message(event),))
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def publish_events(self, events: Sequence[RuntimeMessage]) -> None:
        await self._publish_events_impl(events)

    async def _publish_events_sequential(self, events: Sequence[RuntimeMessage]) -> None:
        pending = deque(events)
        failures: list[Exception] = []

        while pending:
            event = pending.popleft()
            follow_ups: list[RuntimeMessage] = []

            async def collect(
                outcome: HandlerOutcome, target: list[RuntimeMessage] = follow_ups
            ) -> None:
                target.extend(outcome.emitted_events)

            try:
                await self._publish_admitted_event(event, on_outcome=collect)
            except EventPublicationError as exc:
                failures.extend(exc.failures)
                pending.extend(follow_ups)
            except BusDrainingError:
                raise
            except Exception as exc:
                failures.append(exc)
            else:
                pending.extend(follow_ups)

        if failures:
            raise EventPublicationError(failures)

    async def _publish_events_concurrent(self, events: Sequence[RuntimeMessage]) -> None:
        tasks = [self._create_event_task(self._publish_concurrent_event(event)) for event in events]
        failures = await self._collect_publication_failures(tasks)
        for failure in failures:
            if isinstance(failure, BusDrainingError):
                raise failure
        if failures:
            raise EventPublicationError(failures)

    async def _publish_concurrent_event(self, event: RuntimeMessage) -> None:
        follow_up_tasks: list[asyncio.Task[None]] = []

        async def schedule(outcome: HandlerOutcome) -> None:
            follow_up_tasks.extend(
                self._create_event_task(self._publish_concurrent_event(emitted_event))
                for emitted_event in outcome.emitted_events
            )

        direct_failures: list[Exception] = []
        try:
            await self._publish_admitted_event(event, on_outcome=schedule)
        except EventPublicationError as exc:
            direct_failures.extend(exc.failures)
        except BusDrainingError:
            raise
        except Exception as exc:
            direct_failures.append(exc)

        follow_up_failures = await self._collect_publication_failures(follow_up_tasks)
        failures = direct_failures + follow_up_failures
        if failures:
            raise EventPublicationError(failures)

    async def _collect_publication_failures(
        self, tasks: Sequence[asyncio.Task[None]]
    ) -> list[Exception]:
        failures: list[Exception] = []
        for task in asyncio.as_completed(tasks):
            try:
                await task
            except EventPublicationError as exc:
                failures.extend(exc.failures)
            except Exception as exc:
                failures.append(exc)
        return failures

    async def _publish_admitted_event(
        self,
        event: RuntimeMessage,
        *,
        on_outcome: OutcomeCallback,
    ) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self._publish_one_event(event, on_outcome=on_outcome)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _publish_one_event(
        self,
        runtime_event: RuntimeMessage,
        *,
        on_outcome: OutcomeCallback,
    ) -> None:
        handlers = self._registry.get_event_handlers(message_type_of(runtime_event))
        dispatch_id = new_dispatch_id()

        async def final_handler(message: Any) -> None:
            failures = await self._dispatch_handlers(
                handlers,
                runtime_event,
                dispatch_id=dispatch_id,
                on_outcome=on_outcome,
            )
            if failures:
                raise EventPublicationError(failures)

        await self._run_dispatch(
            runtime_event,
            operation="publish",
            dispatch_id=dispatch_id,
            handler_count=len(handlers),
            final_handler=final_handler,
        )

    async def _dispatch_handlers(
        self,
        handlers: Sequence[RegisteredHandler],
        event: RuntimeMessage,
        *,
        dispatch_id: str,
        on_outcome: OutcomeCallback,
    ) -> list[Exception]:
        return await self._dispatch_handlers_impl(
            handlers,
            event,
            dispatch_id=dispatch_id,
            on_outcome=on_outcome,
        )

    async def _dispatch_handlers_sequential(
        self,
        handlers: Sequence[RegisteredHandler],
        event: RuntimeMessage,
        *,
        dispatch_id: str,
        on_outcome: OutcomeCallback,
    ) -> list[Exception]:
        failures: list[Exception] = []
        for handler in handlers:
            try:
                outcome = await self._handler_runtime.invoke(
                    handler,
                    event,
                    operation="publish",
                    dispatch_id=dispatch_id,
                )
                await on_outcome(outcome)
            except Exception as exc:
                failures.append(exc)
        return failures

    async def _dispatch_handlers_concurrent(
        self,
        handlers: Sequence[RegisteredHandler],
        event: RuntimeMessage,
        *,
        dispatch_id: str,
        on_outcome: OutcomeCallback,
    ) -> list[Exception]:
        tasks = [
            self._create_event_task(
                self._handler_runtime.invoke(
                    handler,
                    event,
                    operation="publish",
                    dispatch_id=dispatch_id,
                )
            )
            for handler in handlers
        ]
        failures: list[Exception] = []
        for task in asyncio.as_completed(tasks):
            try:
                outcome = await task
                await on_outcome(outcome)
            except Exception as exc:
                failures.append(exc)
        return failures

    def _create_event_task(
        self, coroutine: Coroutine[Any, Any, TaskResult]
    ) -> asyncio.Task[TaskResult]:
        task = asyncio.create_task(coroutine)
        self._event_tasks.add(task)
        task.add_done_callback(self._event_tasks.discard)
        return task

    async def drain(self) -> None:
        while self._event_tasks:
            await asyncio.gather(*tuple(self._event_tasks), return_exceptions=True)
