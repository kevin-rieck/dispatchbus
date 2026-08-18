import asyncio

import pytest

from dispatchbus.exceptions import BusDrainingError
from dispatchbus.lifecycle import BusLifecycle


@pytest.mark.asyncio
async def test_lifecycle_command_scope_rejects_commands_during_draining() -> None:
    lifecycle = BusLifecycle()
    await lifecycle.begin_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        async with lifecycle.admit_command():
            pass


@pytest.mark.asyncio
async def test_lifecycle_allows_event_continuation_during_draining() -> None:
    lifecycle = BusLifecycle()

    async with lifecycle.admit_command():
        await lifecycle.begin_close(block=False)

        async with lifecycle.admit_event():
            pass


@pytest.mark.asyncio
async def test_lifecycle_rejects_root_events_during_draining() -> None:
    lifecycle = BusLifecycle()
    await lifecycle.begin_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        async with lifecycle.admit_event():
            pass


@pytest.mark.asyncio
async def test_lifecycle_rejects_events_after_close() -> None:
    lifecycle = BusLifecycle()
    await lifecycle.begin_close()
    await lifecycle.finish_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        async with lifecycle.admit_event():
            pass


@pytest.mark.asyncio
async def test_lifecycle_rejects_event_continuations_after_close() -> None:
    lifecycle = BusLifecycle()

    async with lifecycle.admit_command():
        await lifecycle.begin_close(block=False)

    await lifecycle.finish_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        async with lifecycle.admit_event():
            pass


@pytest.mark.asyncio
async def test_lifecycle_rejects_nested_commands_during_draining() -> None:
    lifecycle = BusLifecycle()

    async with lifecycle.admit_command():
        await lifecycle.begin_close(block=False)

        with pytest.raises(BusDrainingError, match="message bus is draining"):
            async with lifecycle.admit_command():
                pass


@pytest.mark.asyncio
async def test_lifecycle_child_tasks_inherit_admitted_context() -> None:
    lifecycle = BusLifecycle()
    event_admitted = asyncio.Event()

    async with lifecycle.admit_command():
        await lifecycle.begin_close(block=False)

        async def publish_from_child_task() -> None:
            async with lifecycle.admit_event():
                event_admitted.set()

        await asyncio.create_task(publish_from_child_task())

    assert event_admitted.is_set()


@pytest.mark.asyncio
async def test_lifecycle_releases_cancelled_command_scope() -> None:
    lifecycle = BusLifecycle()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def run_command() -> None:
        async with lifecycle.admit_command():
            entered.set()
            await release.wait()

    command = asyncio.create_task(run_command())
    await entered.wait()
    close = asyncio.create_task(lifecycle.begin_close())
    await asyncio.sleep(0)
    assert not close.done()

    command.cancel()

    with pytest.raises(asyncio.CancelledError):
        await command

    await asyncio.wait_for(close, timeout=1)
