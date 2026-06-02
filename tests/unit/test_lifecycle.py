import pytest

from dispatchbus.exceptions import BusDrainingError, MaxDispatchChainLengthExceededError
from dispatchbus.lifecycle import BusLifecycle


@pytest.mark.asyncio
async def test_lifecycle_rejects_send_during_draining() -> None:
    lifecycle = BusLifecycle()
    await lifecycle.begin_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await lifecycle.enter_send()


@pytest.mark.asyncio
async def test_lifecycle_allows_nested_publish_during_draining() -> None:
    lifecycle = BusLifecycle()
    token = await lifecycle.enter_send()
    await lifecycle.begin_close(block=False)

    nested = await lifecycle.enter_publish()
    await lifecycle.leave_dispatch(nested)
    await lifecycle.leave_dispatch(token)


@pytest.mark.asyncio
async def test_lifecycle_rejects_top_level_publish_during_draining() -> None:
    lifecycle = BusLifecycle()
    await lifecycle.begin_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await lifecycle.enter_publish()


@pytest.mark.asyncio
async def test_lifecycle_rejects_top_level_publish_after_close() -> None:
    lifecycle = BusLifecycle()
    await lifecycle.begin_close()
    await lifecycle.finish_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await lifecycle.enter_publish()


@pytest.mark.asyncio
async def test_lifecycle_rejects_nested_publish_after_close() -> None:
    lifecycle = BusLifecycle()
    token = await lifecycle.enter_send()
    await lifecycle.begin_close(block=False)
    await lifecycle.leave_dispatch(token)
    await lifecycle.finish_close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await lifecycle.enter_publish()


@pytest.mark.asyncio
async def test_lifecycle_rejects_send_during_draining_even_with_accepted_depth() -> None:
    lifecycle = BusLifecycle()
    token = await lifecycle.enter_send()
    await lifecycle.begin_close(block=False)

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await lifecycle.enter_send()

    await lifecycle.leave_dispatch(token)


@pytest.mark.asyncio
async def test_lifecycle_allows_root_dispatch_at_depth_one() -> None:
    lifecycle = BusLifecycle(max_dispatch_chain_length=1)

    token = await lifecycle.enter_publish()
    await lifecycle.leave_dispatch(token)


@pytest.mark.asyncio
async def test_lifecycle_rejects_nested_dispatch_past_limit() -> None:
    lifecycle = BusLifecycle(max_dispatch_chain_length=1)

    token = await lifecycle.enter_publish()
    try:
        with pytest.raises(MaxDispatchChainLengthExceededError) as exc_info:
            await lifecycle.enter_publish()
    finally:
        await lifecycle.leave_dispatch(token)

    assert exc_info.value.max_dispatch_chain_length == 1
    assert exc_info.value.attempted_depth == 2


@pytest.mark.asyncio
async def test_lifecycle_allows_new_root_dispatch_after_unwind() -> None:
    lifecycle = BusLifecycle(max_dispatch_chain_length=1)

    token = await lifecycle.enter_publish()
    await lifecycle.leave_dispatch(token)

    second = await lifecycle.enter_publish()
    await lifecycle.leave_dispatch(second)
