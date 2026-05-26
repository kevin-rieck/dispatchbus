import pytest

from dispatchbus.exceptions import BusDrainingError
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
