import asyncio

import pytest

from dispatchbus.sync_bridge import SyncBridge


def test_sync_bridge_runs_coroutine() -> None:
    bridge = SyncBridge()

    async def sample() -> str:
        return "ok"

    assert bridge.run(sample()) == "ok"
    bridge.close()


@pytest.mark.asyncio
async def test_sync_bridge_aclose_stops_loop() -> None:
    bridge = SyncBridge()

    assert bridge.run(asyncio.sleep(0, result="ok")) == "ok"

    await bridge.aclose()

    assert bridge._loop is None
