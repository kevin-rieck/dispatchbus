def test_public_api_exports_message_bus() -> None:
    from dispatchr import MessageBus

    assert MessageBus.__name__ == "MessageBus"


def test_public_api_exports_observability_events() -> None:
    from dispatchr import (
        DispatchFinished,
        DispatchStarted,
        HandlerFailed,
        HandlerFinished,
        HandlerStarted,
    )

    assert DispatchStarted.__name__ == "DispatchStarted"
    assert DispatchFinished.__name__ == "DispatchFinished"
    assert HandlerStarted.__name__ == "HandlerStarted"
    assert HandlerFinished.__name__ == "HandlerFinished"
    assert HandlerFailed.__name__ == "HandlerFailed"


def test_public_api_exports_bus_draining_error() -> None:
    from dispatchr.exceptions import BusDrainingError

    assert issubclass(BusDrainingError, Exception)
