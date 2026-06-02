def test_public_api_exports_message_bus() -> None:
    from dispatchbus import MessageBus

    assert MessageBus.__name__ == "MessageBus"


def test_public_api_exports_observability_events() -> None:
    from dispatchbus import (
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
    from dispatchbus.exceptions import BusDrainingError

    assert issubclass(BusDrainingError, Exception)


def test_package_exports_max_dispatch_chain_length_exception() -> None:
    from dispatchbus import MaxDispatchChainLengthExceededError

    assert MaxDispatchChainLengthExceededError.__name__ == ("MaxDispatchChainLengthExceededError")


def test_public_api_exports_message_primitives() -> None:
    from dispatchbus import (
        CommandBase,
        EventBase,
        MessageMetadata,
        get_metadata,
        new_root_metadata,
    )

    assert CommandBase.__name__ == "CommandBase"
    assert EventBase.__name__ == "EventBase"
    assert MessageMetadata.__name__ == "MessageMetadata"
    assert callable(new_root_metadata)
    assert callable(get_metadata)


def test_public_init_exports_get_metadata() -> None:
    from dispatchbus import get_metadata

    assert callable(get_metadata)
