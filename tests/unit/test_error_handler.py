from dispatchbus import MessageBus


def test_message_bus_accepts_error_handler() -> None:
    def dummy_error_handler(exc, message, handler, context) -> None:
        pass

    bus = MessageBus(error_handler=dummy_error_handler)
    assert bus._runtime._error_handler is dummy_error_handler
