import builtins
import logging
from datetime import datetime
from io import StringIO

import pytest

from dispatchbus import CommandBase, MessageBus, MessageMetadata, new_root_metadata
from dispatchbus.debug import DebugSubscriber, debug_subscriber_human, debug_subscriber_key_value
from dispatchbus.exceptions import InvalidMessageError
from dispatchbus.observability import (
    DispatchFinished,
    DispatchStarted,
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
)


async def sample_handler(message: object) -> None:
    return None


class AddUser(CommandBase):
    message_name = "user.add"

    def __init__(self, name: str, _metadata: MessageMetadata | None = None) -> None:
        self.name = name
        self._metadata = _metadata

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("AddUser is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "AddUser":
        return AddUser(self.name, _metadata=metadata)


class FlushTrackingStream(StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flush_calls = 0

    def flush(self) -> None:
        self.flush_calls += 1
        super().flush()


class RecordingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _handler_started() -> HandlerStarted:
    return HandlerStarted(
        message=object(),
        metadata=new_root_metadata(),
        message_type=object,
        operation="send",
        timestamp=datetime(2026, 1, 1),
        dispatch_id="dispatch-1",
        handler=sample_handler,
        handler_name="tests.sample_handler",
    )


def _handler_finished() -> HandlerFinished:
    return HandlerFinished(
        message=object(),
        metadata=new_root_metadata(),
        message_type=object,
        operation="send",
        timestamp=datetime(2026, 1, 1),
        dispatch_id="dispatch-1",
        handler=sample_handler,
        handler_name="tests.sample_handler",
        duration_ms=1.25,
    )


def _handler_failed() -> HandlerFailed:
    return HandlerFailed(
        message=object(),
        metadata=new_root_metadata(),
        message_type=object,
        operation="send",
        timestamp=datetime(2026, 1, 1),
        dispatch_id="dispatch-1",
        handler=sample_handler,
        handler_name="tests.sample_handler",
        duration_ms=2.5,
        error=ValueError("boom"),
    )


def _dispatch_started() -> DispatchStarted:
    return DispatchStarted(
        message=object(),
        metadata=new_root_metadata(),
        message_type=object,
        operation="send",
        timestamp=datetime(2026, 1, 1),
        dispatch_id="dispatch-1",
        handler_count=1,
    )


def _dispatch_finished() -> DispatchFinished:
    return DispatchFinished(
        message=object(),
        metadata=new_root_metadata(),
        message_type=object,
        operation="publish",
        timestamp=datetime(2026, 1, 1),
        dispatch_id="dispatch-2",
        handler_count=3,
        duration_ms=7.5,
        success=True,
    )


def test_debug_subscriber_human_stream_outputs_handler_events_only_by_default() -> None:
    stream = StringIO()
    subscriber = DebugSubscriber.human(stream=stream)

    subscriber(_dispatch_started())
    subscriber(_handler_started())
    subscriber(_handler_finished())
    subscriber(_handler_failed())

    output = stream.getvalue().splitlines()
    assert len(output) == 3
    assert output[0].startswith("START handler=tests.sample_handler")
    assert "operation=send" in output[0]
    assert output[1].startswith("DONE handler=tests.sample_handler")
    assert "duration_ms=1.25" in output[1]
    assert output[2].startswith("FAIL handler=tests.sample_handler")
    assert "ValueError('boom')" in output[2]


def test_debug_subscriber_include_dispatch_adds_dispatch_lines() -> None:
    stream = StringIO()
    subscriber = DebugSubscriber.human(stream=stream, include_dispatch=True)

    subscriber(_dispatch_started())
    subscriber(_dispatch_finished())

    output = stream.getvalue().splitlines()
    assert output[0].startswith("DISPATCH START message_type=object operation=send")
    assert output[1].startswith("DISPATCH DONE message_type=object operation=publish")
    assert "success=True" in output[1]


def test_debug_subscriber_key_value_stream_renders_machine_friendly_lines() -> None:
    stream = StringIO()
    subscriber = DebugSubscriber.key_value(stream=stream, include_dispatch=True)

    subscriber(_dispatch_started())
    subscriber(_handler_failed())

    output = stream.getvalue().splitlines()
    assert output[0].startswith("event=dispatch_started")
    assert "dispatch_id=dispatch-1" in output[0]
    assert output[1].startswith("event=handler_failed")
    assert "handler_name=tests.sample_handler" in output[1]
    assert "error_type=ValueError" in output[1]
    assert 'error="boom"' in output[1]


def test_debug_subscriber_ignores_unknown_events() -> None:
    stream = StringIO()
    subscriber = DebugSubscriber.human(stream=stream)

    subscriber(object())

    assert stream.getvalue() == ""


def test_debug_subscriber_flushes_after_each_stream_write() -> None:
    stream = FlushTrackingStream()
    subscriber = DebugSubscriber.human(stream=stream)

    subscriber(_handler_started())
    subscriber(_handler_finished())

    assert stream.flush_calls == 2


def test_debug_subscriber_logger_sink_emits_levels_messages_and_extra_fields() -> None:
    logger = logging.getLogger("dispatchbus.tests.debug")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    handler = RecordingHandler()
    logger.addHandler(handler)

    subscriber = DebugSubscriber.key_value(logger=logger, include_dispatch=True)

    subscriber(_dispatch_started())
    subscriber(_handler_failed())

    assert [record.levelno for record in handler.records] == [logging.DEBUG, logging.ERROR]
    assert handler.records[0].msg.startswith("event=dispatch_started")
    assert handler.records[0].dispatch_id == "dispatch-1"
    assert handler.records[1].handler_name == "tests.sample_handler"
    assert handler.records[1].error_type == "ValueError"


def test_top_level_factories_match_class_constructors() -> None:
    human = debug_subscriber_human(include_dispatch=True)
    human_class = DebugSubscriber.human(include_dispatch=True)
    key_value = debug_subscriber_key_value()
    key_value_class = DebugSubscriber.key_value()

    assert human == human_class
    assert key_value == key_value_class


def test_debug_subscriber_rich_console_writes_colored_human_output() -> None:
    Console = pytest.importorskip("rich.console").Console

    stream = StringIO()
    console = Console(file=stream, force_terminal=True, color_system="truecolor")
    subscriber = DebugSubscriber.human(console=console)

    subscriber(_handler_started())
    subscriber(_handler_failed())

    output = stream.getvalue()
    assert "START handler=tests.sample_handler" in output
    assert "FAIL handler=tests.sample_handler" in output
    assert "\x1b[" in output


def test_debug_subscriber_use_rich_without_rich_installed_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("rich"):
            raise ModuleNotFoundError("No module named 'rich'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    subscriber = DebugSubscriber.human(use_rich=True)

    with pytest.raises(RuntimeError, match="Rich output requested but rich is not installed"):
        subscriber(_handler_started())


@pytest.mark.asyncio
async def test_debug_subscriber_can_be_attached_to_message_bus() -> None:
    stream = StringIO()
    bus = MessageBus(subscribers=[DebugSubscriber.human(stream=stream, include_dispatch=True)])

    async def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(AddUser("ada", _metadata=new_root_metadata()))

    output = stream.getvalue()
    assert result == "ADA"
    assert "DISPATCH START" in output
    assert "START handler=" in output
    assert "DONE handler=" in output
    assert "DISPATCH DONE" in output
