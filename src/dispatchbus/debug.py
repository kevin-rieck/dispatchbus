from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import Any, Literal, TextIO

from dispatchbus.observability import (
    DispatchFinished,
    DispatchStarted,
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
)

FormatStyle = Literal["human", "key_value"]
HandledEvent = HandlerStarted | HandlerFinished | HandlerFailed | DispatchStarted | DispatchFinished


@dataclass(frozen=True)
class _RenderedEvent:
    text: str
    level: int
    extra: dict[str, Any]
    style: str | None = None


@dataclass(frozen=True)
class DebugSubscriber:
    _format: FormatStyle
    _include_dispatch: bool = False
    _logger: logging.Logger | None = None
    _stream: TextIO | None = None
    _console: Any = None
    _use_rich: bool = False

    def __call__(self, event: object) -> None:
        handled = self._filter_event(event)
        if handled is None:
            return
        rendered = self._render(handled)
        self._emit(rendered)

    @classmethod
    def human(
        cls,
        *,
        include_dispatch: bool = False,
        logger: logging.Logger | None = None,
        stream: TextIO | None = None,
        console: Any = None,
        use_rich: bool = False,
    ) -> DebugSubscriber:
        return cls(
            _format="human",
            _include_dispatch=include_dispatch,
            _logger=logger,
            _stream=stream,
            _console=console,
            _use_rich=use_rich,
        )

    @classmethod
    def key_value(
        cls,
        *,
        include_dispatch: bool = False,
        logger: logging.Logger | None = None,
        stream: TextIO | None = None,
        console: Any = None,
        use_rich: bool = False,
    ) -> DebugSubscriber:
        return cls(
            _format="key_value",
            _include_dispatch=include_dispatch,
            _logger=logger,
            _stream=stream,
            _console=console,
            _use_rich=use_rich,
        )

    def _filter_event(self, event: object) -> HandledEvent | None:
        if isinstance(event, (HandlerStarted, HandlerFinished, HandlerFailed)):
            return event
        if self._include_dispatch and isinstance(event, (DispatchStarted, DispatchFinished)):
            return event
        return None

    def _render(self, event: HandledEvent) -> _RenderedEvent:
        if self._format == "key_value":
            return _render_key_value(event)
        return _render_human(event)

    def _emit(self, rendered: _RenderedEvent) -> None:
        if self._console is not None or self._use_rich:
            self._emit_console(rendered)
            return
        if self._logger is not None:
            self._logger.log(rendered.level, rendered.text, extra=rendered.extra)
            return
        stream = self._stream or sys.stderr
        stream.write(rendered.text + "\n")
        stream.flush()

    def _emit_console(self, rendered: _RenderedEvent) -> None:
        console = self._console
        if console is None:
            console_class = _load_console_class()
            console = console_class(file=self._stream or sys.stderr)
        if self._format == "human":
            console.print(rendered.text, style=rendered.style, highlight=False)
            return
        console.print(rendered.text, highlight=False)


def debug_subscriber_human(**kwargs: Any) -> DebugSubscriber:
    return DebugSubscriber.human(**kwargs)


def debug_subscriber_key_value(**kwargs: Any) -> DebugSubscriber:
    return DebugSubscriber.key_value(**kwargs)


def _message_type_name(message_type: type[Any]) -> str:
    return message_type.__name__


def _base_extra(event: HandledEvent) -> dict[str, Any]:
    extra: dict[str, Any] = {
        "dispatch_id": event.dispatch_id,
        "operation": event.operation,
        "message_type": _message_type_name(event.message_type),
    }
    if isinstance(event, (HandlerStarted, HandlerFinished, HandlerFailed)):
        extra["handler_name"] = event.handler_name
    if isinstance(event, (DispatchStarted, DispatchFinished)):
        extra["handler_count"] = event.handler_count
    if isinstance(event, DispatchFinished):
        extra["duration_ms"] = event.duration_ms
        extra["success"] = event.success
    if isinstance(event, (HandlerFinished, HandlerFailed)):
        extra["duration_ms"] = event.duration_ms
    if isinstance(event, HandlerFailed):
        extra["error_type"] = type(event.error).__name__
    return extra


def _human_style(event: HandledEvent) -> str:
    if isinstance(event, HandlerStarted):
        return "cyan"
    if isinstance(event, HandlerFinished):
        return "green"
    if isinstance(event, HandlerFailed):
        return "bold red"
    if isinstance(event, DispatchStarted):
        return "magenta"
    return "bright_green"


def _join_fields(*fields: str) -> str:
    return " ".join(fields)


def _render_human(event: HandledEvent) -> _RenderedEvent:
    extra = _base_extra(event)
    message_type = _message_type_name(event.message_type)
    if isinstance(event, HandlerStarted):
        text = _join_fields(
            "START",
            f"handler={event.handler_name}",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
        )
        return _RenderedEvent(
            text=text, level=logging.DEBUG, extra=extra, style=_human_style(event)
        )
    if isinstance(event, HandlerFinished):
        text = _join_fields(
            "DONE",
            f"handler={event.handler_name}",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
            f"duration_ms={event.duration_ms:.2f}",
        )
        return _RenderedEvent(
            text=text, level=logging.DEBUG, extra=extra, style=_human_style(event)
        )
    if isinstance(event, HandlerFailed):
        text = _join_fields(
            "FAIL",
            f"handler={event.handler_name}",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
            f"duration_ms={event.duration_ms:.2f}",
            f"error={event.error!r}",
        )
        return _RenderedEvent(
            text=text, level=logging.ERROR, extra=extra, style=_human_style(event)
        )
    if isinstance(event, DispatchStarted):
        text = _join_fields(
            "DISPATCH START",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
            f"handler_count={event.handler_count}",
        )
        return _RenderedEvent(
            text=text, level=logging.DEBUG, extra=extra, style=_human_style(event)
        )
    text = _join_fields(
        "DISPATCH DONE",
        f"message_type={message_type}",
        f"operation={event.operation}",
        f"dispatch_id={event.dispatch_id}",
        f"handler_count={event.handler_count}",
        f"duration_ms={event.duration_ms:.2f}",
        f"success={event.success}",
    )
    return _RenderedEvent(text=text, level=logging.DEBUG, extra=extra, style=_human_style(event))


def _render_key_value(event: HandledEvent) -> _RenderedEvent:
    extra = _base_extra(event)
    message_type = _message_type_name(event.message_type)
    if isinstance(event, HandlerStarted):
        text = _join_fields(
            "event=handler_started",
            f"handler_name={event.handler_name}",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
        )
        return _RenderedEvent(text=text, level=logging.DEBUG, extra=extra)
    if isinstance(event, HandlerFinished):
        text = _join_fields(
            "event=handler_finished",
            f"handler_name={event.handler_name}",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
            f"duration_ms={event.duration_ms:.2f}",
        )
        return _RenderedEvent(text=text, level=logging.DEBUG, extra=extra)
    if isinstance(event, HandlerFailed):
        text = _join_fields(
            "event=handler_failed",
            f"handler_name={event.handler_name}",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
            f"duration_ms={event.duration_ms:.2f}",
            f"error_type={type(event.error).__name__}",
            f'error="{event.error}"',
        )
        return _RenderedEvent(text=text, level=logging.ERROR, extra=extra)
    if isinstance(event, DispatchStarted):
        text = _join_fields(
            "event=dispatch_started",
            f"message_type={message_type}",
            f"operation={event.operation}",
            f"dispatch_id={event.dispatch_id}",
            f"handler_count={event.handler_count}",
        )
        return _RenderedEvent(text=text, level=logging.DEBUG, extra=extra)
    text = _join_fields(
        "event=dispatch_finished",
        f"message_type={message_type}",
        f"operation={event.operation}",
        f"dispatch_id={event.dispatch_id}",
        f"handler_count={event.handler_count}",
        f"duration_ms={event.duration_ms:.2f}",
        f"success={event.success}",
    )
    return _RenderedEvent(text=text, level=logging.DEBUG, extra=extra)


def _load_console_class() -> type[Any]:
    try:
        from rich.console import Console
    except ModuleNotFoundError as exc:
        raise RuntimeError("Rich output requested but rich is not installed") from exc
    return Console
