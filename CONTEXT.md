# dispatchbus Context

`dispatchbus` is an in-memory Python message bus for applications that want explicit command and event dispatch without bringing in a framework.

## Language

**Message bus**:
The object applications use to send commands and publish events through registered handlers.
_Avoid_: Framework, broker

**Command**:
A message that expects exactly one registered handler and may return a result.
_Avoid_: Request, job

**Event**:
A message that may have zero, one, or many registered handlers and does not return a caller-visible result.
_Avoid_: Notification, signal

**Dispatch tree**:
A root command or event plus every follow-up event emitted by handlers during that accepted dispatch.
_Avoid_: Call graph, workflow

**Dispatch depth**:
The active nesting level of a command or event within a dispatch tree. The root has depth one; sibling events do not increase one another's depth.
_Avoid_: Dispatch chain length, tree size

**Follow-up event**:
An event emitted from a handler through the event context and causally linked to the handler's message.
_Avoid_: Nested event, sub-event

**Lifecycle admission**:
The decision that accepts or rejects root work and follow-up events while the message bus is open, draining, or closed.
_Avoid_: Shutdown gate, state check

**Dispatch trace**:
The observable record of dispatch and handler lifecycle facts emitted to subscribers.
_Avoid_: Logging, telemetry
