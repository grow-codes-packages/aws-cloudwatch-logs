"""Client-side PutLogEvents batching (DECISIONS 5, API.md §2.2).

CloudWatch Logs rejects a PutLogEvents call with more than 10,000 events, more than 1,048,576
bytes (sum of UTF-8 message bytes + 26 per event), events spanning more than 24 hours or events
out of chronological order. These limits are enforced here, before anything is sent (the Moto
emulator enforces none of them).
"""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from ._core import ValidationError
from ._types import InputLogEvent, TimeInput
from .config import DEFAULT_MAX_EVENT_BYTES, MAX_EVENT_BYTES_HARD_CAP, OVERSIZE_POLICIES

__all__ = [
    "EVENT_OVERHEAD_BYTES",
    "MAX_BATCH_BYTES",
    "MAX_BATCH_SPAN_MS",
    "MAX_EVENTS_PER_BATCH",
    "BatchPlan",
    "plan_log_event_batches",
    "to_epoch_ms",
    "truncate_message",
    "utf8_bytes",
]

#: Most events in one PutLogEvents call.
MAX_EVENTS_PER_BATCH = 10_000
#: Most bytes in one PutLogEvents call (sum of UTF-8 message bytes + 26 per event).
MAX_BATCH_BYTES = 1_048_576
#: Bytes AWS adds to every event when it checks the batch size.
EVENT_OVERHEAD_BYTES = 26
#: Longest time span (last - first timestamp) of one batch: 24 hours, in milliseconds.
MAX_BATCH_SPAN_MS = 24 * 60 * 60 * 1000


@dataclasses.dataclass(frozen=True)
class BatchPlan:
    """Result of :func:`plan_log_event_batches`.

    Attributes:
        batches: batches in send order; each is sorted by timestamp, within every service
            limit, and holds wire events ``{"timestamp": <epoch ms>, "message": <str>}``.
        truncated: events that were truncated.
        dropped: events that were dropped (``oversize_policy="drop"``).
    """

    batches: list[list[dict[str, Any]]]
    truncated: int = 0
    dropped: int = 0


def utf8_bytes(text: str) -> int:
    """UTF-8 size of ``text`` in bytes."""
    return len(text.encode("utf-8"))


def _cut_utf8(data: bytes, max_bytes: int) -> str:
    end = min(len(data), max(0, max_bytes))
    while 0 < end < len(data) and (data[end] & 0xC0) == 0x80:
        end -= 1
    return data[:end].decode("utf-8")


def _marker(removed: int) -> str:
    return f" …[truncated {removed} bytes]"


def truncate_message(message: str, max_bytes: int) -> str:
    """Truncate ``message`` on a UTF-8 character boundary so the result, including the appended
    `` …[truncated N bytes]`` marker, is at most ``max_bytes`` UTF-8 bytes. ``N`` is the number
    of bytes removed from the original. Returns ``message`` unchanged when it already fits."""
    data = message.encode("utf-8")
    if len(data) <= max_bytes:
        return message
    # ``removed`` only grows (a longer marker leaves less room) and is bounded: this converges.
    removed = len(data) - max_bytes
    while True:
        kept = _cut_utf8(data, max_bytes - utf8_bytes(_marker(removed)))
        actual = len(data) - utf8_bytes(kept)
        if actual == removed:
            out = kept + _marker(actual)
            return out if utf8_bytes(out) <= max_bytes else _cut_utf8(out.encode(), max_bytes)
        removed = actual


def to_epoch_ms(value: TimeInput, name: str, operation: str) -> int:
    """Epoch milliseconds of a ``datetime`` (naive = UTC) or a number of milliseconds;
    ``ValidationError`` for anything else."""
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return math.floor(aware.timestamp() * 1000)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValidationError(f"{operation}: {name} is not a valid time")
    return math.trunc(value)


def now_ms() -> int:
    return math.floor(datetime.now(timezone.utc).timestamp() * 1000)


def _event_fields(event: Any, index: int) -> tuple[Any, Any]:
    if isinstance(event, InputLogEvent):
        return event.message, event.timestamp
    if isinstance(event, Mapping):
        return event.get("message"), event.get("timestamp")
    raise ValidationError(f"put_log_events: event {index} is not an InputLogEvent or mapping")


def plan_log_event_batches(
    events: Iterable[InputLogEvent | Mapping[str, Any]],
    *,
    max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES,
    oversize_policy: str = "truncate",
) -> BatchPlan:
    """Apply the per-event size policy, sort events by timestamp (stable) and split them into
    batches that respect every PutLogEvents limit.

    Events are :class:`InputLogEvent` objects or mappings with ``message`` and an optional
    ``timestamp`` (``None`` = now). Raises ``ValidationError`` for invalid timestamps,
    non-string messages, a bad ``max_event_bytes`` / ``oversize_policy``, or an oversized event
    with ``oversize_policy="error"``.
    """
    op = "put_log_events"
    if (
        isinstance(max_event_bytes, bool)
        or not isinstance(max_event_bytes, int)
        or not 1 <= max_event_bytes <= MAX_EVENT_BYTES_HARD_CAP
    ):
        raise ValidationError(
            f"{op}: max_event_bytes must be an integer from 1 to {MAX_EVENT_BYTES_HARD_CAP}"
        )
    if oversize_policy not in OVERSIZE_POLICIES:
        raise ValidationError(f"{op}: oversize_policy must be truncate, drop or error")
    truncated = dropped = 0
    now = now_ms()
    prepared: list[tuple[int, str, int]] = []
    for index, event in enumerate(events):
        message, timestamp = _event_fields(event, index)
        if not isinstance(message, str):
            raise ValidationError(f"{op}: event {index} has no string message")
        ts = now if timestamp is None else to_epoch_ms(timestamp, f"event {index} timestamp", op)
        size = utf8_bytes(message)
        if size > max_event_bytes:
            if oversize_policy == "error":
                raise ValidationError(
                    f"{op}: event {index} is {size} bytes, more than max_event_bytes "
                    f"{max_event_bytes}"
                )
            if oversize_policy == "drop":
                dropped += 1
                continue
            message = truncate_message(message, max_event_bytes)
            size = utf8_bytes(message)
            truncated += 1
        prepared.append((ts, message, size))
    prepared.sort(key=lambda item: item[0])
    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 0
    first = 0
    for ts, message, size in prepared:
        cost = size + EVENT_OVERHEAD_BYTES
        if current and (
            len(current) >= MAX_EVENTS_PER_BATCH
            or current_bytes + cost > MAX_BATCH_BYTES
            or ts - first > MAX_BATCH_SPAN_MS
        ):
            batches.append(current)
            current = []
            current_bytes = 0
        if not current:
            first = ts
        current.append({"timestamp": ts, "message": message})
        current_bytes += cost
    if current:
        batches.append(current)
    return BatchPlan(batches=batches, truncated=truncated, dropped=dropped)
