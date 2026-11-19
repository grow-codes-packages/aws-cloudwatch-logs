"""Logging category for CloudWatch Logs: exporters (SPEC §4 ``LogExporter``) and query
clients (``LogQueryClient``, DECISIONS 2)."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ._batching import truncate_message, utf8_bytes
from ._core import CloudWatchLogsError, ValidationError
from ._logging import (
    AsyncBaseLogQueryClient,
    BaseLogQueryClient,
    ExportResult,
    LogRecord,
    LogSearchResult,
    SearchOrder,
)
from ._ops import event_to_log_record, to_filter_pattern
from ._types import InputLogEvent

if TYPE_CHECKING:
    from ._livetail import AsyncLiveTailSession, LiveTailSession
    from .client import AsyncCloudWatchLogsClient, CloudWatchLogsClient

__all__ = [
    "AsyncCloudWatchLogsExporter",
    "AsyncCloudWatchLogsQueryClient",
    "CloudWatchLogsExporter",
    "CloudWatchLogsQueryClient",
    "record_to_message",
]


def _dumps(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False, default=str)


def record_to_message(record: LogRecord, max_event_bytes: int, policy: str) -> str | None:
    """Serialize a record to its normalized JSON message.

    When the message is larger than ``max_event_bytes``, ``truncate`` shortens the record
    ``body`` so the message stays valid JSON (falling back to a plain cut only when the other
    fields alone are too large); ``drop`` and ``error`` return ``None``.
    """
    data = record.to_json()
    message = _dumps(data)
    size = utf8_bytes(message)
    if size <= max_event_bytes:
        return message
    if policy != "truncate":
        return None
    body = str(data["body"])
    while size > max_event_bytes and body:
        target = max(0, utf8_bytes(body) - (size - max_event_bytes) - 64)
        nxt = "" if target < 48 else truncate_message(body, target)
        body = nxt if len(nxt) < len(body) else ""
        message = _dumps({**data, "body": body})
        size = utf8_bytes(message)
    return message if size <= max_event_bytes else truncate_message(message, max_event_bytes)


def _events(
    records: Sequence[LogRecord], max_event_bytes: int, policy: str
) -> tuple[list[InputLogEvent], int]:
    events: list[InputLogEvent] = []
    oversized = 0
    for record in records:
        message = record_to_message(record, max_event_bytes, policy)
        if message is None:
            oversized += 1
        else:
            events.append(InputLogEvent(message=message, timestamp=record.timestamp))
    return events, oversized


def _result(accepted: int, rejected: int) -> ExportResult:
    if rejected == 0:
        return ExportResult(accepted=accepted)
    return ExportResult(
        accepted=accepted,
        rejected=rejected,
        error=ValidationError(
            f"logs.export: {rejected} records rejected (oversized, too old, too new or expired)"
        ),
    )


class CloudWatchLogsExporter:
    """Exports log records with ``PutLogEvents`` to one group / stream (sync).

    The message of each event is the normalized LogRecord JSON (redaction already applied by
    the log client); the event timestamp is the record timestamp in ms. The stream is created
    on first use (and the group with ``create_group``). ``gzip`` is ignored: PutLogEvents has no
    request compression in the AWS API model.
    """

    def __init__(
        self, client: CloudWatchLogsClient, group: str, stream: str, *, create_group: bool = False
    ) -> None:
        self.client = client
        self.group = group
        self.stream = stream
        self.create_group = create_group

    def export(
        self, records: Sequence[LogRecord], *, gzip: bool = False, timeout_s: float = 30.0
    ) -> ExportResult:
        """Send one batch; ``ExportResult.rejected`` counts oversized and rejected records."""
        cfg = self.client.config
        events, rejected = _events(records, cfg.max_event_bytes, cfg.oversize_policy)
        accepted = 0
        if events:
            result = self.client.put_log_events(
                self.group, self.stream, events, auto_create=True, create_group=self.create_group
            )
            accepted = result.accepted
            rejected += result.rejected
        return _result(accepted, rejected)

    def shutdown(self) -> None:
        """Nothing to release (the client owns the connection)."""


class AsyncCloudWatchLogsExporter:
    """Async :class:`CloudWatchLogsExporter` for :class:`AsyncCloudWatchLogsClient`."""

    def __init__(
        self,
        client: AsyncCloudWatchLogsClient,
        group: str,
        stream: str,
        *,
        create_group: bool = False,
    ) -> None:
        self.client = client
        self.group = group
        self.stream = stream
        self.create_group = create_group

    async def export(
        self, records: Sequence[LogRecord], *, gzip: bool = False, timeout_s: float = 30.0
    ) -> ExportResult:
        """Send one batch; ``ExportResult.rejected`` counts oversized and rejected records."""
        cfg = self.client.config
        events, rejected = _events(records, cfg.max_event_bytes, cfg.oversize_policy)
        accepted = 0
        if events:
            result = await self.client.put_log_events(
                self.group, self.stream, events, auto_create=True, create_group=self.create_group
            )
            accepted = result.accepted
            rejected += result.rejected
        return _result(accepted, rejected)

    async def shutdown(self) -> None:
        """Nothing to release (the client owns the connection)."""


class CloudWatchLogsQueryClient(BaseLogQueryClient):
    """``LogQueryClient`` over one log group (sync).

    ``search`` uses ``FilterLogEvents``: ``order="asc"`` reads natively (oldest first, cursor =
    the service token); ``order="desc"`` (default) walks the time window backwards in slices
    (5 min, each 4 times wider) from now to one hour before the group's creation time (else 30
    days), newest slice first, newest record first. A plain query is an exact phrase; a query
    starting with ``{ ? [ - "`` is a native filter pattern. ``tail`` uses Live Tail,
    ``list_streams`` ``DescribeLogStreams``; ``list_labels`` raises ``UnsupportedError``
    (CloudWatch Logs has no label inventory).
    """

    def __init__(self, client: CloudWatchLogsClient, group: str) -> None:
        self.client = client
        #: The log group searched.
        self.group = group

    def search(
        self,
        query: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100,
        order: SearchOrder = "desc",
        cursor: str | None = None,
    ) -> LogSearchResult:
        """One page of records matching ``query`` (see the class docs)."""
        return self.client._execute(
            self.client._ops.search(self.group, query, start, end, limit, order, cursor)
        )

    def tail(
        self,
        query: str,
        on_record: Callable[[LogRecord], None],
        *,
        interval_s: float = 1.0,
        on_error: Callable[[CloudWatchLogsError], None] | None = None,
    ) -> LiveTailSession:
        """Stream new records with Live Tail (filter pattern built like :meth:`search`).

        ``interval_s`` does not apply (the service pushes events). Errors that end the session
        go to ``on_error``; start-up errors are raised.
        """
        return self.client.start_live_tail(
            [self.group],
            lambda event: on_record(event_to_log_record(event)),
            pattern=to_filter_pattern(query),
            on_error=on_error,
        )

    def list_streams(self) -> list[str]:
        """Names of the streams of the group (all pages)."""
        return [s.name for s in self.client.describe_log_streams(self.group)]


class AsyncCloudWatchLogsQueryClient(AsyncBaseLogQueryClient):
    """Async :class:`CloudWatchLogsQueryClient` for :class:`AsyncCloudWatchLogsClient`."""

    def __init__(self, client: AsyncCloudWatchLogsClient, group: str) -> None:
        self.client = client
        #: The log group searched.
        self.group = group

    async def search(
        self,
        query: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100,
        order: SearchOrder = "desc",
        cursor: str | None = None,
    ) -> LogSearchResult:
        """One page of records matching ``query`` (see :class:`CloudWatchLogsQueryClient`)."""
        return await self.client._execute(
            self.client._ops.search(self.group, query, start, end, limit, order, cursor)
        )

    async def tail(
        self,
        query: str,
        on_record: Callable[[LogRecord], None],
        *,
        interval_s: float = 1.0,
        on_error: Callable[[CloudWatchLogsError], None] | None = None,
    ) -> AsyncLiveTailSession:
        """Stream new records with Live Tail (see :meth:`CloudWatchLogsQueryClient.tail`)."""
        return await self.client.start_live_tail(
            [self.group],
            lambda event: on_record(event_to_log_record(event)),
            pattern=to_filter_pattern(query),
            on_error=on_error,
        )

    async def list_streams(self) -> list[str]:
        """Names of the streams of the group (all pages)."""
        return [s.name async for s in self.client.describe_log_streams(self.group)]
