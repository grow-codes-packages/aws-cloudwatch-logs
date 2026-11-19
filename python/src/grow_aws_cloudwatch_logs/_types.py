"""Public result and input types of the CloudWatch Logs service functions."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from datetime import datetime
from typing import Literal, TypeAlias

__all__ = [
    "EnsureLogGroupResult",
    "EnsureLogStreamResult",
    "FilterLogEventsPage",
    "FilteredLogEvent",
    "InputLogEvent",
    "LiveTailEvent",
    "LiveTailUpdate",
    "LogGroupInfo",
    "LogStreamInfo",
    "PutLogEventsResult",
    "QueryInfo",
    "QueryResults",
    "QueryStatistics",
    "QueryStatus",
    "QUERY_STATUSES",
    "QUERY_TERMINAL_STATUSES",
    "RejectedLogEventsInfo",
    "StreamLogEvent",
    "TimeInput",
]

#: A point in time: an aware ``datetime`` (naive = UTC) or epoch milliseconds.
TimeInput: TypeAlias = datetime | int | float

#: Logs Insights query status.
QueryStatus = Literal[
    "Scheduled", "Running", "Complete", "Failed", "Cancelled", "Timeout", "Unknown"
]

#: Every query status.
QUERY_STATUSES: tuple[str, ...] = (
    "Scheduled",
    "Running",
    "Complete",
    "Failed",
    "Cancelled",
    "Timeout",
    "Unknown",
)

#: Statuses after which a query does not change any more.
QUERY_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"Complete", "Failed", "Cancelled", "Timeout", "Unknown"}
)


@dataclasses.dataclass(frozen=True)
class InputLogEvent:
    """One log event to send with ``put_log_events``.

    Attributes:
        message: the message text.
        timestamp: event time (aware ``datetime`` or epoch milliseconds; ``None`` = now).
    """

    message: str
    timestamp: TimeInput | None = None


@dataclasses.dataclass(frozen=True)
class LogGroupInfo:
    """A log group (``DescribeLogGroups``).

    Attributes:
        name: log group name.
        arn: log group ARN without the trailing ``:*``.
        creation_time: creation time.
        retention_days: retention in days (``None`` = never expire).
        stored_bytes: stored bytes (approximate, eventually consistent).
        kms_key_id: KMS key ARN that encrypts the group.
        metric_filter_count: number of metric filters.
        log_group_class: ``STANDARD``, ``INFREQUENT_ACCESS`` …
    """

    name: str
    arn: str
    creation_time: datetime | None = None
    retention_days: int | None = None
    stored_bytes: int | None = None
    kms_key_id: str | None = None
    metric_filter_count: int | None = None
    log_group_class: str | None = None


@dataclasses.dataclass(frozen=True)
class LogStreamInfo:
    """A log stream (``DescribeLogStreams``).

    Attributes:
        name: log stream name.
        arn: log stream ARN.
        creation_time: creation time.
        first_event_time: time of the first event.
        last_event_time: time of the last event (eventually consistent on AWS).
        last_ingestion_time: last ingestion time.
    """

    name: str
    arn: str | None = None
    creation_time: datetime | None = None
    first_event_time: datetime | None = None
    last_event_time: datetime | None = None
    last_ingestion_time: datetime | None = None


@dataclasses.dataclass(frozen=True)
class FilteredLogEvent:
    """One event returned by ``filter_log_events``."""

    timestamp: datetime
    message: str
    ingestion_time: datetime | None = None
    log_stream_name: str | None = None
    event_id: str | None = None


@dataclasses.dataclass(frozen=True)
class FilterLogEventsPage:
    """One ``FilterLogEvents`` page; ``next_token`` is ``None`` at the end."""

    events: list[FilteredLogEvent]
    next_token: str | None = None


@dataclasses.dataclass(frozen=True)
class StreamLogEvent:
    """One event returned by ``get_log_events``."""

    timestamp: datetime
    message: str
    ingestion_time: datetime | None = None


@dataclasses.dataclass(frozen=True)
class RejectedLogEventsInfo:
    """Events of one batch the service did not store (indexes into the sorted batch).

    As documented by AWS the ``…end_index`` fields are inclusive (events ``0..index`` were
    rejected) and ``too_new_log_event_start_index`` is the first rejected event at the end.

    Attributes:
        batch: index of the batch within the call (0-based).
        too_old_log_event_end_index: last event older than 14 days or the retention.
        too_new_log_event_start_index: first event more than 2 hours in the future.
        expired_log_event_end_index: last event older than the retention period.
    """

    batch: int
    too_old_log_event_end_index: int | None = None
    too_new_log_event_start_index: int | None = None
    expired_log_event_end_index: int | None = None


@dataclasses.dataclass(frozen=True)
class PutLogEventsResult:
    """Result of ``put_log_events``.

    Attributes:
        accepted: events the service accepted.
        rejected: events the service rejected (too old, too new, expired; never retried).
        truncated: events truncated by the size policy before sending.
        dropped: events dropped by the size policy (``oversize_policy="drop"``).
        batches: PutLogEvents calls made.
        rejected_info: rejection details per batch that had rejections.
    """

    accepted: int = 0
    rejected: int = 0
    truncated: int = 0
    dropped: int = 0
    batches: int = 0
    rejected_info: list[RejectedLogEventsInfo] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(frozen=True)
class EnsureLogGroupResult:
    """Result of ``ensure_log_group``: whether this call created the group, and its ARN."""

    created: bool
    arn: str


@dataclasses.dataclass(frozen=True)
class EnsureLogStreamResult:
    """Result of ``ensure_log_stream``: whether this call created the stream."""

    created: bool


@dataclasses.dataclass(frozen=True)
class QueryStatistics:
    """Logs Insights scan statistics (reported by AWS, not by the emulator)."""

    records_matched: float = 0.0
    records_scanned: float = 0.0
    bytes_scanned: float = 0.0


@dataclasses.dataclass(frozen=True)
class QueryResults:
    """Result of ``get_query_results`` / ``query``.

    Attributes:
        query_id: the query id.
        status: query status.
        rows: result rows, field name → value (``@timestamp``, ``@message``, ``@ptr`` …).
        statistics: scan statistics when the service reports them.
    """

    query_id: str
    status: str
    rows: list[Mapping[str, str]] = dataclasses.field(default_factory=list)
    statistics: QueryStatistics | None = None


@dataclasses.dataclass(frozen=True)
class QueryInfo:
    """One Logs Insights query (``describe_queries``)."""

    query_id: str
    query_string: str
    status: str
    create_time: datetime | None = None
    log_group_name: str | None = None


@dataclasses.dataclass(frozen=True)
class LiveTailEvent:
    """One live-tail event."""

    timestamp: datetime
    message: str
    ingestion_time: datetime | None = None
    log_stream_name: str | None = None
    log_group_identifier: str | None = None


@dataclasses.dataclass(frozen=True)
class LiveTailUpdate:
    """One ``sessionUpdate`` frame: whether results were sampled and how many it held."""

    sampled: bool
    count: int
