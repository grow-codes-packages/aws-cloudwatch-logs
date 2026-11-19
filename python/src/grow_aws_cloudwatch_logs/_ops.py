"""The CloudWatch Logs operations, written once for the sync and the async client.

Every operation is a generator that *yields instructions* (:class:`Call` an AWS SDK method,
:class:`Sleep`, :class:`Emit` an item of an iterator) and receives the results; mapped SDK
errors are thrown back into it. ``CloudWatchLogsClient`` drives the generators with blocking
boto3 calls, ``AsyncCloudWatchLogsClient`` with awaited aiobotocore calls, so both clients have
exactly the same behaviour.
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import dataclasses
import json
import math
import re
import time
from collections.abc import Generator, Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypeVar

from ._aws_errors import aws_error_code
from ._batching import plan_log_event_batches, to_epoch_ms
from ._core import (
    CancelledError,
    CloudWatchLogsError,
    ConfigError,
    ConflictError,
    ErrorCode,
    HealthStatus,
    NotFoundError,
    TimeoutError,
    UnavailableError,
    ValidationError,
)
from ._emf import DEFAULT_EMF_NAMESPACE, build_emf
from ._logging import LogRecord, LogSearchResult, Severity
from ._types import (
    QUERY_TERMINAL_STATUSES,
    EnsureLogGroupResult,
    EnsureLogStreamResult,
    FilteredLogEvent,
    FilterLogEventsPage,
    InputLogEvent,
    LiveTailEvent,
    LogGroupInfo,
    LogStreamInfo,
    PutLogEventsResult,
    QueryInfo,
    QueryResults,
    QueryStatistics,
    RejectedLogEventsInfo,
    StreamLogEvent,
    TimeInput,
)
from .config import CloudWatchLogsConfig, is_valid_log_group_name, is_valid_log_stream_name

__all__ = [
    "DEFAULT_SEARCH_LOOKBACK_MS",
    "MAX_SEARCH_LIMIT",
    "RETENTION_DAYS",
    "SEARCH_FIRST_SLICE_MS",
    "SEARCH_MAX_SLICE_MS",
    "SEARCH_SCAN_CAP",
    "Call",
    "Emit",
    "Ops",
    "Sleep",
    "event_to_log_record",
    "parse_insights_timestamp",
    "partition_for_region",
    "to_filter_pattern",
]

T = TypeVar("T")

#: Allowed ``retention_days`` values.
RETENTION_DAYS: tuple[int, ...] = (
    1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557,
    2922, 3288, 3653,
)  # fmt: skip

#: Look-back of a descending search without ``start`` when the group's creation time is
#: unknown (30 days, ms). With a known creation time the window starts one hour before it.
DEFAULT_SEARCH_LOOKBACK_MS = 30 * 24 * 60 * 60 * 1000
#: First time slice of a descending search (5 minutes, ms); each further slice is 4x wider.
SEARCH_FIRST_SLICE_MS = 5 * 60_000
#: Largest time slice of a descending search (30 days, ms).
SEARCH_MAX_SLICE_MS = 30 * 24 * 60 * 60 * 1000
#: Events read per slice before the slice is narrowed (the newest half is kept).
SEARCH_SCAN_CAP = 10_000
#: Largest page size of ``search``.
MAX_SEARCH_LIMIT = 10_000

_NATIVE_PATTERN_START = ("{", "?", "[", "-", '"')
_INSIGHTS_TS = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z?$")


@dataclasses.dataclass(frozen=True)
class Call:
    """Call one AWS SDK method (``method`` is the boto3 name, ``params`` its keyword args).

    ``probe`` uses the one-attempt health client; ``best_effort`` runs even when the caller's
    cancel token is already cancelled (used to stop a query on cancellation).
    """

    op: str
    method: str
    params: Mapping[str, Any]
    probe: bool = False
    best_effort: bool = False


@dataclasses.dataclass(frozen=True)
class Sleep:
    """Wait ``seconds`` (cancellable)."""

    seconds: float


@dataclasses.dataclass(frozen=True)
class Emit:
    """Hand one item to the caller of an iterator operation."""

    item: Any


Instr = Call | Sleep | Emit
Op = Generator[Instr, Any, T]


# --------------------------------------------------------------------------- helpers


def _dt(ms: Any) -> datetime | None:
    if isinstance(ms, bool) or not isinstance(ms, (int, float)):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _dt0(ms: Any) -> datetime:
    return _dt(ms) or datetime.fromtimestamp(0, tz=timezone.utc)


def _strip_star(arn: str) -> str:
    return arn[:-2] if arn.endswith(":*") else arn


def _check_range(value: Any, low: int, high: int, name: str, op: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValidationError(f"{op}: {name} must be an integer from {low} to {high}")


def _check_group(name: Any, op: str) -> None:
    if not is_valid_log_group_name(name):
        raise ValidationError(f"{op}: log group name must be 1-512 characters of [.-_/#A-Za-z0-9]")


def _check_stream(name: Any, op: str) -> None:
    if not is_valid_log_stream_name(name):
        raise ValidationError(f"{op}: log stream name must be 1-512 characters without : or *")


def _check_retention(days: Any, op: str) -> None:
    if days is None or (days == 0 and not isinstance(days, bool)):
        return
    if isinstance(days, bool) or days not in RETENTION_DAYS:
        allowed = ", ".join(str(d) for d in RETENTION_DAYS)
        raise ValidationError(f"{op}: retention_days must be 0 or one of {allowed}")


def _same_kms_key(existing: str | None, requested: str) -> bool:
    if existing == requested:
        return True
    return bool(existing) and (
        str(existing).endswith("/" + requested) or str(existing).endswith(":" + requested)
    )


def _to_group(g: Mapping[str, Any]) -> LogGroupInfo:
    def opt_int(key: str) -> int | None:
        value = g.get(key)
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    return LogGroupInfo(
        name=str(g.get("logGroupName") or ""),
        arn=_strip_star(str(g.get("logGroupArn") or g.get("arn") or "")),
        creation_time=_dt(g.get("creationTime")),
        retention_days=opt_int("retentionInDays"),
        stored_bytes=opt_int("storedBytes"),
        kms_key_id=g.get("kmsKeyId") if isinstance(g.get("kmsKeyId"), str) else None,
        metric_filter_count=opt_int("metricFilterCount"),
        log_group_class=g.get("logGroupClass") if isinstance(g.get("logGroupClass"), str) else None,
    )


def _to_stream(s: Mapping[str, Any]) -> LogStreamInfo:
    return LogStreamInfo(
        name=str(s.get("logStreamName") or ""),
        arn=s.get("arn") if isinstance(s.get("arn"), str) else None,
        creation_time=_dt(s.get("creationTime")),
        first_event_time=_dt(s.get("firstEventTimestamp")),
        last_event_time=_dt(s.get("lastEventTimestamp")),
        last_ingestion_time=_dt(s.get("lastIngestionTime")),
    )


def parse_insights_timestamp(value: str) -> datetime | None:
    """Convert a Logs Insights ``@timestamp`` (``YYYY-MM-DD HH:mm:ss.SSS`` UTC on AWS, epoch
    milliseconds on the emulator) to an aware UTC ``datetime``; ``None`` when unparseable."""
    text = value.strip()
    if text.isdigit():
        return _dt(int(text))
    match = _INSIGHTS_TS.match(text)
    if match is None:
        return None
    day, hour, minute, second, frac = match.groups()
    try:
        base = datetime.strptime(f"{day} {hour}:{minute}:{second}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    micros = int((frac or "0")[:6].ljust(6, "0"))
    return base.replace(microsecond=micros, tzinfo=timezone.utc)


def partition_for_region(region: str) -> str:
    """AWS partition of a region (``aws``, ``aws-cn``, ``aws-us-gov``)."""
    if region.startswith("cn-"):
        return "aws-cn"
    if region.startswith("us-gov-"):
        return "aws-us-gov"
    return "aws"


def to_filter_pattern(query: str) -> str:
    """Convert a search query to a CloudWatch filter pattern.

    Empty → match everything; a query starting with ``{``, ``?``, ``[``, ``-`` or ``"`` is
    passed through as a native pattern; anything else becomes an exact phrase (``"…"``, inner
    quotes and backslashes escaped).
    """
    q = query.strip()
    if not q:
        return ""
    if q.startswith(_NATIVE_PATTERN_START):
        return q
    return '"' + q.replace("\\", "\\\\").replace('"', '\\"') + '"'


def event_to_log_record(event: FilteredLogEvent | LiveTailEvent) -> LogRecord:
    """Normalize a CloudWatch event: a message holding a normalized LogRecord JSON is decoded;
    any other message becomes ``body`` with severity ``INFO``. Attributes ``aws.log.stream``,
    ``aws.event.id`` (and ``aws.log.group`` for live tail) are added."""
    extra: dict[str, Any] = {}
    if event.log_stream_name is not None:
        extra["aws.log.stream"] = event.log_stream_name
    event_id = getattr(event, "event_id", None)
    if event_id is not None:
        extra["aws.event.id"] = event_id
    message = event.message
    if message.lstrip().startswith("{"):
        try:
            data = json.loads(message)
            if isinstance(data, dict):
                record = LogRecord.from_json(data)
                return dataclasses.replace(record, attributes={**record.attributes, **extra})
        except (ValueError, TypeError, ValidationError):
            pass
    group = getattr(event, "log_group_identifier", None)
    if group is not None:
        extra["aws.log.group"] = group
    return LogRecord(
        body=message,
        severity=Severity.INFO,
        severity_text="INFO",
        timestamp=event.timestamp,
        attributes=extra,
        observed_timestamp=event.ingestion_time,
    )


def _b64(cursor: Mapping[str, Any]) -> str:
    raw = json.dumps(cursor, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(text: str) -> dict[str, Any]:
    try:
        raw = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
        data = json.loads(raw.decode())
    except (ValueError, binascii.Error, UnicodeDecodeError):
        data = None
    if isinstance(data, dict):
        if data.get("o") == "a" and isinstance(data.get("t"), str):
            return {"o": "a", "t": data["t"]}
        nums = [data.get(k) for k in ("e", "w", "k")]
        if (
            data.get("o") == "d"
            and all(isinstance(n, int) and not isinstance(n, bool) for n in nums)
            and data["w"] > 0
            and data["k"] >= 0
        ):
            return {"o": "d", "e": data["e"], "w": data["w"], "k": data["k"]}
    raise ValidationError("logs.search: invalid cursor")


def _newest_first(events: list[FilteredLogEvent]) -> list[FilteredLogEvent]:
    return sorted(events, key=lambda e: (e.timestamp, e.event_id or ""), reverse=True)


# --------------------------------------------------------------------------- operations


class Ops:
    """Operation generators shared by the sync and async clients."""

    def __init__(self, config: CloudWatchLogsConfig) -> None:
        self.config = config
        #: Effective region (set by the client once its SDK client exists).
        self.region: str | None = config.region
        self._created_ms: dict[str, int] = {}

    def group_or_default(self, group: str | None, op: str) -> str:
        """``group`` or ``config.log_group`` (``ConfigError`` when neither is set)."""
        name = group if group is not None else self.config.log_group
        if name is None:
            raise ConfigError(f"{op}: no log group given and config.log_group is not set")
        return name

    # ------------------------------------------------------------------ core

    def health(self) -> Op[HealthStatus]:
        """Signed ``DescribeLogGroups {limit: 1}`` on the one-attempt probe client."""
        params: dict[str, Any] = {"limit": 1}
        if self.config.log_group is not None:
            params["logGroupNamePrefix"] = self.config.log_group
        try:
            yield Call("health", "describe_log_groups", params, probe=True)
        except CloudWatchLogsError as err:
            if err.code in (ErrorCode.CANCELLED, ErrorCode.CONFIG):
                raise
            if err.code in (ErrorCode.AUTHENTICATION, ErrorCode.PERMISSION_DENIED):
                return HealthStatus(True, "ok", {"auth": "failed", "code": err.code.value})
            if err.code == ErrorCode.RATE_LIMITED:
                return HealthStatus(True, "degraded", {"code": err.code.value})
            if err.http_status is not None:
                return HealthStatus(
                    False,
                    "unhealthy",
                    {"httpStatus": str(err.http_status), "error": err.code.value},
                )
            return HealthStatus(False, "unreachable", {"error": err.code.value})
        return HealthStatus(True, "ok", {})

    def ping(self) -> Op[float]:
        """Authenticated ``DescribeLogGroups {limit: 1}``; latency in milliseconds."""
        started = time.monotonic()
        yield Call("ping", "describe_log_groups", {"limit": 1})
        return (time.monotonic() - started) * 1000.0

    # ------------------------------------------------------------------ bootstrap

    def get_log_group(self, name: str, op: str = "get_log_group") -> Op[LogGroupInfo | None]:
        """The log group named exactly ``name`` (``None`` when missing)."""
        _check_group(name, op)
        token: str | None = None
        while True:
            params: dict[str, Any] = {"logGroupNamePrefix": name, "limit": 50}
            if token:
                params["nextToken"] = token
            out = yield Call(op, "describe_log_groups", params)
            for g in out.get("logGroups") or []:
                if g.get("logGroupName") == name:
                    return _to_group(g)
            token = out.get("nextToken")
            if not token:
                return None

    def require_group(self, name: str, op: str) -> Op[LogGroupInfo]:
        """Like :meth:`get_log_group` but ``NotFoundError`` when missing."""
        group = yield from self.get_log_group(name, op)
        if group is None:
            raise NotFoundError(f"{op}: log group not found")
        return group

    def ensure_log_group(
        self,
        name: str,
        retention_days: int | None = None,
        kms_key_id: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Op[EnsureLogGroupResult]:
        """Create the group when missing; on an existing group add tags, re-apply retention and
        raise ``ConflictError`` when the KMS key differs."""
        op = "ensure_log_group"
        _check_group(name, op)
        _check_retention(retention_days, op)
        tag_map = dict(tags or {})
        params: dict[str, Any] = {"logGroupName": name}
        if kms_key_id:
            params["kmsKeyId"] = kms_key_id
        if tag_map:
            params["tags"] = tag_map
        created = True
        try:
            yield Call(op, "create_log_group", params)
        except ConflictError as err:
            if aws_error_code(err) != "ResourceAlreadyExistsException":
                raise
            created = False
        group = yield from self.require_group(name, op)
        if not created:
            if kms_key_id and not _same_kms_key(group.kms_key_id, kms_key_id):
                raise ConflictError(f"{op}: the existing log group uses a different KMS key")
            if tag_map:
                yield Call(op, "tag_resource", {"resourceArn": group.arn, "tags": tag_map})
        if retention_days is not None:
            yield from self.put_retention_policy(name, retention_days, op)
        return EnsureLogGroupResult(created=created, arn=group.arn)

    def ensure_log_stream(
        self, group: str, stream: str, create_group: bool = False
    ) -> Op[EnsureLogStreamResult]:
        """Create the stream when missing (``NotFoundError`` for a missing group unless
        ``create_group``)."""
        op = "ensure_log_stream"
        _check_group(group, op)
        _check_stream(stream, op)
        if create_group:
            yield from self.ensure_log_group(group)
        try:
            yield Call(op, "create_log_stream", {"logGroupName": group, "logStreamName": stream})
        except ConflictError as err:
            if aws_error_code(err) != "ResourceAlreadyExistsException":
                raise
            return EnsureLogStreamResult(created=False)
        return EnsureLogStreamResult(created=True)

    def put_retention_policy(
        self, group: str, days: int | None, op: str = "put_retention_policy"
    ) -> Op[None]:
        """Set the retention; ``None`` or ``0`` removes it (never expire)."""
        _check_group(group, op)
        _check_retention(days, op)
        if not days:
            yield Call(op, "delete_retention_policy", {"logGroupName": group})
            return
        yield Call(op, "put_retention_policy", {"logGroupName": group, "retentionInDays": days})

    def delete_log_group(self, name: str, ignore_missing: bool = False) -> Op[bool]:
        """Delete a group with its streams and events."""
        op = "delete_log_group"
        _check_group(name, op)
        try:
            yield Call(op, "delete_log_group", {"logGroupName": name})
        except NotFoundError:
            if ignore_missing:
                return False
            raise
        return True

    def delete_log_stream(self, group: str, stream: str, ignore_missing: bool = False) -> Op[bool]:
        """Delete a stream."""
        op = "delete_log_stream"
        _check_group(group, op)
        _check_stream(stream, op)
        try:
            yield Call(op, "delete_log_stream", {"logGroupName": group, "logStreamName": stream})
        except NotFoundError:
            if ignore_missing:
                return False
            raise
        return True

    def describe_log_groups(
        self, prefix: str | None = None, page_size: int | None = None, limit: int | None = None
    ) -> Op[None]:
        """Emit log groups (sorted by name) across pages."""
        op = "describe_log_groups"
        _check_range(page_size, 1, 50, "page_size", op)
        _check_range(limit, 1, 2**63, "limit", op)
        token: str | None = None
        count = 0
        while True:
            params: dict[str, Any] = {"limit": page_size or 50}
            if prefix:
                params["logGroupNamePrefix"] = prefix
            if token:
                params["nextToken"] = token
            out = yield Call(op, "describe_log_groups", params)
            for g in out.get("logGroups") or []:
                yield Emit(_to_group(g))
                count += 1
                if limit is not None and count >= limit:
                    return
            token = out.get("nextToken")
            if not token:
                return

    def describe_log_streams(
        self,
        group: str,
        prefix: str | None = None,
        order_by: Literal["LogStreamName", "LastEventTime"] | None = None,
        descending: bool | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> Op[None]:
        """Emit the streams of a group across pages."""
        op = "describe_log_streams"
        _check_group(group, op)
        _check_range(page_size, 1, 50, "page_size", op)
        _check_range(limit, 1, 2**63, "limit", op)
        if order_by not in (None, "LogStreamName", "LastEventTime"):
            raise ValidationError(f"{op}: order_by must be LogStreamName or LastEventTime")
        if order_by == "LastEventTime" and prefix:
            raise ValidationError(f"{op}: prefix cannot be combined with order_by LastEventTime")
        token: str | None = None
        count = 0
        while True:
            params: dict[str, Any] = {"logGroupName": group, "limit": page_size or 50}
            if prefix:
                params["logStreamNamePrefix"] = prefix
            if order_by:
                params["orderBy"] = order_by
            if descending is not None:
                params["descending"] = descending
            if token:
                params["nextToken"] = token
            out = yield Call(op, "describe_log_streams", params)
            for s in out.get("logStreams") or []:
                yield Emit(_to_stream(s))
                count += 1
                if limit is not None and count >= limit:
                    return
            token = out.get("nextToken")
            if not token:
                return

    def resolve_arn(self, name: str, op: str) -> Op[str]:
        """ARN (without ``:*``) of a log group given by name or ARN."""
        if isinstance(name, str) and name.startswith("arn:"):
            return _strip_star(name)
        group = yield from self.require_group(name, op)
        return group.arn

    def tag_log_group(self, name: str, tags: Mapping[str, str]) -> Op[None]:
        """Add or overwrite tags (``TagResource``)."""
        op = "tag_log_group"
        arn = yield from self.resolve_arn(name, op)
        yield Call(op, "tag_resource", {"resourceArn": arn, "tags": dict(tags)})

    def untag_log_group(self, name: str, keys: Iterable[str]) -> Op[None]:
        """Remove tags (``UntagResource``)."""
        op = "untag_log_group"
        arn = yield from self.resolve_arn(name, op)
        yield Call(op, "untag_resource", {"resourceArn": arn, "tagKeys": list(keys)})

    def list_log_group_tags(self, name: str) -> Op[dict[str, str]]:
        """Tags of a group (``ListTagsForResource``)."""
        op = "list_log_group_tags"
        arn = yield from self.resolve_arn(name, op)
        out = yield Call(op, "list_tags_for_resource", {"resourceArn": arn})
        return dict(out.get("tags") or {})

    def log_group_arn(self, name: str, op: str = "log_group_arn") -> Op[str]:
        """ARN of a group: built from ``config.account_id`` + region when set, else looked up."""
        if isinstance(name, str) and name.startswith("arn:"):
            return _strip_star(name)
        _check_group(name, op)
        if self.config.account_id is not None and self.region:
            region = self.region
            partition = partition_for_region(region)
            return f"arn:{partition}:logs:{region}:{self.config.account_id}:log-group:{name}"
        return (yield from self.resolve_arn(name, op))

    # ------------------------------------------------------------------ ingest

    def put_log_events(
        self,
        group: str,
        stream: str,
        events: Iterable[InputLogEvent | Mapping[str, Any]],
        auto_create: bool | None = None,
        create_group: bool = False,
        max_event_bytes: int | None = None,
        oversize_policy: str | None = None,
    ) -> Op[PutLogEventsResult]:
        """Batch, size-limit and send events with ``PutLogEvents``."""
        op = "put_log_events"
        _check_group(group, op)
        _check_stream(stream, op)
        plan = plan_log_event_batches(
            events,
            max_event_bytes=max_event_bytes or self.config.max_event_bytes,
            oversize_policy=oversize_policy or self.config.oversize_policy,
        )
        auto = self.config.auto_create if auto_create is None else auto_create
        accepted = rejected = batches = 0
        infos: list[RejectedLogEventsInfo] = []
        for index, batch in enumerate(plan.batches):
            params = {"logGroupName": group, "logStreamName": stream, "logEvents": batch}
            try:
                out = yield Call(op, "put_log_events", params)
            except NotFoundError as err:
                if not auto:
                    raise
                try:
                    yield from self.ensure_log_stream(group, stream, create_group)
                except NotFoundError:
                    raise err from None
                out = yield Call(op, "put_log_events", params)
            batches += 1
            info = out.get("rejectedLogEventsInfo")
            count = 0
            if info:
                old_end = -1
                new_start = len(batch)
                too_old = info.get("tooOldLogEventEndIndex")
                expired = info.get("expiredLogEventEndIndex")
                too_new = info.get("tooNewLogEventStartIndex")
                for value in (too_old, expired):
                    if isinstance(value, int):
                        old_end = max(old_end, value)
                if isinstance(too_new, int):
                    new_start = min(new_start, too_new)
                # AWS documents the "end index" fields as inclusive.
                old = min(len(batch), old_end + 1)
                count = old + max(0, len(batch) - max(new_start, old))
                infos.append(
                    RejectedLogEventsInfo(
                        batch=index,
                        too_old_log_event_end_index=too_old if isinstance(too_old, int) else None,
                        too_new_log_event_start_index=too_new if isinstance(too_new, int) else None,
                        expired_log_event_end_index=expired if isinstance(expired, int) else None,
                    )
                )
            rejected += count
            accepted += len(batch) - count
        return PutLogEventsResult(
            accepted=accepted,
            rejected=rejected,
            truncated=plan.truncated,
            dropped=plan.dropped,
            batches=batches,
            rejected_info=infos,
        )

    def emf(
        self,
        metric: str,
        value: float | Sequence[float],
        dimensions: Mapping[str, str] | None,
        namespace: str = DEFAULT_EMF_NAMESPACE,
        unit: str = "None",
        timestamp: datetime | int | None = None,
        properties: Mapping[str, str | int | float | bool] | None = None,
        group: str | None = None,
        stream: str | None = None,
    ) -> Op[dict[str, Any]]:
        """Build an EMF document and send it as one event; returns the document."""
        target = self.group_or_default(group, "emf")
        doc = build_emf(
            metric,
            value,
            dimensions,
            namespace=namespace,
            unit=unit,
            timestamp=timestamp,
            properties=properties,
        )
        message = json.dumps(doc, separators=(",", ":"), ensure_ascii=False)
        result = yield from self.put_log_events(
            target,
            stream or self.config.log_stream,
            [InputLogEvent(message=message, timestamp=doc["_aws"]["Timestamp"])],
            oversize_policy="error",
        )
        if result.rejected:
            raise ValidationError("emf: the service rejected the event")
        return doc

    # ------------------------------------------------------------------ query

    def filter_log_events_page(
        self,
        group: str | None = None,
        streams: Sequence[str] | None = None,
        stream_prefix: str | None = None,
        pattern: str | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        next_token: str | None = None,
    ) -> Op[FilterLogEventsPage]:
        """One ``FilterLogEvents`` request; events ordered by timestamp, then event id."""
        op = "filter_log_events"
        name = self.group_or_default(group, op)
        _check_range(page_size, 1, 10_000, "page_size", op)
        if streams is not None and stream_prefix is not None:
            raise ValidationError(f"{op}: streams and stream_prefix are mutually exclusive")
        if streams is not None and not 1 <= len(streams) <= 100:
            raise ValidationError(f"{op}: streams must hold 1-100 names")
        params: dict[str, Any] = {}
        if name.startswith("arn:"):
            params["logGroupIdentifier"] = _strip_star(name)
        else:
            params["logGroupName"] = name
        if streams is not None:
            params["logStreamNames"] = list(streams)
        if stream_prefix is not None:
            params["logStreamNamePrefix"] = stream_prefix
        if pattern:
            params["filterPattern"] = pattern
        if start is not None:
            params["startTime"] = to_epoch_ms(start, "start", op)
        if end is not None:
            params["endTime"] = to_epoch_ms(end, "end", op)
        if page_size is not None:
            params["limit"] = page_size
        if next_token:
            params["nextToken"] = next_token
        out = yield Call(op, "filter_log_events", params)
        events = [
            FilteredLogEvent(
                timestamp=_dt0(ev.get("timestamp")),
                message=str(ev.get("message") or ""),
                ingestion_time=_dt(ev.get("ingestionTime")),
                log_stream_name=ev.get("logStreamName"),
                event_id=ev.get("eventId"),
            )
            for ev in out.get("events") or []
        ]
        events.sort(key=lambda e: (e.timestamp, e.event_id or ""))
        return FilterLogEventsPage(events=events, next_token=out.get("nextToken") or None)

    def filter_log_events(
        self,
        group: str | None = None,
        streams: Sequence[str] | None = None,
        stream_prefix: str | None = None,
        pattern: str | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> Op[None]:
        """Emit events matching a filter pattern across pages until no token is left."""
        _check_range(limit, 1, 2**63, "limit", "filter_log_events")
        token: str | None = None
        count = 0
        while True:
            size = page_size
            if limit is not None:
                size = min(limit - count, page_size or 10_000)
            page = yield from self.filter_log_events_page(
                group, streams, stream_prefix, pattern, start, end, size, token
            )
            for event in page.events:
                yield Emit(event)
                count += 1
                if limit is not None and count >= limit:
                    return
            token = page.next_token
            if token is None:
                return

    def get_log_events(
        self,
        group: str,
        stream: str,
        start_from_head: bool = False,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> Op[None]:
        """Emit the events of one stream (forward from the head, or newest page first)."""
        op = "get_log_events"
        _check_group(group, op)
        _check_stream(stream, op)
        _check_range(page_size, 1, 10_000, "page_size", op)
        _check_range(limit, 1, 2**63, "limit", op)
        base: dict[str, Any] = {
            "logGroupName": group,
            "logStreamName": stream,
            "startFromHead": bool(start_from_head),
        }
        if start is not None:
            base["startTime"] = to_epoch_ms(start, "start", op)
        if end is not None:
            base["endTime"] = to_epoch_ms(end, "end", op)
        if page_size is not None:
            base["limit"] = page_size
        token: str | None = None
        count = 0
        while True:
            params = dict(base)
            if token is not None:
                params["nextToken"] = token
            out = yield Call(op, "get_log_events", params)
            page = out.get("events") or []
            for ev in page:
                yield Emit(
                    StreamLogEvent(
                        timestamp=_dt0(ev.get("timestamp")),
                        message=str(ev.get("message") or ""),
                        ingestion_time=_dt(ev.get("ingestionTime")),
                    )
                )
                count += 1
                if limit is not None and count >= limit:
                    return
            nxt = out.get("nextForwardToken" if start_from_head else "nextBackwardToken")
            if not nxt or nxt == token or not page:
                return
            token = nxt

    def _query_params(
        self,
        query_string: str,
        groups: Sequence[str] | None,
        start: TimeInput | None,
        end: TimeInput | None,
        limit: int | None,
        op: str,
    ) -> dict[str, Any]:
        if not isinstance(query_string, str) or not query_string.strip():
            raise ValidationError(f"{op}: query_string must not be empty")
        names = list(groups) if groups is not None else [self.group_or_default(None, op)]
        if not 1 <= len(names) <= 50:
            raise ValidationError(f"{op}: groups must hold 1-50 log groups")
        _check_range(limit, 1, 10_000, "limit", op)
        end_ms = math.floor(time.time() * 1000) if end is None else to_epoch_ms(end, "end", op)
        start_ms = end_ms - 3_600_000 if start is None else to_epoch_ms(start, "start", op)
        if start_ms > end_ms:
            raise ValidationError(f"{op}: start must not be after end")
        params: dict[str, Any] = {
            "queryString": query_string,
            # StartQuery takes epoch SECONDS.
            "startTime": math.floor(start_ms / 1000),
            "endTime": math.ceil(end_ms / 1000),
        }
        if any(g.startswith("arn:") for g in names):
            params["logGroupIdentifiers"] = [_strip_star(g) for g in names]
        else:
            params["logGroupNames"] = names
        if limit is not None:
            params["limit"] = limit
        return params

    def start_query(
        self,
        query_string: str,
        groups: Sequence[str] | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        limit: int | None = None,
    ) -> Op[str]:
        """Start a Logs Insights query; returns its id."""
        op = "start_query"
        params = self._query_params(query_string, groups, start, end, limit, op)
        out = yield Call(op, "start_query", params)
        query_id = out.get("queryId")
        if not query_id:
            raise UnavailableError(f"{op}: no query id returned")
        return str(query_id)

    def get_query_results(self, query_id: str, op: str = "get_query_results") -> Op[QueryResults]:
        """Status and rows of a Logs Insights query."""
        if not isinstance(query_id, str) or not query_id:
            raise ValidationError(f"{op}: query_id must not be empty")
        out = yield Call(op, "get_query_results", {"queryId": query_id})
        rows = [
            {str(f["field"]): str(f.get("value") or "") for f in row if f.get("field") is not None}
            for row in out.get("results") or []
        ]
        stats = out.get("statistics")
        statistics = None
        if isinstance(stats, Mapping):
            statistics = QueryStatistics(
                records_matched=float(stats.get("recordsMatched") or 0),
                records_scanned=float(stats.get("recordsScanned") or 0),
                bytes_scanned=float(stats.get("bytesScanned") or 0),
            )
        return QueryResults(
            query_id=query_id,
            status=str(out.get("status") or "Unknown"),
            rows=list(rows),
            statistics=statistics,
        )

    def stop_query(self, query_id: str, best_effort: bool = False) -> Op[bool]:
        """Stop a running query; ``False`` when it had already finished."""
        op = "stop_query"
        if not isinstance(query_id, str) or not query_id:
            raise ValidationError(f"{op}: query_id must not be empty")
        out = yield Call(op, "stop_query", {"queryId": query_id}, best_effort=best_effort)
        return out.get("success") is True

    def _stop_quietly(self, query_id: str) -> Op[None]:
        with contextlib.suppress(CloudWatchLogsError):
            yield from self.stop_query(query_id, best_effort=True)

    def query(
        self,
        query_string: str,
        groups: Sequence[str] | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        limit: int | None = None,
        timeout_s: float | timedelta = 60.0,
        poll_interval_s: float | timedelta = 1.0,
        retry_on_query_limit: int = 3,
    ) -> Op[QueryResults]:
        """Run a Logs Insights query to completion (poll until a terminal status)."""
        op = "query"
        params = self._query_params(query_string, groups, start, end, limit, op)
        timeout = timeout_s.total_seconds() if isinstance(timeout_s, timedelta) else timeout_s
        interval = (
            poll_interval_s.total_seconds()
            if isinstance(poll_interval_s, timedelta)
            else float(poll_interval_s)
        )
        deadline = time.monotonic() + timeout
        attempts = max(1, retry_on_query_limit)
        attempt = 1
        while True:
            try:
                out = yield Call(op, "start_query", params)
                break
            except ValidationError as err:
                if (
                    aws_error_code(err) != "LimitExceededException"
                    or attempt >= attempts
                    or time.monotonic() >= deadline
                ):
                    raise
                yield Sleep(min(float(2 ** (attempt - 1)), 5.0))
                attempt += 1
        query_id = out.get("queryId")
        if not query_id:
            raise UnavailableError(f"{op}: no query id returned")
        while True:
            try:
                res = yield from self.get_query_results(query_id, op)
            except CancelledError:
                yield from self._stop_quietly(query_id)
                raise
            if res.status in QUERY_TERMINAL_STATUSES:
                if res.status == "Complete":
                    return res
                if res.status == "Timeout":
                    raise TimeoutError(f"{op}: query {query_id} timed out in the service")
                if res.status == "Cancelled":
                    raise CancelledError(f"{op}: query {query_id} was cancelled")
                raise UnavailableError(
                    f"{op}: query {query_id} ended with status {res.status}", retryable=False
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                yield from self._stop_quietly(query_id)
                raise TimeoutError(f"{op}: query {query_id} not complete within {timeout:g}s")
            try:
                yield Sleep(min(interval, remaining))
            except CancelledError:
                yield from self._stop_quietly(query_id)
                raise
            interval = min(interval * 2, 5.0)

    def describe_queries(
        self, group: str | None = None, status: str | None = None, max_results: int | None = None
    ) -> Op[list[QueryInfo]]:
        """Recent Logs Insights queries."""
        op = "describe_queries"
        _check_range(max_results, 1, 1000, "max_results", op)
        params: dict[str, Any] = {}
        if group is not None:
            params["logGroupName"] = group
        if status is not None:
            params["status"] = status
        if max_results is not None:
            params["maxResults"] = max_results
        out = yield Call(op, "describe_queries", params)
        result = []
        for q in out.get("queries") or []:
            created = q.get("createTime")
            result.append(
                QueryInfo(
                    query_id=str(q.get("queryId") or ""),
                    query_string=str(q.get("queryString") or ""),
                    status=str(q.get("status") or "Unknown"),
                    create_time=_dt(math.trunc(created))
                    if isinstance(created, (int, float))
                    else None,
                    log_group_name=q.get("logGroupName"),
                )
            )
        return result

    # ------------------------------------------------------------------ live tail

    def live_tail_params(
        self,
        groups: Sequence[str],
        streams: Sequence[str] | None,
        stream_prefixes: Sequence[str] | None,
        pattern: str | None,
    ) -> Op[dict[str, Any]]:
        """Validate a live-tail request and build the ``StartLiveTail`` parameters."""
        op = "start_live_tail"
        if isinstance(groups, str) or not 1 <= len(groups) <= 10:
            raise ValidationError(f"{op}: groups must hold 1-10 log groups")
        if streams is not None and stream_prefixes is not None:
            raise ValidationError(f"{op}: streams and stream_prefixes are mutually exclusive")
        if (streams is not None or stream_prefixes is not None) and len(groups) != 1:
            raise ValidationError(f"{op}: streams and stream_prefixes need exactly one log group")
        arns = []
        for g in groups:
            arns.append((yield from self.log_group_arn(g, op)))
        params: dict[str, Any] = {"logGroupIdentifiers": arns}
        if streams is not None:
            params["logStreamNames"] = list(streams)
        if stream_prefixes is not None:
            params["logStreamNamePrefixes"] = list(stream_prefixes)
        if pattern:
            params["logEventFilterPattern"] = pattern
        return params

    # ------------------------------------------------------------------ logging search

    def search(
        self,
        group: str,
        query: str,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        order: str,
        cursor: str | None,
    ) -> Op[LogSearchResult]:
        """``LogQueryClient.search`` over ``FilterLogEvents`` (DECISIONS 2)."""
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_SEARCH_LIMIT
        ):
            raise ValidationError(
                f"logs.search: limit must be an integer from 1 to {MAX_SEARCH_LIMIT}"
            )
        if order not in ("asc", "desc"):
            raise ValidationError("logs.search: order must be asc or desc")
        pattern = to_filter_pattern(query)
        parsed = None if cursor is None else _decode_cursor(cursor)
        if parsed is not None and parsed["o"] != order[0]:
            raise ValidationError("logs.search: the cursor belongs to the other order")
        if order == "asc":
            return (yield from self._search_asc(group, pattern, limit, start, end, parsed))
        return (yield from self._search_desc(group, pattern, limit, start, end, parsed))

    def _search_asc(
        self,
        group: str,
        pattern: str,
        limit: int,
        start: datetime | None,
        end: datetime | None,
        cursor: Mapping[str, Any] | None,
    ) -> Op[LogSearchResult]:
        records: list[LogRecord] = []
        token = None if cursor is None else cursor["t"]
        # FilterLogEvents can return empty pages with a token: read until ``limit`` or the end.
        while True:
            page = yield from self.filter_log_events_page(
                group, None, None, pattern, start, end, limit - len(records), token
            )
            records.extend(event_to_log_record(e) for e in page.events[: limit - len(records)])
            token = page.next_token
            if token is None or len(records) >= limit:
                break
        if token is None:
            return LogSearchResult(records=records)
        return LogSearchResult(records=records, next_cursor=_b64({"o": "a", "t": token}))

    def _default_start(self, group: str, end_ms: int) -> Op[int]:
        if group not in self._created_ms:
            info = yield from self.get_log_group(group, "logs.search")
            if info is not None and info.creation_time is not None:
                self._created_ms[group] = math.floor(info.creation_time.timestamp() * 1000)
        created = self._created_ms.get(group)
        if created is None:
            return end_ms - DEFAULT_SEARCH_LOOKBACK_MS
        return min(end_ms, created - 3_600_000)

    def _scan(self, group: str, pattern: str, start: int, end: int) -> Op[list[FilteredLogEvent]]:
        out: list[FilteredLogEvent] = []
        token: str | None = None
        while True:
            page = yield from self.filter_log_events_page(
                group, None, None, pattern, start, end, None, token
            )
            out.extend(page.events)
            token = page.next_token
            if token is None or len(out) > SEARCH_SCAN_CAP:
                return out[: SEARCH_SCAN_CAP + 1]

    def _search_desc(
        self,
        group: str,
        pattern: str,
        limit: int,
        start: datetime | None,
        end: datetime | None,
        cursor: Mapping[str, Any] | None,
    ) -> Op[LogSearchResult]:
        end_ms = to_epoch_ms(end or datetime.now(timezone.utc), "end", "logs.search")
        if start is not None:
            start_ms = to_epoch_ms(start, "start", "logs.search")
        else:
            start_ms = yield from self._default_start(group, end_ms)
        slice_end = end_ms + 1 if cursor is None else cursor["e"]
        width = SEARCH_FIRST_SLICE_MS if cursor is None else cursor["w"]
        skip = 0 if cursor is None else cursor["k"]
        records: list[LogRecord] = []
        while len(records) < limit and slice_end > start_ms:
            slice_start = max(start_ms, slice_end - width)
            events = yield from self._scan(group, pattern, slice_start, slice_end - 1)
            while len(events) > SEARCH_SCAN_CAP and slice_end - slice_start > 1000:
                width = max(1000, (slice_end - slice_start) // 2)
                slice_start = max(start_ms, slice_end - width)
                events = yield from self._scan(group, pattern, slice_start, slice_end - 1)
            available = _newest_first(events)[skip:]
            take = available[: limit - len(records)]
            records.extend(event_to_log_record(e) for e in take)
            if len(take) < len(available):
                nxt = {"o": "d", "e": slice_end, "w": width, "k": skip + len(take)}
                return LogSearchResult(records=records, next_cursor=_b64(nxt))
            slice_end = slice_start
            skip = 0
            width = min(width * 4, SEARCH_MAX_SLICE_MS)
        if slice_end > start_ms:
            nxt = {"o": "d", "e": slice_end, "w": width, "k": 0}
            return LogSearchResult(records=records, next_cursor=_b64(nxt))
        return LogSearchResult(records=records)
