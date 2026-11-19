"""Amazon CloudWatch Logs clients (sync and async) on the official AWS SDK for Python.

:class:`CloudWatchLogsClient` uses boto3, :class:`AsyncCloudWatchLogsClient` aioboto3 /
aiobotocore (SPEC §2.3 "Cloud SDK services"). Both run the same operation code
(:mod:`._ops`), so names, defaults, results and errors are identical; only the I/O differs.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import threading
import time
from collections.abc import (
    AsyncIterator,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from datetime import datetime, timedelta
from types import TracebackType
from typing import Any, Literal, TypeVar

import boto3
import botocore.session
from botocore.credentials import AssumeRoleCredentialFetcher, DeferredRefreshableCredentials

from ._aws_errors import map_aws_error
from ._core import (
    CancelledError,
    CancelToken,
    CloudWatchLogsError,
    ConfigError,
    HealthStatus,
    TimeoutError,
    UnavailableError,
    UnsupportedError,
)
from ._core.hooks import SdkLogger
from ._cwl_logging import (
    AsyncCloudWatchLogsExporter,
    AsyncCloudWatchLogsQueryClient,
    CloudWatchLogsExporter,
    CloudWatchLogsQueryClient,
)
from ._emf import DEFAULT_EMF_NAMESPACE
from ._livetail import AsyncLiveTailSession, LiveTailSession, _Dispatcher
from ._logging import (
    AsyncBatchingLogClient,
    AttrValue,
    BatchingLogClient,
    BatchOptions,
    RedactionProcessor,
)
from ._ops import Call, Emit, Ops, Sleep
from ._sdk import CallState, HookBridge, TlsFiles, bind_state, botocore_config
from ._types import (
    EnsureLogGroupResult,
    EnsureLogStreamResult,
    FilteredLogEvent,
    FilterLogEventsPage,
    InputLogEvent,
    LiveTailEvent,
    LiveTailUpdate,
    LogGroupInfo,
    LogStreamInfo,
    PutLogEventsResult,
    QueryInfo,
    QueryResults,
    StreamLogEvent,
    TimeInput,
)
from .config import CloudWatchLogsConfig

__all__ = ["AsyncCloudWatchLogsClient", "ClientInternals", "CloudWatchLogsClient"]

T = TypeVar("T")

_DEFAULT_REDACTION = RedactionProcessor()


@dataclasses.dataclass(frozen=True)
class ClientInternals:
    """Test seams; not part of the stable API.

    ``inject_host_prefix=False`` stops botocore from sending StartLiveTail to
    ``streaming-<host>`` (needed only for local mock servers); ``sleep`` replaces the wait
    between Logs Insights polls.
    """

    inject_host_prefix: bool = True
    sleep: Callable[[float], None] | None = None


class _Base:
    """Configuration, operations and hook plumbing shared by both clients."""

    def __init__(
        self, config: CloudWatchLogsConfig | None, internals: ClientInternals | None
    ) -> None:
        if config is None:
            config = CloudWatchLogsConfig.from_env()
        self.config = config.validate()
        self._internals = internals or ClientInternals()
        self._ops = Ops(self.config)
        self._log = SdkLogger(self.config.logger, self.config.known_secrets)
        self._bridge = HookBridge(self.config, self._log)
        self._owns_sdk = self.config.sdk_client is None
        self._tls: TlsFiles | None = None
        self._closed = False

    def _secrets(self) -> list[str]:
        return self.config.known_secrets()

    def _state(self, call: Call, cancel: CancelToken | None) -> CallState:
        total = self.config.total_timeout
        return CallState(
            op=call.op,
            total_timeout=total,
            deadline=time.monotonic() + total,
            cancel=None if call.best_effort else cancel,
        )

    def _closed_error(self, op: str) -> CancelledError:
        return CancelledError(f"{op}: client is closed")

    def _client_kwargs(self, probe: bool) -> dict[str, Any]:
        if self._tls is None:
            self._tls = TlsFiles(self.config, self._log)
        kwargs: dict[str, Any] = {
            "endpoint_url": self.config.endpoint,
            "config": botocore_config(
                self.config,
                probe=probe,
                inject_host_prefix=self._internals.inject_host_prefix,
            ),
        }
        if self._tls.verify is not None:
            kwargs["verify"] = self._tls.verify
        return kwargs

    def _role_args(self) -> dict[str, Any]:
        extra: dict[str, Any] = {"RoleSessionName": self.config.role_session_name}
        if self.config.external_id is not None:
            extra["ExternalId"] = self.config.external_id.reveal()
        return extra

    def _session_kwargs(self) -> dict[str, Any]:
        cfg = self.config
        kwargs: dict[str, Any] = {}
        if cfg.region is not None:
            kwargs["region_name"] = cfg.region
        if cfg.access_key_id is not None and cfg.secret_access_key is not None:
            kwargs["aws_access_key_id"] = cfg.access_key_id
            kwargs["aws_secret_access_key"] = cfg.secret_access_key.reveal()
            if cfg.session_token is not None:
                kwargs["aws_session_token"] = cfg.session_token.reveal()
        return kwargs

    @staticmethod
    def _unsupported_version() -> UnsupportedError:
        return UnsupportedError(
            "server_version: Amazon CloudWatch Logs does not expose a server version"
        )


# =============================================================================== sync


class CloudWatchLogsClient(_Base):
    """Synchronous Amazon CloudWatch Logs client (boto3). Thread-safe; use as a context manager
    or call :meth:`close`.

    Every I/O method accepts ``cancel=`` (a :class:`CancelToken`) to abort from another thread
    (the abandoned AWS call finishes in the background). Iterator methods (``describe_*``,
    ``filter_log_events``, ``get_log_events``) fetch pages lazily while you iterate.
    """

    def __init__(
        self,
        config: CloudWatchLogsConfig | None = None,
        *,
        internals: ClientInternals | None = None,
    ) -> None:
        super().__init__(config, internals)
        self._lock = threading.Lock()
        self._sdk: Any = self.config.sdk_client
        self._probe: Any = self.config.sdk_client
        self._log_clients: list[BatchingLogClient] = []
        self._tails: set[LiveTailSession] = set()
        if self._sdk is not None:
            self._ops.region = getattr(getattr(self._sdk, "meta", None), "region_name", None)

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None, **overrides: Any
    ) -> CloudWatchLogsClient:
        """Build a client from ``GROW_AWS_CLOUDWATCH_LOGS_*`` and the standard AWS variables."""
        return cls(CloudWatchLogsConfig.from_env(env, **overrides))

    # ------------------------------------------------------------------ SDK plumbing

    def _build(self, probe: bool) -> Any:
        bs = botocore.session.Session(profile=self.config.profile)
        session = boto3.session.Session(botocore_session=bs, **self._session_kwargs())
        if self.config.role_arn is not None:
            fetcher = AssumeRoleCredentialFetcher(
                client_creator=bs.create_client,
                source_credentials=bs.get_credentials(),
                role_arn=self.config.role_arn,
                extra_args=self._role_args(),
            )
            # botocore has no public setter for refreshable session credentials.
            bs._credentials = DeferredRefreshableCredentials(
                method="assume-role", refresh_using=fetcher.fetch_credentials
            )
        client = session.client("logs", **self._client_kwargs(probe))
        self._bridge.register(client, probe=probe)
        return client

    def _client(self, probe: bool, op: str) -> Any:
        current = self._probe if probe else self._sdk
        if current is not None:
            return current
        with self._lock:
            current = self._probe if probe else self._sdk
            if current is None:
                try:
                    current = self._build(probe)
                except Exception as exc:  # noqa: BLE001 - every SDK failure is mapped
                    mapped = map_aws_error(op, exc, self._secrets())
                    raise mapped from mapped.cause
                if probe:
                    self._probe = current
                else:
                    self._sdk = current
                self._ops.region = self.config.region or current.meta.region_name
            return current

    @property
    def sdk(self) -> Any:
        """The underlying boto3 ``logs`` client (for operations this package does not wrap)."""
        return self._client(False, "sdk")

    @property
    def region(self) -> str | None:
        """The effective AWS region."""
        return self.config.region or self._client(False, "region").meta.region_name

    def _invoke(self, call: Call, cancel: CancelToken | None) -> Any:
        if self._closed and not call.best_effort:
            raise self._closed_error(call.op)
        sdk = self._client(call.probe, call.op)
        fn = getattr(sdk, call.method)
        state = self._state(call, cancel)

        def run() -> Any:
            restore = bind_state(state)
            try:
                return fn(**call.params)
            finally:
                restore()

        try:
            if state.cancel is None:
                return run()
            state.cancel.raise_if_cancelled(call.op)
            return self._run_cancellable(run, state.cancel, call.op)
        except CloudWatchLogsError:
            raise
        except Exception as exc:  # noqa: BLE001 - every SDK failure is mapped
            mapped = map_aws_error(call.op, exc, self._secrets())
            raise mapped from mapped.cause

    @staticmethod
    def _run_cancellable(run: Callable[[], Any], cancel: CancelToken, op: str) -> Any:
        done = threading.Event()
        box: dict[str, Any] = {}
        ctx = contextvars.copy_context()

        def work() -> None:
            try:
                box["value"] = ctx.run(run)
            except BaseException as exc:  # noqa: BLE001 - re-raised in the caller thread
                box["error"] = exc
            finally:
                done.set()

        unregister = cancel.on_cancel(done.set)
        threading.Thread(target=work, name="grow-cwl-call", daemon=True).start()
        done.wait()
        unregister()
        if "value" in box:
            return box["value"]
        if "error" in box:
            raise box["error"]
        raise CancelledError(f"{op}: cancelled")

    def _sleep(self, seconds: float, cancel: CancelToken | None, op: str) -> None:
        if self._internals.sleep is not None:
            self._internals.sleep(seconds)
        elif cancel is not None:
            if cancel.wait(seconds):
                raise CancelledError(f"{op}: cancelled")
        else:
            time.sleep(seconds)
        if cancel is not None and cancel.cancelled:
            raise CancelledError(f"{op}: cancelled")

    def _step(self, instr: Any, cancel: CancelToken | None) -> Any:
        if isinstance(instr, Call):
            return self._invoke(instr, cancel)
        if isinstance(instr, Sleep):
            self._sleep(instr.seconds, cancel, "sleep")
            return None
        raise TypeError(f"unexpected instruction {instr!r}")  # pragma: no cover

    def _execute(self, gen: Generator[Any, Any, T], cancel: CancelToken | None = None) -> T:
        value: Any = None
        error: BaseException | None = None
        while True:
            try:
                instr = gen.throw(error) if error is not None else gen.send(value)
            except StopIteration as stop:
                return stop.value  # type: ignore[no-any-return]
            value = error = None
            try:
                value = self._step(instr, cancel)
            except CloudWatchLogsError as exc:
                error = exc

    def _iterate(self, gen: Generator[Any, Any, None], cancel: CancelToken | None) -> Iterator[Any]:
        value: Any = None
        error: BaseException | None = None
        try:
            while True:
                try:
                    instr = gen.throw(error) if error is not None else gen.send(value)
                except StopIteration:
                    return
                value = error = None
                if isinstance(instr, Emit):
                    yield instr.item
                    continue
                try:
                    value = self._step(instr, cancel)
                except CloudWatchLogsError as exc:
                    error = exc
        finally:
            gen.close()

    # ------------------------------------------------------------------ core

    def health(self, *, cancel: CancelToken | None = None) -> HealthStatus:
        """Liveness via a signed ``DescribeLogGroups {limit: 1}`` without retries.

        Never raises for an unhealthy or unreachable service (only ``CANCELLED`` / ``CONFIG``).
        An authentication or permission error still counts as healthy (``details["auth"] ==
        "failed"``: the service answered); throttling reports ``degraded``.
        """
        return self._execute(self._ops.health(), cancel)

    def ping(self, *, cancel: CancelToken | None = None) -> float:
        """Authenticated round trip; latency in milliseconds. ``AuthenticationError`` on bad
        credentials, ``PermissionDeniedError`` without ``logs:DescribeLogGroups``."""
        return self._execute(self._ops.ping(), cancel)

    def server_version(self, *, cancel: CancelToken | None = None) -> str:
        """Always :class:`UnsupportedError`: CloudWatch Logs exposes no server version (see
        ``API_VERSION``)."""
        raise self._unsupported_version()

    def close(self) -> None:
        """Flush and close every log client from :meth:`log_client`, stop live-tail sessions
        and release connections. Idempotent; never closes a caller-supplied SDK client."""
        if self._closed:
            return
        for lc in list(self._log_clients):
            with contextlib.suppress(Exception):
                lc.close()
        for tail in list(self._tails):
            with contextlib.suppress(Exception):
                tail.close()
        self._closed = True
        if self._owns_sdk:
            for client in {id(c): c for c in (self._sdk, self._probe) if c is not None}.values():
                with contextlib.suppress(Exception):
                    client.close()
        if self._tls is not None:
            self._tls.close()

    def __enter__(self) -> CloudWatchLogsClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"CloudWatchLogsClient(config={self.config!r})"

    # ------------------------------------------------------------------ bootstrap

    def get_log_group(self, name: str, *, cancel: CancelToken | None = None) -> LogGroupInfo | None:
        """The log group named exactly ``name`` (``None`` when it does not exist)."""
        return self._execute(self._ops.get_log_group(name), cancel)

    def ensure_log_group(
        self,
        name: str,
        *,
        retention_days: int | None = None,
        kms_key_id: str | None = None,
        tags: Mapping[str, str] | None = None,
        cancel: CancelToken | None = None,
    ) -> EnsureLogGroupResult:
        """Create the log group when missing (``CreateLogGroup``), then apply retention / tags.

        On an existing group the requested ``tags`` are added (never removed) and the retention
        is re-applied; a different ``kms_key_id`` raises ``ConflictError``. ``retention_days``:
        ``None`` = leave unchanged, ``0`` = never expire, else one of ``RETENTION_DAYS``.
        """
        return self._execute(
            self._ops.ensure_log_group(name, retention_days, kms_key_id, tags), cancel
        )

    def ensure_log_stream(
        self,
        group: str,
        stream: str,
        *,
        create_group: bool = False,
        cancel: CancelToken | None = None,
    ) -> EnsureLogStreamResult:
        """Create the log stream when missing; a missing group raises ``NotFoundError`` unless
        ``create_group``."""
        return self._execute(self._ops.ensure_log_stream(group, stream, create_group), cancel)

    def put_retention_policy(
        self, group: str, days: int | None, *, cancel: CancelToken | None = None
    ) -> None:
        """Set the retention (``PutRetentionPolicy``); ``None`` or ``0`` removes it."""
        self._execute(self._ops.put_retention_policy(group, days), cancel)

    def delete_log_group(
        self, name: str, *, ignore_missing: bool = False, cancel: CancelToken | None = None
    ) -> bool:
        """Delete a group with its streams and events; ``False`` when missing and
        ``ignore_missing`` (else ``NotFoundError``)."""
        return self._execute(self._ops.delete_log_group(name, ignore_missing), cancel)

    def delete_log_stream(
        self,
        group: str,
        stream: str,
        *,
        ignore_missing: bool = False,
        cancel: CancelToken | None = None,
    ) -> bool:
        """Delete a stream; ``False`` when missing and ``ignore_missing``."""
        return self._execute(self._ops.delete_log_stream(group, stream, ignore_missing), cancel)

    def describe_log_groups(
        self,
        *,
        prefix: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Iterator[LogGroupInfo]:
        """Iterate log groups (sorted by name) across pages; ``page_size`` 1-50 (default 50),
        ``limit`` stops after that many groups."""
        return self._iterate(self._ops.describe_log_groups(prefix, page_size, limit), cancel)

    def describe_log_streams(
        self,
        group: str,
        *,
        prefix: str | None = None,
        order_by: Literal["LogStreamName", "LastEventTime"] | None = None,
        descending: bool | None = None,
        page_size: int | None = None,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Iterator[LogStreamInfo]:
        """Iterate the streams of a group across pages (``prefix`` is not allowed with
        ``order_by="LastEventTime"``)."""
        return self._iterate(
            self._ops.describe_log_streams(group, prefix, order_by, descending, page_size, limit),
            cancel,
        )

    def tag_log_group(
        self, name: str, tags: Mapping[str, str], *, cancel: CancelToken | None = None
    ) -> None:
        """Add or overwrite tags of a log group (name or ARN; ``TagResource``)."""
        self._execute(self._ops.tag_log_group(name, tags), cancel)

    def untag_log_group(
        self, name: str, keys: Iterable[str], *, cancel: CancelToken | None = None
    ) -> None:
        """Remove tags from a log group (name or ARN; ``UntagResource``)."""
        self._execute(self._ops.untag_log_group(name, keys), cancel)

    def list_log_group_tags(
        self, name: str, *, cancel: CancelToken | None = None
    ) -> dict[str, str]:
        """Tags of a log group (name or ARN; ``ListTagsForResource``)."""
        return self._execute(self._ops.list_log_group_tags(name), cancel)

    def log_group_arn(self, name: str, *, cancel: CancelToken | None = None) -> str:
        """ARN of a log group (without ``:*``): built from ``config.account_id`` and the region
        when the account is configured, otherwise looked up with ``DescribeLogGroups``."""
        if self.config.account_id is not None:
            self._client(False, "log_group_arn")
        return self._execute(self._ops.log_group_arn(name), cancel)

    # ------------------------------------------------------------------ ingest

    def put_log_events(
        self,
        group: str,
        stream: str,
        events: Iterable[InputLogEvent | Mapping[str, Any]],
        *,
        auto_create: bool | None = None,
        create_group: bool = False,
        max_event_bytes: int | None = None,
        oversize_policy: str | None = None,
        cancel: CancelToken | None = None,
    ) -> PutLogEventsResult:
        """Send events with ``PutLogEvents``.

        Events are sorted, oversized events are truncated / dropped / rejected per
        ``oversize_policy`` (default ``config.oversize_policy``, limit
        ``config.max_event_bytes``) and split into batches that respect every service limit
        (10,000 events, 1 MiB incl. 26 bytes per event, 24 h span). A missing stream is created
        when ``auto_create`` (default ``config.auto_create``); ``create_group`` also creates a
        missing group. Events the service rejects (too old / new, expired) are reported, never
        retried. Throttling and 5xx are retried by the AWS SDK (a lost response may duplicate
        events).
        """
        return self._execute(
            self._ops.put_log_events(
                group, stream, events, auto_create, create_group, max_event_bytes, oversize_policy
            ),
            cancel,
        )

    def emf(
        self,
        metric: str,
        value: float | Sequence[float],
        dimensions: Mapping[str, str] | None = None,
        *,
        namespace: str = DEFAULT_EMF_NAMESPACE,
        unit: str = "None",
        timestamp: datetime | int | None = None,
        properties: Mapping[str, str | int | float | bool] | None = None,
        group: str | None = None,
        stream: str | None = None,
        cancel: CancelToken | None = None,
    ) -> dict[str, Any]:
        """Build an Embedded Metric Format document (:func:`build_emf`) and send it as one log
        event to ``group`` / ``stream`` (defaults ``config.log_group`` / ``config.log_stream``).
        Returns the document sent."""
        return self._execute(
            self._ops.emf(
                metric, value, dimensions, namespace, unit, timestamp, properties, group, stream
            ),
            cancel,
        )

    # ------------------------------------------------------------------ query

    def filter_log_events_page(
        self,
        *,
        group: str | None = None,
        streams: Sequence[str] | None = None,
        stream_prefix: str | None = None,
        pattern: str | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        next_token: str | None = None,
        cancel: CancelToken | None = None,
    ) -> FilterLogEventsPage:
        """One ``FilterLogEvents`` request (``next_token`` is ``None`` at the end; AWS may return
        empty pages that still carry a token). Events are ordered by timestamp, then id."""
        return self._execute(
            self._ops.filter_log_events_page(
                group, streams, stream_prefix, pattern, start, end, page_size, next_token
            ),
            cancel,
        )

    def filter_log_events(
        self,
        *,
        group: str | None = None,
        streams: Sequence[str] | None = None,
        stream_prefix: str | None = None,
        pattern: str | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Iterator[FilteredLogEvent]:
        """Iterate events matching a filter pattern (``FilterLogEvents``) across pages until no
        token is left (or ``limit``). ``group`` is a name or ARN (default ``config.log_group``);
        ``streams`` (max 100) and ``stream_prefix`` are mutually exclusive; ``start`` / ``end``
        are inclusive (``datetime`` or epoch ms)."""
        return self._iterate(
            self._ops.filter_log_events(
                group, streams, stream_prefix, pattern, start, end, page_size, limit
            ),
            cancel,
        )

    def get_log_events(
        self,
        group: str,
        stream: str,
        *,
        start_from_head: bool = False,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Iterator[StreamLogEvent]:
        """Iterate the events of one stream (``GetLogEvents``).

        ``start_from_head=True`` reads oldest → newest; the default reads the newest page first,
        then older pages (events inside a page stay in ascending order). Iteration stops when
        the service returns the token that was sent.
        """
        return self._iterate(
            self._ops.get_log_events(group, stream, start_from_head, start, end, page_size, limit),
            cancel,
        )

    def start_query(
        self,
        query_string: str,
        *,
        groups: Sequence[str] | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> str:
        """Start a Logs Insights query (``StartQuery``); returns its id. ``groups`` are names or
        ARNs (max 50, default ``[config.log_group]``); ``end`` defaults to now, ``start`` to one
        hour before ``end``."""
        return self._execute(self._ops.start_query(query_string, groups, start, end, limit), cancel)

    def get_query_results(
        self, query_id: str, *, cancel: CancelToken | None = None
    ) -> QueryResults:
        """Status and rows of a Logs Insights query (``GetQueryResults``)."""
        return self._execute(self._ops.get_query_results(query_id), cancel)

    def stop_query(self, query_id: str, *, cancel: CancelToken | None = None) -> bool:
        """Stop a running query (``StopQuery``); ``False`` when it had already finished."""
        return self._execute(self._ops.stop_query(query_id), cancel)

    def query(
        self,
        query_string: str,
        *,
        groups: Sequence[str] | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        limit: int | None = None,
        timeout_s: float | timedelta = 60.0,
        poll_interval_s: float | timedelta = 1.0,
        retry_on_query_limit: int = 3,
        cancel: CancelToken | None = None,
    ) -> QueryResults:
        """Run a Logs Insights query to completion: ``StartQuery``, then poll
        ``GetQueryResults`` (interval doubling up to 5 s) until a terminal status.

        ``Failed`` / ``Unknown`` → ``UnavailableError`` (not retryable), ``Timeout`` →
        ``TimeoutError``, ``Cancelled`` → ``CancelledError`` (messages include the query id).
        After ``timeout_s`` or on cancellation the query is stopped best-effort.
        ``LimitExceededException`` on start is retried ``retry_on_query_limit`` times.
        """
        return self._execute(
            self._ops.query(
                query_string,
                groups,
                start,
                end,
                limit,
                timeout_s,
                poll_interval_s,
                retry_on_query_limit,
            ),
            cancel,
        )

    def describe_queries(
        self,
        *,
        group: str | None = None,
        status: str | None = None,
        max_results: int | None = None,
        cancel: CancelToken | None = None,
    ) -> list[QueryInfo]:
        """Recent Logs Insights queries (``DescribeQueries``)."""
        return self._execute(self._ops.describe_queries(group, status, max_results), cancel)

    # ------------------------------------------------------------------ live tail

    def start_live_tail(
        self,
        groups: Sequence[str],
        on_event: Callable[[LiveTailEvent], None],
        *,
        streams: Sequence[str] | None = None,
        stream_prefixes: Sequence[str] | None = None,
        pattern: str | None = None,
        on_update: Callable[[LiveTailUpdate], None] | None = None,
        on_error: Callable[[CloudWatchLogsError], None] | None = None,
        cancel: CancelToken | None = None,
    ) -> LiveTailSession:
        """Start a Live Tail session (``StartLiveTail``) and call ``on_event`` for every event.

        Returns once the session has started (``sessionStart``). ``groups`` are 1-10 names or
        ARNs (names become ARNs); ``streams`` / ``stream_prefixes`` need exactly one group. On
        AWS the session streams new events for up to 3 hours; the Moto emulator replays the
        matching existing events once and ends.
        """
        if self.config.account_id is not None:
            self._client(False, "start_live_tail")
        params = self._execute(
            self._ops.live_tail_params(groups, streams, stream_prefixes, pattern), cancel
        )
        out = self._invoke(Call("start_live_tail", "start_live_tail", params), cancel)
        stream = out.get("responseStream")
        if stream is None:
            raise UnavailableError("start_live_tail: the service returned no event stream")
        dispatcher = _Dispatcher(on_event, on_update, on_error, list(self._secrets()))
        session = LiveTailSession(stream, dispatcher, on_done=self._tails.discard)
        self._tails.add(session)
        try:
            session._start()
        except BaseException:
            self._tails.discard(session)
            raise
        return session

    # ------------------------------------------------------------------ logging category

    def exporter(
        self, *, group: str | None = None, stream: str | None = None, create_group: bool = False
    ) -> CloudWatchLogsExporter:
        """A ``LogExporter`` writing to ``group`` / ``stream`` (defaults ``config.log_group`` /
        ``config.log_stream``)."""
        target = self._ops.group_or_default(group, "exporter")
        return CloudWatchLogsExporter(
            self, target, stream or self.config.log_stream, create_group=create_group
        )

    def log_client(
        self,
        *,
        group: str | None = None,
        stream: str | None = None,
        create_group: bool = False,
        options: BatchOptions | None = None,
        resource: Mapping[str, AttrValue] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
    ) -> BatchingLogClient:
        """A ``BatchingLogClient`` exporting to ``group`` / ``stream`` (records are sent as
        normalized LogRecord JSON; the stream is created on first use). :meth:`close` of this
        client flushes and closes it too."""
        exporter = self.exporter(group=group, stream=stream, create_group=create_group)
        lc = BatchingLogClient(
            exporter, options=options, resource=resource, redaction=redaction, correlate=correlate
        )
        self._log_clients.append(lc)
        return lc

    def log_query_client(self, *, group: str | None = None) -> CloudWatchLogsQueryClient:
        """A ``LogQueryClient`` over ``group`` (default ``config.log_group``)."""
        return CloudWatchLogsQueryClient(
            self, self._ops.group_or_default(group, "log_query_client")
        )


# =============================================================================== async


class AsyncCloudWatchLogsClient(_Base):
    """Asynchronous Amazon CloudWatch Logs client (aioboto3 / aiobotocore); use ``async with``
    or call :meth:`aclose`. Same methods as :class:`CloudWatchLogsClient`, awaited; iterator
    methods return async iterators.

    Cancel a call by cancelling its task: the SDK raises :class:`CancelledError`, which is also
    an :class:`asyncio.CancelledError`.
    """

    def __init__(
        self,
        config: CloudWatchLogsConfig | None = None,
        *,
        internals: ClientInternals | None = None,
    ) -> None:
        super().__init__(config, internals)
        self._lock: asyncio.Lock | None = None
        self._sdk: Any = self.config.sdk_client
        self._probe: Any = self.config.sdk_client
        self._contexts: list[Any] = []
        self._log_clients: list[AsyncBatchingLogClient] = []
        self._tails: set[AsyncLiveTailSession] = set()
        if self._sdk is not None:
            self._ops.region = getattr(getattr(self._sdk, "meta", None), "region_name", None)

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None, **overrides: Any
    ) -> AsyncCloudWatchLogsClient:
        """Build a client from ``GROW_AWS_CLOUDWATCH_LOGS_*`` and the standard AWS variables."""
        return cls(CloudWatchLogsConfig.from_env(env, **overrides))

    # ------------------------------------------------------------------ SDK plumbing

    async def _build(self, probe: bool) -> Any:
        try:
            import aioboto3  # noqa: PLC0415 - optional dependency (extra "async")
            from aiobotocore.credentials import (  # noqa: PLC0415
                AioAssumeRoleCredentialFetcher,
                AioDeferredRefreshableCredentials,
            )
            from aiobotocore.session import AioSession  # noqa: PLC0415
        except ImportError:  # pragma: no cover - the test environment installs the extra
            raise ConfigError(
                "config: the async client needs aioboto3; install grow-aws-cloudwatch-logs[async]"
            ) from None
        bs = AioSession(profile=self.config.profile)
        session = aioboto3.Session(botocore_session=bs, **self._session_kwargs())
        if self.config.role_arn is not None:
            fetcher = AioAssumeRoleCredentialFetcher(
                client_creator=bs.create_client,
                source_credentials=await bs.get_credentials(),
                role_arn=self.config.role_arn,
                extra_args=self._role_args(),
            )
            bs._credentials = AioDeferredRefreshableCredentials(
                method="assume-role", refresh_using=fetcher.fetch_credentials
            )
        context = session.client("logs", **self._client_kwargs(probe))
        client = await context.__aenter__()
        self._contexts.append(context)
        self._bridge.register(client, probe=probe)
        return client

    async def _client(self, probe: bool, op: str) -> Any:
        current = self._probe if probe else self._sdk
        if current is not None:
            return current
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            current = self._probe if probe else self._sdk
            if current is None:
                try:
                    current = await self._build(probe)
                except CloudWatchLogsError:
                    raise
                except Exception as exc:  # noqa: BLE001 - every SDK failure is mapped
                    mapped = map_aws_error(op, exc, self._secrets())
                    raise mapped from mapped.cause
                if probe:
                    self._probe = current
                else:
                    self._sdk = current
                self._ops.region = self.config.region or current.meta.region_name
            return current

    async def get_sdk(self) -> Any:
        """The underlying aiobotocore ``logs`` client (created on first use)."""
        return await self._client(False, "sdk")

    async def get_region(self) -> str | None:
        """The effective AWS region."""
        return self.config.region or (await self._client(False, "region")).meta.region_name

    async def _invoke(self, call: Call) -> Any:
        if self._closed and not call.best_effort:
            raise self._closed_error(call.op)
        sdk = await self._client(call.probe, call.op)
        fn = getattr(sdk, call.method)
        state = self._state(call, None)
        state.deadline = None
        restore = bind_state(state)
        try:
            return await asyncio.wait_for(fn(**call.params), self.config.total_timeout)
        except CloudWatchLogsError:
            raise
        except asyncio.CancelledError as exc:
            raise CancelledError(f"{call.op}: cancelled", cause=exc) from exc
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"{call.op}: timed out after {self.config.total_timeout:g}s"
            ) from None
        except Exception as exc:  # noqa: BLE001 - every SDK failure is mapped
            mapped = map_aws_error(call.op, exc, self._secrets())
            raise mapped from mapped.cause
        finally:
            restore()

    async def _sleep(self, seconds: float) -> None:
        if self._internals.sleep is not None:
            self._internals.sleep(seconds)
            await asyncio.sleep(0)
            return
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError as exc:
            if isinstance(exc, CloudWatchLogsError):
                raise
            raise CancelledError("sleep: cancelled", cause=exc) from exc

    async def _step(self, instr: Any) -> Any:
        if isinstance(instr, Call):
            return await self._invoke(instr)
        if isinstance(instr, Sleep):
            await self._sleep(instr.seconds)
            return None
        raise TypeError(f"unexpected instruction {instr!r}")  # pragma: no cover

    async def _execute(self, gen: Generator[Any, Any, T]) -> T:
        value: Any = None
        error: BaseException | None = None
        while True:
            try:
                instr = gen.throw(error) if error is not None else gen.send(value)
            except StopIteration as stop:
                return stop.value  # type: ignore[no-any-return]
            value = error = None
            try:
                value = await self._step(instr)
            except CloudWatchLogsError as exc:
                error = exc

    async def _iterate(self, gen: Generator[Any, Any, None]) -> AsyncIterator[Any]:
        value: Any = None
        error: BaseException | None = None
        try:
            while True:
                try:
                    instr = gen.throw(error) if error is not None else gen.send(value)
                except StopIteration:
                    return
                value = error = None
                if isinstance(instr, Emit):
                    yield instr.item
                    continue
                try:
                    value = await self._step(instr)
                except CloudWatchLogsError as exc:
                    error = exc
        finally:
            gen.close()

    # ------------------------------------------------------------------ core

    async def health(self) -> HealthStatus:
        """See :meth:`CloudWatchLogsClient.health`."""
        return await self._execute(self._ops.health())

    async def ping(self) -> float:
        """See :meth:`CloudWatchLogsClient.ping`."""
        return await self._execute(self._ops.ping())

    async def server_version(self) -> str:
        """Always :class:`UnsupportedError` (CloudWatch Logs exposes no server version)."""
        raise self._unsupported_version()

    async def aclose(self) -> None:
        """Flush and close log clients, stop live-tail sessions and release connections.
        Idempotent; never closes a caller-supplied SDK client."""
        if self._closed:
            return
        for lc in list(self._log_clients):
            with contextlib.suppress(Exception):
                await lc.close()
        for tail in list(self._tails):
            with contextlib.suppress(Exception):
                await tail.close()
        self._closed = True
        contexts, self._contexts = self._contexts, []
        for context in contexts:
            with contextlib.suppress(Exception):
                await context.__aexit__(None, None, None)
        if self._tls is not None:
            self._tls.close()

    close = aclose

    async def __aenter__(self) -> AsyncCloudWatchLogsClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"AsyncCloudWatchLogsClient(config={self.config!r})"

    # ------------------------------------------------------------------ bootstrap

    async def get_log_group(self, name: str) -> LogGroupInfo | None:
        """See :meth:`CloudWatchLogsClient.get_log_group`."""
        return await self._execute(self._ops.get_log_group(name))

    async def ensure_log_group(
        self,
        name: str,
        *,
        retention_days: int | None = None,
        kms_key_id: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> EnsureLogGroupResult:
        """See :meth:`CloudWatchLogsClient.ensure_log_group`."""
        return await self._execute(
            self._ops.ensure_log_group(name, retention_days, kms_key_id, tags)
        )

    async def ensure_log_stream(
        self, group: str, stream: str, *, create_group: bool = False
    ) -> EnsureLogStreamResult:
        """See :meth:`CloudWatchLogsClient.ensure_log_stream`."""
        return await self._execute(self._ops.ensure_log_stream(group, stream, create_group))

    async def put_retention_policy(self, group: str, days: int | None) -> None:
        """See :meth:`CloudWatchLogsClient.put_retention_policy`."""
        await self._execute(self._ops.put_retention_policy(group, days))

    async def delete_log_group(self, name: str, *, ignore_missing: bool = False) -> bool:
        """See :meth:`CloudWatchLogsClient.delete_log_group`."""
        return await self._execute(self._ops.delete_log_group(name, ignore_missing))

    async def delete_log_stream(
        self, group: str, stream: str, *, ignore_missing: bool = False
    ) -> bool:
        """See :meth:`CloudWatchLogsClient.delete_log_stream`."""
        return await self._execute(self._ops.delete_log_stream(group, stream, ignore_missing))

    def describe_log_groups(
        self,
        *,
        prefix: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[LogGroupInfo]:
        """See :meth:`CloudWatchLogsClient.describe_log_groups` (async iterator)."""
        return self._iterate(self._ops.describe_log_groups(prefix, page_size, limit))

    def describe_log_streams(
        self,
        group: str,
        *,
        prefix: str | None = None,
        order_by: Literal["LogStreamName", "LastEventTime"] | None = None,
        descending: bool | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[LogStreamInfo]:
        """See :meth:`CloudWatchLogsClient.describe_log_streams` (async iterator)."""
        return self._iterate(
            self._ops.describe_log_streams(group, prefix, order_by, descending, page_size, limit)
        )

    async def tag_log_group(self, name: str, tags: Mapping[str, str]) -> None:
        """See :meth:`CloudWatchLogsClient.tag_log_group`."""
        await self._execute(self._ops.tag_log_group(name, tags))

    async def untag_log_group(self, name: str, keys: Iterable[str]) -> None:
        """See :meth:`CloudWatchLogsClient.untag_log_group`."""
        await self._execute(self._ops.untag_log_group(name, keys))

    async def list_log_group_tags(self, name: str) -> dict[str, str]:
        """See :meth:`CloudWatchLogsClient.list_log_group_tags`."""
        return await self._execute(self._ops.list_log_group_tags(name))

    async def log_group_arn(self, name: str) -> str:
        """See :meth:`CloudWatchLogsClient.log_group_arn`."""
        if self.config.account_id is not None:
            await self._client(False, "log_group_arn")
        return await self._execute(self._ops.log_group_arn(name))

    # ------------------------------------------------------------------ ingest

    async def put_log_events(
        self,
        group: str,
        stream: str,
        events: Iterable[InputLogEvent | Mapping[str, Any]],
        *,
        auto_create: bool | None = None,
        create_group: bool = False,
        max_event_bytes: int | None = None,
        oversize_policy: str | None = None,
    ) -> PutLogEventsResult:
        """See :meth:`CloudWatchLogsClient.put_log_events`."""
        return await self._execute(
            self._ops.put_log_events(
                group, stream, events, auto_create, create_group, max_event_bytes, oversize_policy
            )
        )

    async def emf(
        self,
        metric: str,
        value: float | Sequence[float],
        dimensions: Mapping[str, str] | None = None,
        *,
        namespace: str = DEFAULT_EMF_NAMESPACE,
        unit: str = "None",
        timestamp: datetime | int | None = None,
        properties: Mapping[str, str | int | float | bool] | None = None,
        group: str | None = None,
        stream: str | None = None,
    ) -> dict[str, Any]:
        """See :meth:`CloudWatchLogsClient.emf`."""
        return await self._execute(
            self._ops.emf(
                metric, value, dimensions, namespace, unit, timestamp, properties, group, stream
            )
        )

    # ------------------------------------------------------------------ query

    async def filter_log_events_page(
        self,
        *,
        group: str | None = None,
        streams: Sequence[str] | None = None,
        stream_prefix: str | None = None,
        pattern: str | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        next_token: str | None = None,
    ) -> FilterLogEventsPage:
        """See :meth:`CloudWatchLogsClient.filter_log_events_page`."""
        return await self._execute(
            self._ops.filter_log_events_page(
                group, streams, stream_prefix, pattern, start, end, page_size, next_token
            )
        )

    def filter_log_events(
        self,
        *,
        group: str | None = None,
        streams: Sequence[str] | None = None,
        stream_prefix: str | None = None,
        pattern: str | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[FilteredLogEvent]:
        """See :meth:`CloudWatchLogsClient.filter_log_events` (async iterator)."""
        return self._iterate(
            self._ops.filter_log_events(
                group, streams, stream_prefix, pattern, start, end, page_size, limit
            )
        )

    def get_log_events(
        self,
        group: str,
        stream: str,
        *,
        start_from_head: bool = False,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[StreamLogEvent]:
        """See :meth:`CloudWatchLogsClient.get_log_events` (async iterator)."""
        return self._iterate(
            self._ops.get_log_events(group, stream, start_from_head, start, end, page_size, limit)
        )

    async def start_query(
        self,
        query_string: str,
        *,
        groups: Sequence[str] | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        limit: int | None = None,
    ) -> str:
        """See :meth:`CloudWatchLogsClient.start_query`."""
        return await self._execute(self._ops.start_query(query_string, groups, start, end, limit))

    async def get_query_results(self, query_id: str) -> QueryResults:
        """See :meth:`CloudWatchLogsClient.get_query_results`."""
        return await self._execute(self._ops.get_query_results(query_id))

    async def stop_query(self, query_id: str) -> bool:
        """See :meth:`CloudWatchLogsClient.stop_query`."""
        return await self._execute(self._ops.stop_query(query_id))

    async def query(
        self,
        query_string: str,
        *,
        groups: Sequence[str] | None = None,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        limit: int | None = None,
        timeout_s: float | timedelta = 60.0,
        poll_interval_s: float | timedelta = 1.0,
        retry_on_query_limit: int = 3,
    ) -> QueryResults:
        """See :meth:`CloudWatchLogsClient.query`."""
        return await self._execute(
            self._ops.query(
                query_string,
                groups,
                start,
                end,
                limit,
                timeout_s,
                poll_interval_s,
                retry_on_query_limit,
            )
        )

    async def describe_queries(
        self,
        *,
        group: str | None = None,
        status: str | None = None,
        max_results: int | None = None,
    ) -> list[QueryInfo]:
        """See :meth:`CloudWatchLogsClient.describe_queries`."""
        return await self._execute(self._ops.describe_queries(group, status, max_results))

    # ------------------------------------------------------------------ live tail

    async def start_live_tail(
        self,
        groups: Sequence[str],
        on_event: Callable[[LiveTailEvent], None],
        *,
        streams: Sequence[str] | None = None,
        stream_prefixes: Sequence[str] | None = None,
        pattern: str | None = None,
        on_update: Callable[[LiveTailUpdate], None] | None = None,
        on_error: Callable[[CloudWatchLogsError], None] | None = None,
    ) -> AsyncLiveTailSession:
        """See :meth:`CloudWatchLogsClient.start_live_tail`; the session is read by a task on
        the running event loop."""
        if self.config.account_id is not None:
            await self._client(False, "start_live_tail")
        params = await self._execute(
            self._ops.live_tail_params(groups, streams, stream_prefixes, pattern)
        )
        out = await self._invoke(Call("start_live_tail", "start_live_tail", params))
        stream = out.get("responseStream")
        if stream is None:
            raise UnavailableError("start_live_tail: the service returned no event stream")
        dispatcher = _Dispatcher(on_event, on_update, on_error, list(self._secrets()))
        session = AsyncLiveTailSession(stream, dispatcher, on_done=self._tails.discard)
        self._tails.add(session)
        try:
            await session._start()
        except BaseException:
            self._tails.discard(session)
            raise
        return session

    # ------------------------------------------------------------------ logging category

    def exporter(
        self, *, group: str | None = None, stream: str | None = None, create_group: bool = False
    ) -> AsyncCloudWatchLogsExporter:
        """See :meth:`CloudWatchLogsClient.exporter`."""
        target = self._ops.group_or_default(group, "exporter")
        return AsyncCloudWatchLogsExporter(
            self, target, stream or self.config.log_stream, create_group=create_group
        )

    def log_client(
        self,
        *,
        group: str | None = None,
        stream: str | None = None,
        create_group: bool = False,
        options: BatchOptions | None = None,
        resource: Mapping[str, AttrValue] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
    ) -> AsyncBatchingLogClient:
        """See :meth:`CloudWatchLogsClient.log_client` (an ``AsyncBatchingLogClient``)."""
        exporter = self.exporter(group=group, stream=stream, create_group=create_group)
        lc = AsyncBatchingLogClient(
            exporter, options=options, resource=resource, redaction=redaction, correlate=correlate
        )
        self._log_clients.append(lc)
        return lc

    def log_query_client(self, *, group: str | None = None) -> AsyncCloudWatchLogsQueryClient:
        """See :meth:`CloudWatchLogsClient.log_query_client`."""
        return AsyncCloudWatchLogsQueryClient(
            self, self._ops.group_or_default(group, "log_query_client")
        )
