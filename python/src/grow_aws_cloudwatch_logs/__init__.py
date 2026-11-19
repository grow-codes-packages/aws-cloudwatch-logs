"""Grow Codes SDK for Amazon CloudWatch Logs, built on the official AWS SDK for Python.

>>> from grow_aws_cloudwatch_logs import CloudWatchLogsClient
>>> client = CloudWatchLogsClient.from_env()  # doctest: +SKIP
>>> client.ensure_log_group("my-app/prod", retention_days=30)  # doctest: +SKIP
"""

from . import _core
from ._core import *  # noqa: F403
from ._version import __version__

__all__ = ["__version__"]
__all__ += _core.__all__

from . import _logging  # noqa: E402
from ._logging import *  # noqa: E402, F403

__all__ += _logging.__all__

from ._aws_errors import AWS_ERROR_CODES, AwsSdkError, aws_error_code, map_aws_error  # noqa: E402
from ._batching import (  # noqa: E402
    EVENT_OVERHEAD_BYTES,
    MAX_BATCH_BYTES,
    MAX_BATCH_SPAN_MS,
    MAX_EVENTS_PER_BATCH,
    BatchPlan,
    plan_log_event_batches,
    truncate_message,
    utf8_bytes,
)
from ._cwl_logging import (  # noqa: E402
    AsyncCloudWatchLogsExporter,
    AsyncCloudWatchLogsQueryClient,
    CloudWatchLogsExporter,
    CloudWatchLogsQueryClient,
    record_to_message,
)
from ._emf import DEFAULT_EMF_NAMESPACE, EMF_UNITS, build_emf  # noqa: E402
from ._livetail import AsyncLiveTailSession, LiveTailSession  # noqa: E402
from ._ops import (  # noqa: E402
    DEFAULT_SEARCH_LOOKBACK_MS,
    MAX_SEARCH_LIMIT,
    RETENTION_DAYS,
    SEARCH_FIRST_SLICE_MS,
    SEARCH_MAX_SLICE_MS,
    SEARCH_SCAN_CAP,
    event_to_log_record,
    parse_insights_timestamp,
    partition_for_region,
    to_filter_pattern,
)
from ._types import (  # noqa: E402
    QUERY_STATUSES,
    QUERY_TERMINAL_STATUSES,
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
    QueryStatistics,
    QueryStatus,
    RejectedLogEventsInfo,
    StreamLogEvent,
    TimeInput,
)
from .client import AsyncCloudWatchLogsClient, ClientInternals, CloudWatchLogsClient  # noqa: E402
from .config import (  # noqa: E402
    API_VERSION,
    DEFAULT_MAX_EVENT_BYTES,
    DEFAULT_ROLE_SESSION_NAME,
    DEFAULT_URL,
    MAX_EVENT_BYTES_HARD_CAP,
    OVERSIZE_POLICIES,
    CloudWatchLogsConfig,
    OversizePolicy,
    default_log_stream_name,
    is_valid_log_group_name,
    is_valid_log_stream_name,
)

__all__ += [
    # clients + config
    "CloudWatchLogsClient",
    "AsyncCloudWatchLogsClient",
    "ClientInternals",
    "CloudWatchLogsConfig",
    "DEFAULT_URL",
    "API_VERSION",
    "DEFAULT_MAX_EVENT_BYTES",
    "DEFAULT_ROLE_SESSION_NAME",
    "MAX_EVENT_BYTES_HARD_CAP",
    "OVERSIZE_POLICIES",
    "OversizePolicy",
    "default_log_stream_name",
    "is_valid_log_group_name",
    "is_valid_log_stream_name",
    # errors
    "AWS_ERROR_CODES",
    "AwsSdkError",
    "aws_error_code",
    "map_aws_error",
    # types
    "EnsureLogGroupResult",
    "EnsureLogStreamResult",
    "FilteredLogEvent",
    "FilterLogEventsPage",
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
    # ingest
    "EVENT_OVERHEAD_BYTES",
    "MAX_BATCH_BYTES",
    "MAX_BATCH_SPAN_MS",
    "MAX_EVENTS_PER_BATCH",
    "BatchPlan",
    "plan_log_event_batches",
    "truncate_message",
    "utf8_bytes",
    "DEFAULT_EMF_NAMESPACE",
    "EMF_UNITS",
    "build_emf",
    # query + live tail
    "RETENTION_DAYS",
    "DEFAULT_SEARCH_LOOKBACK_MS",
    "MAX_SEARCH_LIMIT",
    "SEARCH_FIRST_SLICE_MS",
    "SEARCH_MAX_SLICE_MS",
    "SEARCH_SCAN_CAP",
    "parse_insights_timestamp",
    "partition_for_region",
    "to_filter_pattern",
    "event_to_log_record",
    "LiveTailSession",
    "AsyncLiveTailSession",
    # logging category
    "CloudWatchLogsExporter",
    "AsyncCloudWatchLogsExporter",
    "CloudWatchLogsQueryClient",
    "AsyncCloudWatchLogsQueryClient",
    "record_to_message",
]
