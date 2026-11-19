//! Data types, options and limits of the service functions.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::OversizePolicy;
use crate::core::{Error, Result};

/// Retention values (days) CloudWatch Logs accepts.
pub const RETENTION_DAYS: &[u32] = &[
    1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922,
    3288, 3653,
];

/// Largest `DescribeLogGroups` / `DescribeLogStreams` page.
pub const MAX_DESCRIBE_PAGE_SIZE: usize = 50;
/// Largest `FilterLogEvents` / `GetLogEvents` page (and Logs Insights row limit).
pub const MAX_EVENTS_PAGE_SIZE: usize = 10_000;
/// Maximum number of events in one `PutLogEvents` call.
pub const MAX_BATCH_EVENTS: usize = 10_000;
/// Maximum size of one `PutLogEvents` call: Σ(UTF-8 message bytes + 26).
pub const MAX_BATCH_BYTES: usize = 1_048_576;
/// Per-event overhead counted against [`MAX_BATCH_BYTES`].
pub const EVENT_OVERHEAD_BYTES: usize = 26;
/// Largest timestamp span inside one `PutLogEvents` call.
pub const MAX_BATCH_SPAN: Duration = Duration::from_secs(24 * 3600);
/// Most log groups one live-tail session can follow.
pub const MAX_LIVE_TAIL_GROUPS: usize = 10;
/// Most log groups one Logs Insights query can read.
pub const MAX_QUERY_GROUPS: usize = 50;
/// Default timeout of [`query`](crate::CloudWatchLogsClient::query).
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
/// First poll interval of [`query`](crate::CloudWatchLogsClient::query).
pub const DEFAULT_QUERY_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Largest poll interval of [`query`](crate::CloudWatchLogsClient::query).
pub const MAX_QUERY_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Default number of `StartQuery` retries after `LimitExceededException`.
pub const DEFAULT_QUERY_LIMIT_RETRIES: u32 = 3;

/// Attribute key with the stream name on records read back from CloudWatch Logs.
pub const ATTR_LOG_STREAM: &str = "aws.log.stream";
/// Attribute key with the event id on records read back by `search`.
pub const ATTR_EVENT_ID: &str = "aws.event.id";
/// Attribute key with the log group on plain live-tail records.
pub const ATTR_LOG_GROUP: &str = "aws.log.group";

/// `VALIDATION` unless `name` is a valid log group name (1–512 of `[.-_/#A-Za-z0-9]`).
pub fn validate_group_name(op: &str, name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 512
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'/' | b'#'));
    if ok {
        Ok(())
    } else {
        Err(Error::validation(format!(
            "{op}: log group name must be 1-512 characters of [.-_/#A-Za-z0-9]"
        )))
    }
}

/// `VALIDATION` unless `name` is a valid log stream name (1–512 characters, no `:` or `*`).
pub fn validate_stream_name(op: &str, name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 512 || name.contains([':', '*']) {
        return Err(Error::validation(format!(
            "{op}: log stream name must be 1-512 characters without ':' or '*'"
        )));
    }
    Ok(())
}

pub(crate) fn validate_retention(op: &str, days: u32) -> Result<()> {
    if days != 0 && !RETENTION_DAYS.contains(&days) {
        return Err(Error::validation(format!(
            "{op}: retention {days} is not one of the allowed values"
        )));
    }
    Ok(())
}

pub(crate) fn validate_page_size(op: &str, size: Option<usize>, max: usize) -> Result<()> {
    match size {
        Some(n) if n == 0 || n > max => Err(Error::validation(format!(
            "{op}: pageSize must be between 1 and {max}"
        ))),
        _ => Ok(()),
    }
}

/// Removes the `:*` suffix `DescribeLogGroups` puts on group ARNs.
pub(crate) fn trim_star_arn(arn: &str) -> &str {
    arn.strip_suffix(":*").unwrap_or(arn)
}

/// Epoch milliseconds → time (`None` or `0` → `None`).
pub(crate) fn ms_time(ms: Option<i64>) -> Option<SystemTime> {
    match ms {
        Some(ms) if ms > 0 => Some(UNIX_EPOCH + Duration::from_millis(ms as u64)),
        _ => None,
    }
}

/// Time → epoch milliseconds (clamped at the epoch).
pub(crate) fn to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub(crate) fn clamp_i32(n: usize) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// A log group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct LogGroup {
    /// Log group name.
    pub name: String,
    /// Group ARN without the trailing `:*`.
    pub arn: String,
    /// When the group was created.
    pub creation_time: Option<SystemTime>,
    /// Retention in days; `None` = never expire.
    pub retention_days: Option<u32>,
    /// ARN of the KMS key that encrypts the group.
    pub kms_key_id: Option<String>,
    /// Stored size (approximate and delayed on AWS).
    pub stored_bytes: i64,
    /// Number of metric filters.
    pub metric_filter_count: u32,
    /// Log group class (`STANDARD`, `INFREQUENT_ACCESS`, …) when reported.
    pub class: Option<String>,
}

impl LogGroup {
    pub(crate) fn from_sdk(g: &aws_sdk_cloudwatchlogs::types::LogGroup) -> Self {
        let arn = g
            .log_group_arn()
            .map(str::to_owned)
            .unwrap_or_else(|| trim_star_arn(g.arn().unwrap_or_default()).to_owned());
        LogGroup {
            name: g.log_group_name().unwrap_or_default().to_owned(),
            arn,
            creation_time: ms_time(g.creation_time()),
            retention_days: g
                .retention_in_days()
                .and_then(|d| u32::try_from(d).ok())
                .filter(|d| *d > 0),
            kms_key_id: g.kms_key_id().map(str::to_owned),
            stored_bytes: g.stored_bytes().unwrap_or(0),
            metric_filter_count: g
                .metric_filter_count()
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(0),
            class: g.log_group_class().map(|c| c.as_str().to_owned()),
        }
    }
}

/// A log stream.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct LogStream {
    /// Stream name.
    pub name: String,
    /// Stream ARN.
    pub arn: String,
    /// When the stream was created.
    pub creation_time: Option<SystemTime>,
    /// First event timestamp.
    pub first_event_time: Option<SystemTime>,
    /// Last event timestamp (eventually consistent on AWS).
    pub last_event_time: Option<SystemTime>,
    /// When the last event was ingested.
    pub last_ingestion_time: Option<SystemTime>,
    /// Stored size (deprecated by AWS; may be 0).
    pub stored_bytes: i64,
}

impl LogStream {
    #[allow(deprecated)]
    pub(crate) fn from_sdk(s: &aws_sdk_cloudwatchlogs::types::LogStream) -> Self {
        LogStream {
            name: s.log_stream_name().unwrap_or_default().to_owned(),
            arn: s.arn().unwrap_or_default().to_owned(),
            creation_time: ms_time(s.creation_time()),
            first_event_time: ms_time(s.first_event_timestamp()),
            last_event_time: ms_time(s.last_event_timestamp()),
            last_ingestion_time: ms_time(s.last_ingestion_time()),
            stored_bytes: s.stored_bytes().unwrap_or(0),
        }
    }
}

/// Options of [`ensure_log_group`](crate::CloudWatchLogsClient::ensure_log_group).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnsureLogGroupOptions {
    /// Retention in days (one of [`RETENTION_DAYS`]); `None` = leave as is.
    pub retention_days: Option<u32>,
    /// KMS key for a new group. An existing group with a different key gives `CONFLICT` (keys
    /// are never re-associated silently).
    pub kms_key_id: Option<String>,
    /// Tags added to the group (existing tags are kept, same keys overwritten).
    pub tags: BTreeMap<String, String>,
}

impl EnsureLogGroupOptions {
    /// Sets the retention.
    pub fn retention_days(mut self, days: u32) -> Self {
        self.retention_days = Some(days);
        self
    }
    /// Sets the KMS key.
    pub fn kms_key_id(mut self, key: impl Into<String>) -> Self {
        self.kms_key_id = Some(key.into());
        self
    }
    /// Adds one tag.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

/// What [`ensure_log_group`](crate::CloudWatchLogsClient::ensure_log_group) did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsureLogGroupResult {
    /// `true` when the group did not exist before.
    pub created: bool,
    /// Group ARN without `:*`.
    pub arn: String,
}

/// Options of [`ensure_log_stream`](crate::CloudWatchLogsClient::ensure_log_stream).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EnsureLogStreamOptions {
    /// Create the log group first (`ensure_log_group` without options).
    pub create_group: bool,
}

/// What [`ensure_log_stream`](crate::CloudWatchLogsClient::ensure_log_stream) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnsureLogStreamResult {
    /// `true` when the stream did not exist before.
    pub created: bool,
}

/// Options of `delete_log_group` / `delete_log_stream`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Return `Ok(false)` instead of `NOT_FOUND` for a missing resource.
    pub ignore_missing: bool,
}

/// Options of [`describe_log_groups`](crate::CloudWatchLogsClient::describe_log_groups).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescribeLogGroupsOptions {
    /// Only names starting with this prefix.
    pub prefix: Option<String>,
    /// Page size of each call (1–50; `None` = service default).
    pub page_size: Option<usize>,
    /// Stop after this many groups (`None` = all).
    pub limit: Option<usize>,
}

/// Order of [`describe_log_streams`](crate::CloudWatchLogsClient::describe_log_streams).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StreamOrder {
    /// By stream name (default).
    #[default]
    LogStreamName,
    /// By last event time (cannot be combined with a prefix).
    LastEventTime,
}

/// Options of [`describe_log_streams`](crate::CloudWatchLogsClient::describe_log_streams).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescribeLogStreamsOptions {
    /// Only names starting with this prefix (not with [`StreamOrder::LastEventTime`]).
    pub prefix: Option<String>,
    /// Ordering.
    pub order_by: StreamOrder,
    /// Reverse the order.
    pub descending: bool,
    /// Page size of each call (1–50; `None` = service default).
    pub page_size: Option<usize>,
    /// Stop after this many streams (`None` = all).
    pub limit: Option<usize>,
}

/// One event for `put_log_events`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEvent {
    /// When the event happened (`None` = now). CloudWatch Logs keeps millisecond precision.
    pub timestamp: Option<SystemTime>,
    /// Raw event text.
    pub message: String,
}

impl LogEvent {
    /// An event stamped now.
    pub fn new(message: impl Into<String>) -> Self {
        LogEvent {
            timestamp: None,
            message: message.into(),
        }
    }
    /// An event with an explicit timestamp.
    pub fn at(timestamp: SystemTime, message: impl Into<String>) -> Self {
        LogEvent {
            timestamp: Some(timestamp),
            message: message.into(),
        }
    }
}

/// Options of [`put_log_events`](crate::CloudWatchLogsClient::put_log_events).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutLogEventsOptions {
    /// Create a missing stream and retry once (`None` = the client's setting, default true).
    pub auto_create: Option<bool>,
    /// Also create a missing log group when auto-creating.
    pub create_group: bool,
    /// Overrides the client's per-event size limit.
    pub max_event_bytes: Option<usize>,
    /// Overrides the client's oversize policy.
    pub oversize_policy: Option<OversizePolicy>,
}

/// `rejectedLogEventsInfo` of one `PutLogEvents` call. Indexes refer to the timestamp-sorted
/// events of that batch. As documented by AWS, `too_old_log_event_end_index` and
/// `expired_log_event_end_index` are exclusive (events `[0, index)` rejected) and
/// `too_new_log_event_start_index` is inclusive (events `[index, n)` rejected).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejectedInfo {
    /// 0-based index of the `PutLogEvents` call within `put_log_events`.
    pub batch: usize,
    /// End (exclusive) of the events rejected as too old.
    pub too_old_log_event_end_index: Option<usize>,
    /// Start (inclusive) of the events rejected as too new.
    pub too_new_log_event_start_index: Option<usize>,
    /// End (exclusive) of the events rejected as expired.
    pub expired_log_event_end_index: Option<usize>,
}

impl RejectedInfo {
    /// How many of `n` events this info rejects.
    pub fn count(&self, n: usize) -> usize {
        let head = self
            .too_old_log_event_end_index
            .unwrap_or(0)
            .max(self.expired_log_event_end_index.unwrap_or(0))
            .min(n);
        let tail = self
            .too_new_log_event_start_index
            .unwrap_or(n)
            .max(head)
            .min(n);
        head + (n - tail)
    }
}

/// What [`put_log_events`](crate::CloudWatchLogsClient::put_log_events) did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutLogEventsResult {
    /// Events CloudWatch Logs accepted.
    pub accepted: usize,
    /// Events rejected as too old, too new or expired (never retried).
    pub rejected: usize,
    /// Oversize events dropped by [`OversizePolicy::Drop`].
    pub dropped: usize,
    /// Oversize events truncated by [`OversizePolicy::Truncate`].
    pub truncated: usize,
    /// Number of `PutLogEvents` calls made.
    pub batches: usize,
    /// Rejection details of each batch with rejections.
    pub rejected_info: Vec<RejectedInfo>,
}

/// One event of `FilterLogEvents`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredLogEvent {
    /// Unique within the group.
    pub event_id: String,
    /// Stream that holds the event.
    pub log_stream_name: String,
    /// Event text.
    pub message: String,
    /// Event time.
    pub timestamp: SystemTime,
    /// When CloudWatch received it.
    pub ingestion_time: Option<SystemTime>,
}

impl FilteredLogEvent {
    pub(crate) fn from_sdk(e: &aws_sdk_cloudwatchlogs::types::FilteredLogEvent) -> Self {
        FilteredLogEvent {
            event_id: e.event_id().unwrap_or_default().to_owned(),
            log_stream_name: e.log_stream_name().unwrap_or_default().to_owned(),
            message: e.message().unwrap_or_default().to_owned(),
            timestamp: ms_time(e.timestamp()).unwrap_or(UNIX_EPOCH),
            ingestion_time: ms_time(e.ingestion_time()),
        }
    }
}

/// One event of `GetLogEvents`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamLogEvent {
    /// Event text.
    pub message: String,
    /// Event time.
    pub timestamp: SystemTime,
    /// When CloudWatch received it.
    pub ingestion_time: Option<SystemTime>,
}

/// Options of `filter_log_events` / `filter_log_events_page`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterLogEventsOptions {
    /// Log group (default: the client's log group).
    pub group: Option<String>,
    /// Only these streams (exclusive with `stream_prefix`).
    pub streams: Vec<String>,
    /// Only streams with this prefix.
    pub stream_prefix: Option<String>,
    /// CloudWatch Logs filter pattern (`None` = every event).
    pub pattern: Option<String>,
    /// Earliest event timestamp (inclusive).
    pub start: Option<SystemTime>,
    /// Latest event timestamp (inclusive).
    pub end: Option<SystemTime>,
    /// Page size of each call (1–10,000; `None` = service default).
    pub page_size: Option<usize>,
    /// Stop after this many events in total (`None` = no limit; ignored by the page call).
    pub limit: Option<usize>,
}

/// One `FilterLogEvents` response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterLogEventsPage {
    /// Events ordered by timestamp, then event id.
    pub events: Vec<FilteredLogEvent>,
    /// `None` at the end (AWS may return empty pages with a token).
    pub next_token: Option<String>,
}

/// Options of [`get_log_events`](crate::CloudWatchLogsClient::get_log_events).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetLogEventsOptions {
    /// Read oldest first (forward). The default reads the newest page first, then backwards.
    pub start_from_head: bool,
    /// Earliest event timestamp.
    pub start: Option<SystemTime>,
    /// Latest event timestamp.
    pub end: Option<SystemTime>,
    /// Page size of each call (1–10,000; `None` = service default).
    pub page_size: Option<usize>,
    /// Stop after this many events in total (`None` = no limit).
    pub limit: Option<usize>,
}

/// Status of a Logs Insights query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryStatus {
    /// Queued.
    Scheduled,
    /// Running (AWS may return partial rows).
    Running,
    /// Finished.
    Complete,
    /// Failed.
    Failed,
    /// Cancelled.
    Cancelled,
    /// Timed out in the service.
    Timeout,
    /// Unknown status.
    Unknown,
}

impl QueryStatus {
    /// The AWS spelling (`Complete`, …).
    pub fn as_str(self) -> &'static str {
        match self {
            QueryStatus::Scheduled => "Scheduled",
            QueryStatus::Running => "Running",
            QueryStatus::Complete => "Complete",
            QueryStatus::Failed => "Failed",
            QueryStatus::Cancelled => "Cancelled",
            QueryStatus::Timeout => "Timeout",
            QueryStatus::Unknown => "Unknown",
        }
    }

    /// Parses the AWS spelling; anything else is `Unknown`.
    pub fn parse(s: &str) -> QueryStatus {
        match s {
            "Scheduled" => QueryStatus::Scheduled,
            "Running" => QueryStatus::Running,
            "Complete" => QueryStatus::Complete,
            "Failed" => QueryStatus::Failed,
            "Cancelled" => QueryStatus::Cancelled,
            "Timeout" => QueryStatus::Timeout,
            _ => QueryStatus::Unknown,
        }
    }

    /// Whether the query has finished (anything but `Scheduled` and `Running`).
    pub fn is_terminal(self) -> bool {
        !matches!(self, QueryStatus::Scheduled | QueryStatus::Running)
    }
}

impl std::fmt::Display for QueryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Scan statistics of a query (AWS only).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct QueryStatistics {
    /// Records that matched.
    pub records_matched: f64,
    /// Records scanned.
    pub records_scanned: f64,
    /// Bytes scanned.
    pub bytes_scanned: f64,
}

/// Result of `get_query_results` or `query`.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResults {
    /// Query id.
    pub query_id: String,
    /// Status.
    pub status: QueryStatus,
    /// Rows: field name (`@timestamp`, `@message`, `@logStream`, `@log`, `@ptr`, …) → value.
    pub rows: Vec<BTreeMap<String, String>>,
    /// Statistics when the service reports them.
    pub statistics: Option<QueryStatistics>,
}

/// Options of `start_query` and `query`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    /// Log group names or ARNs (default: the client's log group; at most 50).
    pub groups: Vec<String>,
    /// Query start (default: one hour before `end`). Second precision, rounded down.
    pub start: Option<SystemTime>,
    /// Query end (default now). Second precision, rounded up.
    pub end: Option<SystemTime>,
    /// Row limit (1–10,000; `None` = service default).
    pub limit: Option<usize>,
    /// Timeout of `query` (default 60 s).
    pub timeout: Duration,
    /// First poll interval of `query` (default 1 s, growing to 5 s).
    pub poll_interval: Duration,
    /// How often `query` retries `StartQuery` after `LimitExceededException` (default 3).
    pub retry_on_query_limit: u32,
}

impl Default for QueryOptions {
    fn default() -> Self {
        QueryOptions {
            groups: Vec::new(),
            start: None,
            end: None,
            limit: None,
            timeout: DEFAULT_QUERY_TIMEOUT,
            poll_interval: DEFAULT_QUERY_POLL_INTERVAL,
            retry_on_query_limit: DEFAULT_QUERY_LIMIT_RETRIES,
        }
    }
}

impl QueryOptions {
    /// Sets the groups.
    pub fn groups<I, S>(mut self, groups: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.groups = groups.into_iter().map(Into::into).collect();
        self
    }
    /// Sets the time range.
    pub fn range(mut self, start: Option<SystemTime>, end: Option<SystemTime>) -> Self {
        self.start = start;
        self.end = end;
        self
    }
    /// Sets the row limit.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Sets the timeout of `query`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    /// Sets the first poll interval of `query`.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }
}

/// One Logs Insights query of `describe_queries`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryInfo {
    /// Query id.
    pub query_id: String,
    /// Query text.
    pub query_string: String,
    /// Status.
    pub status: QueryStatus,
    /// When it was created.
    pub create_time: Option<SystemTime>,
    /// Log group.
    pub log_group_name: Option<String>,
}

/// Options of [`describe_queries`](crate::CloudWatchLogsClient::describe_queries).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescribeQueriesOptions {
    /// Only queries of this log group.
    pub group: Option<String>,
    /// Only queries with this status.
    pub status: Option<QueryStatus>,
    /// At most this many queries (`None` = all).
    pub max_results: Option<usize>,
}

/// One `sessionUpdate` frame of a live-tail session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveTailUpdate {
    /// The service sampled the results.
    pub sampled: bool,
    /// Events in the frame (0 for keep-alive updates).
    pub count: usize,
}

/// One event of a live-tail session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTailEvent {
    /// Event text.
    pub message: String,
    /// Event time.
    pub timestamp: SystemTime,
    /// When CloudWatch received it.
    pub ingestion_time: Option<SystemTime>,
    /// Stream that holds the event.
    pub log_stream_name: String,
    /// Group ARN or name as reported by the service.
    pub log_group_identifier: String,
    /// The service sampled the update (more than ~500 events/s).
    pub sampled: bool,
}

/// Callback receiving live-tail events.
pub type LiveTailCallback = std::sync::Arc<dyn Fn(LiveTailEvent) + Send + Sync>;
/// Callback receiving the error that ended a live-tail session.
pub type LiveTailErrorCallback = std::sync::Arc<dyn Fn(&Error) + Send + Sync>;
/// Callback receiving live-tail update frames.
pub type LiveTailUpdateCallback = std::sync::Arc<dyn Fn(LiveTailUpdate) + Send + Sync>;

/// Options of [`start_live_tail`](crate::CloudWatchLogsClient::start_live_tail).
#[derive(Clone, Default)]
pub struct LiveTailOptions {
    /// Log group names or ARNs (default: the client's log group; at most 10). Names become
    /// ARNs with the client's region and account.
    pub groups: Vec<String>,
    /// Only these streams (exclusive with `stream_prefixes`; one group only).
    pub streams: Vec<String>,
    /// Only streams with these prefixes (one group only).
    pub stream_prefixes: Vec<String>,
    /// CloudWatch Logs filter pattern (`None` = every event).
    pub pattern: Option<String>,
    /// Called with the error that ends the session (also in `LiveTailSession::error`).
    pub on_error: Option<LiveTailErrorCallback>,
    /// Called for every `sessionUpdate` frame, including empty keep-alive updates.
    pub on_update: Option<LiveTailUpdateCallback>,
}

impl std::fmt::Debug for LiveTailOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveTailOptions")
            .field("groups", &self.groups)
            .field("streams", &self.streams)
            .field("stream_prefixes", &self.stream_prefixes)
            .field("pattern", &self.pattern)
            .field("on_error", &self.on_error.is_some())
            .field("on_update", &self.on_update.is_some())
            .finish()
    }
}

impl LiveTailOptions {
    /// Sets the groups.
    pub fn groups<I, S>(mut self, groups: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.groups = groups.into_iter().map(Into::into).collect();
        self
    }
    /// Sets the filter pattern.
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }
    /// Sets the error callback.
    pub fn on_error(mut self, f: impl Fn(&Error) + Send + Sync + 'static) -> Self {
        self.on_error = Some(std::sync::Arc::new(f));
        self
    }
    /// Sets the update callback.
    pub fn on_update(mut self, f: impl Fn(LiveTailUpdate) + Send + Sync + 'static) -> Self {
        self.on_update = Some(std::sync::Arc::new(f));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        assert!(validate_group_name("op", "a/b-c_d.e#f").is_ok());
        assert!(validate_group_name("op", "").is_err());
        assert!(validate_group_name("op", "bad name").is_err());
        assert!(validate_group_name("op", &"a".repeat(513)).is_err());
        assert!(validate_stream_name("op", "s").is_ok());
        assert!(validate_stream_name("op", "a:b").is_err());
        assert!(validate_retention("op", 0).is_ok());
        assert!(validate_retention("op", 7).is_ok());
        assert!(validate_retention("op", 8).is_err());
        assert!(validate_page_size("op", Some(0), 50).is_err());
        assert!(validate_page_size("op", Some(51), 50).is_err());
        assert!(validate_page_size("op", None, 50).is_ok());
        assert_eq!(trim_star_arn("arn:x:*"), "arn:x");
        assert_eq!(ms_time(Some(0)), None);
        assert_eq!(to_ms(UNIX_EPOCH - Duration::from_secs(1)), 0);
        assert_eq!(clamp_i32(usize::MAX), i32::MAX);
        for s in [
            "Scheduled",
            "Running",
            "Complete",
            "Failed",
            "Cancelled",
            "Timeout",
            "Unknown",
        ] {
            assert_eq!(QueryStatus::parse(s).to_string(), s);
        }
        assert_eq!(QueryStatus::parse("x"), QueryStatus::Unknown);
        let r = RejectedInfo {
            batch: 0,
            too_old_log_event_end_index: Some(1),
            too_new_log_event_start_index: Some(2),
            expired_log_event_end_index: None,
        };
        assert_eq!(r.count(3), 2);
        let opts = LiveTailOptions::default()
            .groups(["g"])
            .pattern("p")
            .on_error(|_| {})
            .on_update(|_| {});
        assert!(format!("{opts:?}").contains("on_error: true"));
        let q = QueryOptions::default()
            .groups(["g"])
            .range(None, None)
            .limit(3)
            .timeout(Duration::from_secs(1))
            .poll_interval(Duration::from_millis(5));
        assert_eq!(q.limit, Some(3));
        let e = EnsureLogGroupOptions::default()
            .retention_days(7)
            .kms_key_id("k")
            .tag("a", "b");
        assert_eq!(e.tags.len(), 1);
        assert_eq!(LogEvent::new("m").timestamp, None);
        assert!(LogEvent::at(UNIX_EPOCH, "m").timestamp.is_some());
    }
}
