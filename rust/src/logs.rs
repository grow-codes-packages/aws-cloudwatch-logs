//! Logging category mapping (SPEC §4): the `LogExporter`, the batching `LogClient` and the
//! `LogQueryClient` over one log group.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::client::CloudWatchLogsClient;
use crate::config::OversizePolicy;
use crate::core::{Error, ErrorCode, Result};
use crate::ingest::truncate_message;
use crate::logging::{
    Attributes, BatchOptions, BatchingLogClient, ExportOptions, ExportResult, LogExporter,
    LogQueryClient, LogRecord, LogSearchResult, RecordCallback, RedactionProcessor, SearchOptions,
    Severity, SortOrder, TailHandle, TailOptions,
};
use crate::models::{
    to_ms, validate_stream_name, DescribeLogStreamsOptions, FilterLogEventsOptions,
    FilteredLogEvent, LiveTailEvent, LiveTailOptions, LogEvent, PutLogEventsOptions, ATTR_EVENT_ID,
    ATTR_LOG_GROUP, ATTR_LOG_STREAM, MAX_EVENTS_PAGE_SIZE,
};
use crate::query::sort_events;

/// Turns a search query into a CloudWatch Logs filter pattern: empty stays empty (every
/// event), a query that starts with `{`, `?`, `[`, `-` or `"` is passed through as a native
/// pattern, anything else becomes an exact quoted phrase.
pub fn filter_pattern(query: &str) -> String {
    let q = query.trim();
    match q.chars().next() {
        None => String::new(),
        Some('{' | '?' | '[' | '-' | '"') => q.to_owned(),
        Some(_) => format!("\"{}\"", q.replace('\\', "\\\\").replace('"', "\\\"")),
    }
}

/// Decodes a message; the flag is `true` for plain text.
fn decode_message(
    message: &str,
    timestamp: SystemTime,
    ingested: Option<SystemTime>,
) -> (LogRecord, bool) {
    let trimmed = message.trim();
    if trimmed.starts_with('{') {
        if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(trimmed) {
            if map.contains_key("timestamp") && map.contains_key("body") {
                if let Ok(record) = LogRecord::from_json(trimmed) {
                    return (record, false);
                }
            }
        }
    }
    let mut record = LogRecord::new(Severity::Info, message);
    record.timestamp = timestamp;
    record.observed_timestamp = ingested;
    (record, true)
}

/// Decodes a CloudWatch Logs message: a normalized LogRecord JSON document (with `timestamp`
/// and `body`) is decoded as such; any other message becomes an `INFO` record with the message
/// as body, `timestamp` as timestamp and `ingested` as observed timestamp.
pub fn message_to_record(
    message: &str,
    timestamp: SystemTime,
    ingested: Option<SystemTime>,
) -> LogRecord {
    decode_message(message, timestamp, ingested).0
}

/// A `FilterLogEvents` event as a [`LogRecord`] (see [`message_to_record`]) with the
/// attributes `aws.log.stream` and `aws.event.id`.
pub fn filtered_event_to_record(e: &FilteredLogEvent) -> LogRecord {
    let mut r = message_to_record(&e.message, e.timestamp, e.ingestion_time);
    if !e.log_stream_name.is_empty() {
        r.attributes
            .insert(ATTR_LOG_STREAM.into(), e.log_stream_name.clone().into());
    }
    if !e.event_id.is_empty() {
        r.attributes
            .insert(ATTR_EVENT_ID.into(), e.event_id.clone().into());
    }
    r
}

/// A live-tail event as a [`LogRecord`] (see [`message_to_record`]) with the attribute
/// `aws.log.stream` and, for plain messages, `aws.log.group`.
pub fn live_tail_event_to_record(e: &LiveTailEvent) -> LogRecord {
    let (mut r, plain) = decode_message(&e.message, e.timestamp, e.ingestion_time);
    if !e.log_stream_name.is_empty() {
        r.attributes
            .insert(ATTR_LOG_STREAM.into(), e.log_stream_name.clone().into());
    }
    if plain && !e.log_group_identifier.is_empty() {
        r.attributes
            .insert(ATTR_LOG_GROUP.into(), e.log_group_identifier.clone().into());
    }
    r
}

/// The JSON message of `record` within `max_bytes`, or `None` when it must be skipped
/// (drop / error policy). Truncation shortens the body so the message stays valid JSON.
pub(crate) fn encode_record(
    record: &LogRecord,
    max_bytes: usize,
    policy: OversizePolicy,
) -> Option<String> {
    let raw = record.to_json();
    if raw.len() <= max_bytes {
        return Some(raw);
    }
    if policy != OversizePolicy::Truncate {
        return None;
    }
    let mut r = record.clone();
    let original = record.body.clone();
    let (mut lo, mut hi) = (0usize, original.len());
    let mut best: Option<String> = None;
    while lo <= hi {
        let mid = (lo + hi) / 2;
        r.body = truncate_message(&original, mid);
        let text = r.to_json();
        if text.len() <= max_bytes {
            best = Some(text);
            lo = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            hi = mid - 1;
        }
    }
    Some(best.unwrap_or_else(|| truncate_message(&raw, max_bytes)))
}

/// Where an exporter or log client writes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExporterOptions {
    /// Log group (default: the client's log group; required).
    pub group: Option<String>,
    /// Log stream (default: the client's log stream). A missing stream is created on first use
    /// unless auto-create is off.
    pub stream: Option<String>,
    /// Also create a missing log group on the first export.
    pub create_group: bool,
}

/// The [`LogExporter`] of the client: writes normalized LogRecord JSON documents with
/// `PutLogEvents` to one log group and stream. `gzip` is ignored (PutLogEvents has no request
/// compression).
#[derive(Debug, Clone)]
pub struct CloudWatchLogsExporter {
    client: CloudWatchLogsClient,
    group: String,
    stream: String,
    create_group: bool,
}

impl CloudWatchLogsExporter {
    /// Log group written to.
    pub fn group(&self) -> &str {
        &self.group
    }
    /// Log stream written to.
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

#[async_trait]
impl LogExporter for CloudWatchLogsExporter {
    /// Each record becomes one event whose message is the record's normalized JSON and whose
    /// timestamp is the record timestamp (milliseconds). Oversize records follow the client's
    /// size policy (truncation shortens the body).
    async fn export(&self, records: &[LogRecord], _opts: ExportOptions) -> Result<ExportResult> {
        let cfg = self.client.config();
        let mut events = Vec::with_capacity(records.len());
        let mut skipped = 0usize;
        for r in records {
            match encode_record(r, cfg.max_event_bytes(), cfg.oversize_policy()) {
                Some(msg) => events.push(LogEvent::at(r.timestamp, msg)),
                None => skipped += 1,
            }
        }
        let opts = PutLogEventsOptions {
            create_group: self.create_group,
            oversize_policy: Some(OversizePolicy::Truncate),
            ..Default::default()
        };
        let res = self
            .client
            .put_log_events(&self.group, &self.stream, events, opts)
            .await?;
        let rejected = res.rejected + skipped;
        let error = (rejected > 0).then(|| {
            Error::validation(format!(
                "export: {rejected} of {} records were rejected (oversize, too old or too new)",
                records.len()
            ))
        });
        Ok(ExportResult::partial(res.accepted, rejected, error))
    }
}

/// Options of [`CloudWatchLogsClient::log_client`]: where to write plus the batching,
/// resource, redaction and severity options of the generated [`BatchingLogClient`].
#[derive(Clone)]
pub struct LogClientOptions {
    /// Where to write.
    pub exporter: ExporterOptions,
    /// Batch options.
    pub batch: BatchOptions,
    /// Resource attributes added to every record.
    pub resource: Attributes,
    /// Redaction (default [`RedactionProcessor::default`]; `None` disables it).
    pub redaction: Option<RedactionProcessor>,
    /// Minimum severity of loggers.
    pub min_severity: Severity,
}

impl Default for LogClientOptions {
    fn default() -> Self {
        LogClientOptions {
            exporter: ExporterOptions::default(),
            batch: BatchOptions::default(),
            resource: Attributes::new(),
            redaction: Some(RedactionProcessor::default()),
            min_severity: Severity::Info,
        }
    }
}

impl std::fmt::Debug for LogClientOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogClientOptions")
            .field("exporter", &self.exporter)
            .field("batch", &self.batch)
            .field("resource", &self.resource)
            .field("min_severity", &self.min_severity)
            .finish_non_exhaustive()
    }
}

/// Decoded descending-search cursor: continue with events at or before `e` (ms), skipping the
/// first `s` events stamped exactly `e`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DescCursor {
    e: i64,
    s: usize,
}

const DESC_CURSOR_PREFIX: &str = "d1.";
/// Width of the newest slice of a descending search; each further slice is 4× wider.
const FIRST_DESC_SLICE: Duration = Duration::from_secs(5 * 60);
/// Widest slice of a descending search.
const MAX_DESC_SLICE: Duration = Duration::from_secs(30 * 24 * 3600);
/// Most events read for one slice before it is narrowed.
const SEARCH_SCAN_CAP: usize = 10_000;
/// Window of a descending search without `start` when the group's creation time is unknown.
const DEFAULT_DESC_WINDOW: Duration = Duration::from_secs(30 * 24 * 3600);

fn encode_desc_cursor(c: &DescCursor) -> String {
    let raw = serde_json::to_vec(c).unwrap_or_default();
    format!(
        "{DESC_CURSOR_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
    )
}

fn decode_desc_cursor(s: &str) -> Option<DescCursor> {
    let body = s.strip_prefix(DESC_CURSOR_PREFIX)?;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(body)
        .ok()?;
    serde_json::from_slice(&raw).ok()
}

/// The [`LogQueryClient`] over one log group: `search` uses `FilterLogEvents`, `tail` live
/// tail, `list_streams` `DescribeLogStreams`; `list_labels` is `UNSUPPORTED`.
#[derive(Debug, Clone)]
pub struct CloudWatchLogsQueryClient {
    client: CloudWatchLogsClient,
    group: String,
}

impl CloudWatchLogsQueryClient {
    /// The log group searched.
    pub fn group(&self) -> &str {
        &self.group
    }

    async fn read_slice(&self, pattern: &str, lo: i64, hi: i64) -> Result<Vec<FilteredLogEvent>> {
        let at = |ms: i64| UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64);
        self.client
            .filter_log_events(FilterLogEventsOptions {
                group: Some(self.group.clone()),
                pattern: Some(pattern.to_owned()),
                start: Some(at(lo)),
                end: Some(at(hi)),
                ..Default::default()
            })
            .try_collect()
            .await
    }

    async fn search_asc(&self, query: &str, opts: &SearchOptions) -> Result<LogSearchResult> {
        let params = self.client.filter_params(&FilterLogEventsOptions {
            group: Some(self.group.clone()),
            pattern: Some(filter_pattern(query)),
            start: opts.start,
            end: opts.end,
            ..Default::default()
        })?;
        let mut events: Vec<FilteredLogEvent> = Vec::new();
        let mut token = opts.cursor.clone().filter(|c| !c.is_empty());
        while events.len() < opts.limit {
            let want = (opts.limit - events.len()).min(MAX_EVENTS_PAGE_SIZE);
            let (mut page, next) = self
                .client
                .filter_call(&params, token.clone(), Some(want))
                .await?;
            sort_events(&mut page, false);
            events.extend(page);
            match next {
                Some(n) if !n.is_empty() && Some(&n) != token.as_ref() => token = Some(n),
                _ => {
                    token = None;
                    break;
                }
            }
        }
        Ok(LogSearchResult {
            records: events.iter().map(filtered_event_to_record).collect(),
            next_cursor: token,
            total: None,
        })
    }

    async fn search_desc(&self, query: &str, opts: &SearchOptions) -> Result<LogSearchResult> {
        let op = "logs.search";
        let end = opts.end.unwrap_or_else(SystemTime::now);
        let (upper, skip) = match &opts.cursor {
            Some(c) if !c.is_empty() => {
                let cur = decode_desc_cursor(c)
                    .ok_or_else(|| Error::validation(format!("{op}: invalid cursor")))?;
                (cur.e, cur.s)
            }
            _ => (to_ms(end), 0),
        };
        let start = match opts.start {
            Some(s) => s,
            None => match self.client.find_group(op, &self.group).await {
                Ok(g) => g
                    .creation_time
                    // events may be stamped before the group was created
                    .map(|t| t - Duration::from_secs(3600))
                    .unwrap_or(end - DEFAULT_DESC_WINDOW),
                Err(e) if e.code() == ErrorCode::NotFound => end - DEFAULT_DESC_WINDOW,
                Err(e) => return Err(e),
            },
        };
        let lower = to_ms(start);
        let pattern = filter_pattern(query);
        let mut collected: Vec<FilteredLogEvent> = Vec::new();
        let mut width = FIRST_DESC_SLICE.as_millis() as i64;
        let mut hi = upper;
        while collected.len() <= opts.limit && hi >= lower {
            let mut lo = lower.max(hi - width + 1);
            let mut slice = self.read_slice(&pattern, lo, hi).await?;
            // A slice that holds more than the scan cap is narrowed to its newest half.
            while slice.len() > SEARCH_SCAN_CAP && hi > lo {
                lo = hi - (hi - lo) / 2;
                width = hi - lo + 1;
                slice = self.read_slice(&pattern, lo, hi).await?;
            }
            sort_events(&mut slice, true);
            if hi == upper && skip > 0 {
                let n = slice
                    .iter()
                    .take(skip)
                    .take_while(|e| to_ms(e.timestamp) == upper)
                    .count();
                slice.drain(..n);
            }
            collected.extend(slice);
            hi = lo - 1;
            width = (width * 4).min(MAX_DESC_SLICE.as_millis() as i64);
        }
        let more = collected.len() > opts.limit || (collected.len() == opts.limit && hi >= lower);
        collected.truncate(opts.limit);
        let mut next_cursor = None;
        if more {
            if let Some(last) = collected.last().map(|e| to_ms(e.timestamp)) {
                let mut n = collected
                    .iter()
                    .filter(|e| to_ms(e.timestamp) == last)
                    .count();
                if last == upper {
                    n += skip;
                }
                next_cursor = Some(encode_desc_cursor(&DescCursor { e: last, s: n }));
            }
        }
        Ok(LogSearchResult {
            records: collected.iter().map(filtered_event_to_record).collect(),
            next_cursor,
            total: None,
        })
    }
}

#[async_trait]
impl LogQueryClient for CloudWatchLogsQueryClient {
    /// `FilterLogEvents` on the group with [`filter_pattern`]`(query)`. `Asc` pages natively
    /// (the cursor is the service token). `Desc` (the default) walks the time window backwards
    /// in growing slices, newest slice first, each sorted newest first; its cursor is opaque.
    /// Messages that are normalized LogRecord JSON are decoded, other messages become `INFO`
    /// records; every record gets `aws.log.stream` and `aws.event.id`.
    async fn search(&self, query: &str, opts: SearchOptions) -> Result<LogSearchResult> {
        if let (Some(s), Some(e)) = (opts.start, opts.end) {
            if e < s {
                return Err(Error::validation(
                    "logs.search: end must not be before start",
                ));
            }
        }
        if opts.limit == 0 {
            return Ok(LogSearchResult::default());
        }
        match opts.order {
            SortOrder::Asc => self.search_asc(query, &opts).await,
            SortOrder::Desc => self.search_desc(query, &opts).await,
        }
    }

    /// Starts a live-tail session on the group with [`filter_pattern`]`(query)` and calls
    /// `on_record` for every event (see [`live_tail_event_to_record`]). `TailOptions` are not
    /// used: the service pushes events as they are ingested. Stopping the handle closes the
    /// session; it also stops by itself when the service ends the session.
    async fn tail(
        &self,
        query: &str,
        on_record: RecordCallback,
        _opts: TailOptions,
    ) -> Result<TailHandle> {
        let pattern = filter_pattern(query);
        let session = self
            .client
            .start_live_tail(
                LiveTailOptions {
                    groups: vec![self.group.clone()],
                    pattern: (!pattern.is_empty()).then_some(pattern),
                    ..Default::default()
                },
                Arc::new(move |e| on_record(live_tail_event_to_record(&e))),
            )
            .await?;
        let handle = TailHandle::new();
        let watcher = handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = watcher.stopped() => session.close(),
                _ = session.done() => watcher.stop(),
            }
        });
        Ok(handle)
    }

    /// Names of all streams of the group.
    async fn list_streams(&self) -> Result<Vec<String>> {
        Ok(self
            .client
            .describe_log_streams(&self.group, DescribeLogStreamsOptions::default())
            .try_collect()
            .await?
            .into_iter()
            .map(|s| s.name)
            .collect())
    }

    /// `UNSUPPORTED`: CloudWatch Logs has no label inventory.
    async fn list_labels(&self) -> Result<Vec<String>> {
        Err(Error::unsupported(
            "logs.listLabels: Amazon CloudWatch Logs has no label inventory",
        ))
    }
}

impl CloudWatchLogsClient {
    /// A [`LogExporter`] writing to `opts.group` / `opts.stream` (`CONFIG` when no group is
    /// known, `VALIDATION` for invalid names).
    pub fn exporter(&self, opts: ExporterOptions) -> Result<CloudWatchLogsExporter> {
        let op = "exporter";
        let group = self.group_or_default(op, opts.group.as_deref())?;
        let stream = opts
            .stream
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.log_stream().to_owned());
        validate_stream_name(op, &stream)?;
        Ok(CloudWatchLogsExporter {
            client: self.clone(),
            group,
            stream,
            create_group: opts.create_group,
        })
    }

    /// A batching [`LogClient`](crate::logging::LogClient) (redaction, `Logger` facade) that
    /// exports to the configured group and stream. The client flushes and closes it on
    /// [`close`](CloudWatchLogsClient::close). Must be called inside a tokio runtime.
    pub fn log_client(&self, opts: LogClientOptions) -> Result<BatchingLogClient> {
        self.check_open("logClient")?;
        let exporter = self.exporter(opts.exporter)?;
        let client = BatchingLogClient::builder(Arc::new(exporter))
            .batch_options(opts.batch)
            .resource(opts.resource)
            .redaction(opts.redaction)
            .min_severity(opts.min_severity)
            .build()?;
        self.register_log_client(client.clone());
        Ok(client)
    }

    /// The [`LogQueryClient`] over `group` (default: the client's log group).
    pub fn log_query_client(&self, group: Option<&str>) -> Result<CloudWatchLogsQueryClient> {
        let group = self.group_or_default("logQueryClient", group)?;
        Ok(CloudWatchLogsQueryClient {
            client: self.clone(),
            group,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns() {
        assert_eq!(filter_pattern("  "), "");
        assert_eq!(filter_pattern("hello world"), "\"hello world\"");
        assert_eq!(filter_pattern(r#"a"b\c"#), r#""a\"b\\c""#);
        for native in ["{ $.x = 1 }", "?a ?b", "[w1, w2]", "-x", "\"q\""] {
            assert_eq!(filter_pattern(native), native);
        }
    }

    #[test]
    fn cursors() {
        let c = DescCursor { e: 42, s: 3 };
        assert_eq!(decode_desc_cursor(&encode_desc_cursor(&c)), Some(c));
        assert_eq!(decode_desc_cursor("x"), None);
        assert_eq!(decode_desc_cursor("d1.!!"), None);
        assert_eq!(decode_desc_cursor("d1.e30"), None); // {} lacks fields
    }

    #[test]
    fn records() {
        let ts = UNIX_EPOCH + Duration::from_millis(1000);
        let r = message_to_record("plain", ts, None);
        assert_eq!(
            (r.body.as_str(), r.severity, r.timestamp),
            ("plain", Severity::Info, ts)
        );
        let src = LogRecord::new(Severity::Error, "boom").with_attribute("k", "v");
        let back = message_to_record(&src.to_json(), ts, None);
        assert_eq!(back.severity, Severity::Error);
        assert_eq!(back.body, "boom");
        // JSON without the record fields stays plain text
        assert_eq!(message_to_record("{\"a\":1}", ts, None).body, "{\"a\":1}");
        assert_eq!(
            message_to_record("{\"timestamp\":\"bad\",\"body\":1}", ts, None).severity,
            Severity::Info
        );
        let e = LiveTailEvent {
            message: "m".into(),
            timestamp: ts,
            ingestion_time: None,
            log_stream_name: "s".into(),
            log_group_identifier: "arn:g".into(),
            sampled: false,
        };
        let r = live_tail_event_to_record(&e);
        assert_eq!(r.attributes[ATTR_LOG_GROUP], "arn:g");
        assert_eq!(r.attributes[ATTR_LOG_STREAM], "s");
        let e2 = LiveTailEvent {
            message: src.to_json(),
            log_stream_name: String::new(),
            ..e
        };
        let r = live_tail_event_to_record(&e2);
        assert!(!r.attributes.contains_key(ATTR_LOG_GROUP));
        let f = FilteredLogEvent {
            event_id: String::new(),
            log_stream_name: String::new(),
            message: "m".into(),
            timestamp: ts,
            ingestion_time: None,
        };
        assert!(filtered_event_to_record(&f).attributes.is_empty());
    }

    #[test]
    fn record_encoding() {
        let mut r = LogRecord::new(Severity::Info, "x".repeat(2000));
        r.timestamp = UNIX_EPOCH;
        let full = r.to_json();
        assert_eq!(
            encode_record(&r, 10_000, OversizePolicy::Truncate),
            Some(full.clone())
        );
        assert_eq!(encode_record(&r, 500, OversizePolicy::Drop), None);
        assert_eq!(encode_record(&r, 500, OversizePolicy::Error), None);
        let t = encode_record(&r, 500, OversizePolicy::Truncate).unwrap();
        assert!(t.len() <= 500);
        let back = LogRecord::from_json(&t).unwrap();
        assert!(back.body.contains("truncated"));
        // limit below the empty-body record: raw truncation
        let t = encode_record(&r, 20, OversizePolicy::Truncate).unwrap();
        assert!(t.len() <= 20);
        let opts = LogClientOptions::default();
        assert!(format!("{opts:?}").contains("LogClientOptions"));
    }
}
