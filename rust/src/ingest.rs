//! Ingest: `put_log_events` with client-side batching that respects the service limits.

use std::time::SystemTime;

use aws_sdk_cloudwatchlogs::types::InputLogEvent;

use crate::client::CloudWatchLogsClient;
use crate::config::{OversizePolicy, MAX_EVENT_BYTES_LIMIT};
use crate::core::{Error, ErrorCode, Result};
use crate::models::{
    to_ms, validate_group_name, validate_stream_name, EnsureLogStreamOptions, LogEvent,
    PutLogEventsOptions, PutLogEventsResult, RejectedInfo, EVENT_OVERHEAD_BYTES, MAX_BATCH_BYTES,
    MAX_BATCH_EVENTS, MAX_BATCH_SPAN,
};

/// The result of [`plan_batches`].
#[derive(Debug, Default)]
pub(crate) struct BatchPlan {
    pub batches: Vec<Vec<(i64, String)>>,
    pub dropped: usize,
    pub truncated: usize,
}

/// Sorts the events (stable, by timestamp), applies the size policy and splits them into calls
/// that respect 10,000 events, 1 MiB (incl. 26 bytes per event) and a 24 h span.
pub(crate) fn plan_batches(
    op: &str,
    events: Vec<LogEvent>,
    max_bytes: usize,
    policy: OversizePolicy,
    now: SystemTime,
) -> Result<BatchPlan> {
    let mut plan = BatchPlan::default();
    let now_ms = to_ms(now);
    let mut items: Vec<(i64, String)> = Vec::with_capacity(events.len());
    for e in events {
        let ts = e.timestamp.map(to_ms).unwrap_or(now_ms);
        let mut msg = e.message;
        if msg.len() > max_bytes {
            match policy {
                OversizePolicy::Drop => {
                    plan.dropped += 1;
                    continue;
                }
                OversizePolicy::Error => {
                    return Err(Error::validation(format!(
                        "{op}: event of {} bytes exceeds maxEventBytes {max_bytes}",
                        msg.len()
                    )))
                }
                OversizePolicy::Truncate => {
                    msg = truncate_message(&msg, max_bytes);
                    plan.truncated += 1;
                }
            }
        }
        items.push((ts, msg));
    }
    items.sort_by_key(|(ts, _)| *ts);
    let span_ms = MAX_BATCH_SPAN.as_millis() as i64;
    let mut current: Vec<(i64, String)> = Vec::new();
    let mut current_bytes = 0usize;
    let mut first = 0i64;
    for (ts, msg) in items {
        let size = msg.len() + EVENT_OVERHEAD_BYTES;
        if !current.is_empty()
            && (current.len() >= MAX_BATCH_EVENTS
                || current_bytes + size > MAX_BATCH_BYTES
                || ts - first > span_ms)
        {
            plan.batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        if current.is_empty() {
            first = ts;
        }
        current.push((ts, msg));
        current_bytes += size;
    }
    if !current.is_empty() {
        plan.batches.push(current);
    }
    Ok(plan)
}

fn truncation_marker(n: usize) -> String {
    format!(" …[truncated {n} bytes]")
}

/// Largest index `<= n` that starts a character of `s`.
fn char_boundary(s: &str, n: usize) -> usize {
    if n >= s.len() {
        return s.len();
    }
    let mut n = n;
    while n > 0 && !s.is_char_boundary(n) {
        n -= 1;
    }
    n
}

/// Shortens `message` to at most `max_bytes` UTF-8 bytes, cutting on a character boundary and
/// appending ` …[truncated N bytes]` (N = bytes removed). Messages that fit are returned
/// unchanged; when `max_bytes` is smaller than the marker the message is cut without it.
pub fn truncate_message(message: &str, max_bytes: usize) -> String {
    if message.len() <= max_bytes {
        return message.to_owned();
    }
    // The marker with N = len(message) is at least as long as the final marker.
    let marker_len = truncation_marker(message.len()).len();
    if marker_len > max_bytes {
        return message[..char_boundary(message, max_bytes)].to_owned();
    }
    let cut = char_boundary(message, max_bytes - marker_len);
    format!(
        "{}{}",
        &message[..cut],
        truncation_marker(message.len() - cut)
    )
}

impl CloudWatchLogsClient {
    /// Sends events to a stream. The events are stably sorted by timestamp, oversize events
    /// follow the size policy, and the events are split into calls that respect the service
    /// limits (10,000 events, 1 MiB including 26 bytes per event, 24 h span). A missing stream
    /// is created once when auto-create is on. Throttling and 5xx are retried by the SDK; a
    /// response lost after sending may duplicate events. Events rejected as too old or too new
    /// are reported, never retried.
    pub async fn put_log_events(
        &self,
        group: &str,
        stream: &str,
        events: Vec<LogEvent>,
        opts: PutLogEventsOptions,
    ) -> Result<PutLogEventsResult> {
        let op = format!("putLogEvents {group}/{stream}");
        validate_group_name(&op, group)?;
        validate_stream_name(&op, stream)?;
        let max_bytes = opts
            .max_event_bytes
            .unwrap_or_else(|| self.config().max_event_bytes());
        if !(1..=MAX_EVENT_BYTES_LIMIT).contains(&max_bytes) {
            return Err(Error::validation(format!(
                "{op}: maxEventBytes must be between 1 and {MAX_EVENT_BYTES_LIMIT}"
            )));
        }
        let policy = opts
            .oversize_policy
            .unwrap_or_else(|| self.config().oversize_policy());
        let plan = plan_batches(&op, events, max_bytes, policy, SystemTime::now())?;
        let mut result = PutLogEventsResult {
            dropped: plan.dropped,
            truncated: plan.truncated,
            ..Default::default()
        };
        let auto_create = opts
            .auto_create
            .unwrap_or_else(|| self.config().auto_create());
        for batch in plan.batches {
            let n = batch.len();
            let input: Vec<InputLogEvent> = batch
                .into_iter()
                .map(|(ts, msg)| {
                    InputLogEvent::builder()
                        .timestamp(ts)
                        .message(msg)
                        .build()
                        .expect("timestamp and message are set")
                })
                .collect();
            let out = match self.put_batch(&op, group, stream, input.clone()).await {
                Err(e) if auto_create && e.code() == ErrorCode::NotFound => {
                    self.ensure_log_stream(
                        group,
                        stream,
                        EnsureLogStreamOptions {
                            create_group: opts.create_group,
                        },
                    )
                    .await?;
                    self.put_batch(&op, group, stream, input).await?
                }
                other => other?,
            };
            let index = result.batches;
            result.batches += 1;
            let mut rejected = 0;
            if let Some(info) = out.rejected_log_events_info() {
                let to_index = |v: Option<i32>| v.and_then(|i| usize::try_from(i).ok());
                let ri = RejectedInfo {
                    batch: index,
                    too_old_log_event_end_index: to_index(info.too_old_log_event_end_index()),
                    too_new_log_event_start_index: to_index(info.too_new_log_event_start_index()),
                    expired_log_event_end_index: to_index(info.expired_log_event_end_index()),
                };
                rejected = ri.count(n);
                result.rejected_info.push(ri);
            }
            result.rejected += rejected;
            result.accepted += n - rejected;
        }
        Ok(result)
    }

    async fn put_batch(
        &self,
        op: &str,
        group: &str,
        stream: &str,
        events: Vec<InputLogEvent>,
    ) -> Result<aws_sdk_cloudwatchlogs::operation::put_log_events::PutLogEventsOutput> {
        let sdk = self.logs(op).await?;
        let call = sdk
            .put_log_events()
            .log_group_name(group)
            .log_stream_name(stream)
            .set_log_events(Some(events))
            .send();
        self.run(op, call).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn at(ms: u64, msg: &str) -> LogEvent {
        LogEvent::at(UNIX_EPOCH + Duration::from_millis(ms), msg)
    }

    #[test]
    fn truncation() {
        assert_eq!(truncate_message("short", 10), "short");
        let long = "é".repeat(100); // 200 bytes
        let t = truncate_message(&long, 60);
        assert!(t.len() <= 60, "{}", t.len());
        assert!(t.contains("…[truncated"));
        assert!(t.is_char_boundary(t.len()));
        // marker longer than the limit: plain cut on a boundary
        let t = truncate_message(&long, 5);
        assert_eq!(t, "éé");
        let t = truncate_message(&"x".repeat(1000), 100);
        let removed: usize = t
            .rsplit("truncated ")
            .next()
            .unwrap()
            .trim_end_matches(" bytes]")
            .parse()
            .unwrap();
        assert_eq!(t.len() - truncation_marker(removed).len() + removed, 1000);
    }

    #[test]
    fn planning_sorts_and_splits() {
        let now = UNIX_EPOCH + Duration::from_secs(1000);
        let plan = plan_batches(
            "op",
            vec![at(3, "c"), at(1, "a"), at(2, "b"), LogEvent::new("now")],
            100,
            OversizePolicy::Truncate,
            now,
        )
        .unwrap();
        assert_eq!(plan.batches.len(), 1);
        let msgs: Vec<&str> = plan.batches[0].iter().map(|(_, m)| m.as_str()).collect();
        assert_eq!(msgs, ["a", "b", "c", "now"]);
        // count limit
        let many: Vec<LogEvent> = (0..10_001).map(|i| at(i, "x")).collect();
        let plan = plan_batches("op", many, 100, OversizePolicy::Truncate, now).unwrap();
        assert_eq!(plan.batches.len(), 2);
        assert_eq!(plan.batches[0].len(), 10_000);
        // byte limit: 5 × 262,144 (+26) > 1 MiB → 2 batches (3 + 2)
        let big: Vec<LogEvent> = (0..5).map(|i| at(i, &"y".repeat(262_144))).collect();
        let plan = plan_batches("op", big, 262_144, OversizePolicy::Truncate, now).unwrap();
        assert_eq!(plan.batches.len(), 2);
        assert_eq!(plan.batches[0].len(), 3);
        // span limit
        let day = 24 * 3600 * 1000;
        let plan = plan_batches(
            "op",
            vec![at(0, "a"), at(day, "b"), at(day + 1, "c")],
            100,
            OversizePolicy::Truncate,
            now,
        )
        .unwrap();
        assert_eq!(plan.batches.len(), 2);
        // policies
        let plan = plan_batches(
            "op",
            vec![at(0, "toolong"), at(1, "ok")],
            3,
            OversizePolicy::Drop,
            now,
        )
        .unwrap();
        assert_eq!((plan.dropped, plan.batches[0].len()), (1, 1));
        let err =
            plan_batches("op", vec![at(0, "toolong")], 3, OversizePolicy::Error, now).unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
        let plan = plan_batches(
            "op",
            vec![at(0, "toolong")],
            3,
            OversizePolicy::Truncate,
            now,
        )
        .unwrap();
        assert_eq!(plan.truncated, 1);
        assert!(plan_batches("op", vec![], 3, OversizePolicy::Truncate, now)
            .unwrap()
            .batches
            .is_empty());
    }
}
