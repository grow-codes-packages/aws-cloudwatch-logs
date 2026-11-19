//! Query functions: `FilterLogEvents`, `GetLogEvents` and Logs Insights.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;

use crate::aws_error::aws_error_code;
use crate::client::CloudWatchLogsClient;
use crate::core::{Error, Result};
use crate::models::{
    clamp_i32, ms_time, to_ms, trim_star_arn, validate_group_name, validate_page_size,
    validate_stream_name, DescribeQueriesOptions, FilterLogEventsOptions, FilterLogEventsPage,
    FilteredLogEvent, GetLogEventsOptions, QueryInfo, QueryOptions, QueryResults, QueryStatistics,
    QueryStatus, StreamLogEvent, MAX_EVENTS_PAGE_SIZE, MAX_QUERY_GROUPS, MAX_QUERY_POLL_INTERVAL,
};
use crate::paginator::Paginator;
use crate::sdk::IntegerMillis;

/// Orders by (timestamp, event id), newest first when `desc`.
pub(crate) fn sort_events(events: &mut [FilteredLogEvent], desc: bool) {
    events.sort_by(|a, b| {
        let r = a
            .timestamp
            .cmp(&b.timestamp)
            .then_with(|| compare_event_ids(&a.event_id, &b.event_id));
        if desc {
            r.reverse()
        } else {
            r
        }
    });
}

/// Compares numeric ids numerically and others as strings.
fn compare_event_ids(a: &str, b: &str) -> Ordering {
    let numeric = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
    if a.len() != b.len() && numeric(a) && numeric(b) {
        return a.len().cmp(&b.len());
    }
    a.cmp(b)
}

/// Validated `FilterLogEvents` request parameters.
#[derive(Clone)]
pub(crate) struct FilterParams {
    pub op: String,
    pub group: String,
    pub streams: Vec<String>,
    pub stream_prefix: Option<String>,
    pub pattern: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub page_size: Option<usize>,
}

/// Validated `StartQuery` parameters.
pub(crate) struct StartQueryParams {
    names: Option<Vec<String>>,
    identifiers: Option<Vec<String>>,
    start: i64,
    end: i64,
    limit: Option<i32>,
}

/// Parses an Insights `@timestamp` value: the AWS form `2006-01-02 15:04:05.000` (UTC) or epoch
/// milliseconds (Moto). `VALIDATION` for anything else.
pub fn parse_query_timestamp(value: &str) -> Result<SystemTime> {
    let s = value.trim();
    if let Ok(ms) = s.parse::<u64>() {
        return Ok(UNIX_EPOCH + Duration::from_millis(ms));
    }
    let iso = if s.len() >= 19 && s.as_bytes()[10] == b' ' {
        format!("{}T{}", &s[..10], &s[11..])
    } else {
        s.to_owned()
    };
    humantime::parse_rfc3339_weak(iso.trim_end_matches('Z'))
        .map_err(|_| Error::validation("parseQueryTimestamp: unrecognised timestamp format"))
}

impl CloudWatchLogsClient {
    pub(crate) fn filter_params(&self, opts: &FilterLogEventsOptions) -> Result<FilterParams> {
        let label = opts
            .group
            .as_deref()
            .or(self.config().log_group())
            .unwrap_or_default();
        let op = format!("filterLogEvents {label}");
        let group = self.group_or_default(&op, opts.group.as_deref())?;
        if !opts.streams.is_empty() && opts.stream_prefix.is_some() {
            return Err(Error::validation(format!(
                "{op}: streams and streamPrefix are mutually exclusive"
            )));
        }
        if let (Some(s), Some(e)) = (opts.start, opts.end) {
            if e < s {
                return Err(Error::validation(format!(
                    "{op}: end must not be before start"
                )));
            }
        }
        validate_page_size(&op, opts.page_size, MAX_EVENTS_PAGE_SIZE)?;
        Ok(FilterParams {
            op,
            group,
            streams: opts.streams.clone(),
            stream_prefix: opts.stream_prefix.clone(),
            pattern: opts.pattern.clone().filter(|p| !p.is_empty()),
            start: opts.start.map(to_ms),
            end: opts.end.map(to_ms),
            page_size: opts.page_size,
        })
    }

    /// One `FilterLogEvents` call with the given parameters.
    pub(crate) async fn filter_call(
        &self,
        p: &FilterParams,
        token: Option<String>,
        limit: Option<usize>,
    ) -> Result<(Vec<FilteredLogEvent>, Option<String>)> {
        let sdk = self.logs(&p.op).await?;
        let call = sdk
            .filter_log_events()
            .log_group_name(&p.group)
            .set_log_stream_names((!p.streams.is_empty()).then(|| p.streams.clone()))
            .set_log_stream_name_prefix(p.stream_prefix.clone())
            .set_filter_pattern(p.pattern.clone())
            .set_start_time(p.start)
            .set_end_time(p.end)
            .set_limit(limit.or(p.page_size).map(clamp_i32))
            .set_next_token(token)
            .send();
        let out = self.run(&p.op, call).await?;
        let events = out
            .events()
            .iter()
            .map(FilteredLogEvent::from_sdk)
            .collect();
        Ok((events, out.next_token().map(str::to_owned)))
    }

    /// Lazily iterates over the events of a group that match the filter pattern, across all
    /// pages (AWS may return empty pages with a token). Validation errors surface on the first
    /// call of the paginator.
    pub fn filter_log_events(&self, opts: FilterLogEventsOptions) -> Paginator<FilteredLogEvent> {
        let params = match self.filter_params(&opts) {
            Ok(p) => p,
            Err(e) => return Paginator::failed(e),
        };
        let client = self.clone();
        Paginator::new(opts.limit, move |token, remaining| {
            let client = client.clone();
            let params = params.clone();
            async move {
                let limit = remaining.map(|r| {
                    r.min(params.page_size.unwrap_or(MAX_EVENTS_PAGE_SIZE))
                        .max(1)
                });
                client.filter_call(&params, token, limit).await
            }
            .boxed()
        })
    }

    /// One `FilterLogEvents` request starting at `next_token` (`None` = first page).
    /// `opts.limit` is ignored; `page_size` caps the page. Events are sorted by timestamp,
    /// then event id.
    pub async fn filter_log_events_page(
        &self,
        opts: FilterLogEventsOptions,
        next_token: Option<&str>,
    ) -> Result<FilterLogEventsPage> {
        let params = self.filter_params(&opts)?;
        let sent = next_token.filter(|t| !t.is_empty()).map(str::to_owned);
        let (mut events, next) = self.filter_call(&params, sent.clone(), None).await?;
        sort_events(&mut events, false);
        let next_token = next.filter(|t| !t.is_empty() && Some(t) != sent.as_ref());
        Ok(FilterLogEventsPage { events, next_token })
    }

    /// Lazily iterates over the events of one stream. Forward iteration (`start_from_head`)
    /// ends when the service returns the token it was sent (on a live stream: the current
    /// end). Backward iteration yields pages newest first (events inside a page stay in
    /// ascending order).
    pub fn get_log_events(
        &self,
        group: &str,
        stream: &str,
        opts: GetLogEventsOptions,
    ) -> Paginator<StreamLogEvent> {
        let op = format!("getLogEvents {group}/{stream}");
        let checks = validate_group_name(&op, group)
            .and_then(|_| validate_stream_name(&op, stream))
            .and_then(|_| validate_page_size(&op, opts.page_size, MAX_EVENTS_PAGE_SIZE));
        if let Err(e) = checks {
            return Paginator::failed(e);
        }
        let client = self.clone();
        let group = group.to_owned();
        let stream = stream.to_owned();
        Paginator::new(opts.limit, move |token, remaining| {
            let client = client.clone();
            let op = op.clone();
            let group = group.clone();
            let stream = stream.clone();
            let opts = opts.clone();
            async move {
                let limit = remaining
                    .map(|r| r.min(opts.page_size.unwrap_or(MAX_EVENTS_PAGE_SIZE)).max(1))
                    .or(opts.page_size);
                let sdk = client.logs(&op).await?;
                let call = sdk
                    .get_log_events()
                    .log_group_name(group)
                    .log_stream_name(stream)
                    .start_from_head(opts.start_from_head)
                    .set_start_time(opts.start.map(to_ms))
                    .set_end_time(opts.end.map(to_ms))
                    .set_limit(limit.map(clamp_i32))
                    .set_next_token(token)
                    .send();
                let out = client.run(&op, call).await?;
                let events = out
                    .events()
                    .iter()
                    .map(|e| StreamLogEvent {
                        message: e.message().unwrap_or_default().to_owned(),
                        timestamp: ms_time(e.timestamp()).unwrap_or(UNIX_EPOCH),
                        ingestion_time: ms_time(e.ingestion_time()),
                    })
                    .collect();
                let next = if opts.start_from_head {
                    out.next_forward_token()
                } else {
                    out.next_backward_token()
                };
                Ok((events, next.map(str::to_owned)))
            }
            .boxed()
        })
    }

    fn start_query_input(
        &self,
        op: &str,
        query: &str,
        opts: &QueryOptions,
    ) -> Result<StartQueryParams> {
        if query.trim().is_empty() {
            return Err(Error::validation(format!("{op}: query string is required")));
        }
        let mut groups = opts.groups.clone();
        if groups.is_empty() {
            if let Some(g) = self.config().log_group() {
                groups.push(g.to_owned());
            }
        }
        if groups.is_empty() {
            return Err(Error::config(format!(
                "{op}: no log groups given and logGroup is not set"
            )));
        }
        if groups.len() > MAX_QUERY_GROUPS {
            return Err(Error::validation(format!("{op}: at most 50 log groups")));
        }
        if matches!(opts.limit, Some(n) if n == 0 || n > MAX_EVENTS_PAGE_SIZE) {
            return Err(Error::validation(format!(
                "{op}: limit must be between 1 and 10000"
            )));
        }
        let end = opts.end.unwrap_or_else(SystemTime::now);
        let start = opts
            .start
            .unwrap_or_else(|| end - Duration::from_secs(3600));
        if end < start {
            return Err(Error::validation(format!(
                "{op}: end must not be before start"
            )));
        }
        let secs = |t: SystemTime| t.duration_since(UNIX_EPOCH).unwrap_or_default();
        let start_s = secs(start).as_secs() as i64;
        let end_d = secs(end);
        let end_s = end_d.as_secs() as i64 + i64::from(end_d.subsec_nanos() > 0);
        let arns = groups.iter().any(|g| g.starts_with("arn:"));
        for g in groups.iter().filter(|g| !g.starts_with("arn:")) {
            validate_group_name(op, g)?;
        }
        let (names, identifiers) = if arns {
            let ids = groups.iter().map(|g| trim_star_arn(g).to_owned()).collect();
            (None, Some(ids))
        } else {
            (Some(groups), None)
        };
        Ok(StartQueryParams {
            names,
            identifiers,
            start: start_s,
            end: end_s,
            limit: opts.limit.map(clamp_i32),
        })
    }

    /// Starts a Logs Insights query and returns its id. Group names go to `logGroupNames`,
    /// ARNs to `logGroupIdentifiers`; the time range has second precision (start rounded down,
    /// end up; default the last hour).
    pub async fn start_query(&self, query: &str, opts: QueryOptions) -> Result<String> {
        let op = "startQuery";
        let p = self.start_query_input(op, query, &opts)?;
        let sdk = self.logs(op).await?;
        let call = sdk
            .start_query()
            .set_log_group_names(p.names)
            .set_log_group_identifiers(p.identifiers)
            .start_time(p.start)
            .end_time(p.end)
            .query_string(query)
            .set_limit(p.limit)
            .send();
        let out = self.run(op, call).await?;
        Ok(out.query_id().unwrap_or_default().to_owned())
    }

    /// Current status and rows of a query. While the status is `Running`, AWS may return
    /// partial rows.
    pub async fn get_query_results(&self, query_id: &str) -> Result<QueryResults> {
        let op = format!("getQueryResults {query_id}");
        if query_id.is_empty() {
            return Err(Error::validation(format!("{op}: query id is required")));
        }
        let sdk = self.logs(&op).await?;
        let out = self
            .run(&op, sdk.get_query_results().query_id(query_id).send())
            .await?;
        let rows = out
            .results()
            .iter()
            .map(|row| {
                row.iter()
                    .map(|f| {
                        (
                            f.field().unwrap_or_default().to_owned(),
                            f.value().unwrap_or_default().to_owned(),
                        )
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .collect();
        Ok(QueryResults {
            query_id: query_id.to_owned(),
            status: out
                .status()
                .map(|s| QueryStatus::parse(s.as_str()))
                .unwrap_or(QueryStatus::Unknown),
            rows,
            statistics: out.statistics().map(|s| QueryStatistics {
                records_matched: s.records_matched(),
                records_scanned: s.records_scanned(),
                bytes_scanned: s.bytes_scanned(),
            }),
        })
    }

    /// Runs a Logs Insights query to completion: `StartQuery` (retried on
    /// `LimitExceededException`), then `GetQueryResults` until a terminal status. `Failed` and
    /// `Unknown` give `UNAVAILABLE`, `Cancelled` `CANCELLED` and `Timeout` `TIMEOUT` errors
    /// naming the query id. After `opts.timeout` the query is stopped (best effort) and
    /// `TIMEOUT` returned; dropping the future cancels the polling.
    pub async fn query(&self, query: &str, opts: QueryOptions) -> Result<QueryResults> {
        let op = "query";
        self.start_query_input(op, query, &opts)?;
        let timeout = if opts.timeout.is_zero() {
            crate::models::DEFAULT_QUERY_TIMEOUT
        } else {
            opts.timeout
        };
        let mut interval = if opts.poll_interval.is_zero() {
            crate::models::DEFAULT_QUERY_POLL_INTERVAL
        } else {
            opts.poll_interval
        };
        let id_slot = std::sync::Mutex::new(None::<String>);
        let work = async {
            let mut attempt = 0u32;
            let id = loop {
                match self.start_query(query, opts.clone()).await {
                    Ok(id) => break id,
                    Err(e)
                        if attempt < opts.retry_on_query_limit
                            && aws_error_code(&e) == Some("LimitExceededException") =>
                    {
                        tokio::time::sleep(interval * 2u32.saturating_pow(attempt)).await;
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                }
            };
            *crate::client::lock(&id_slot) = Some(id.clone());
            loop {
                let res = self.get_query_results(&id).await?;
                if res.status.is_terminal() {
                    let msg = |what: &str| format!("{op} {id}: {what}");
                    return match res.status {
                        QueryStatus::Complete => Ok(res),
                        QueryStatus::Cancelled => {
                            Err(Error::cancelled(msg("the query was cancelled")))
                        }
                        QueryStatus::Timeout => {
                            Err(Error::timeout(msg("the query timed out in the service")))
                        }
                        other => Err(Error::unavailable(msg(&format!(
                            "query ended with status {other}"
                        )))
                        .with_retryable(false)),
                    };
                }
                tokio::time::sleep(interval).await;
                interval = (interval * 3 / 2).min(MAX_QUERY_POLL_INTERVAL);
            }
        };
        match tokio::time::timeout(timeout, work).await {
            Ok(result) => result,
            Err(_) => {
                let id = crate::client::lock(&id_slot).clone();
                let name = match &id {
                    Some(id) => format!("{op} {id}"),
                    None => op.to_owned(),
                };
                if let Some(id) = id {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(5), self.stop_query(&id)).await;
                }
                Err(Error::timeout(format!(
                    "{name}: query did not complete in time"
                )))
            }
        }
    }

    /// Stops a running query; `false` when it had already finished (not an error).
    pub async fn stop_query(&self, query_id: &str) -> Result<bool> {
        let op = format!("stopQuery {query_id}");
        if query_id.is_empty() {
            return Err(Error::validation(format!("{op}: query id is required")));
        }
        let sdk = self.logs(&op).await?;
        let out = self
            .run(&op, sdk.stop_query().query_id(query_id).send())
            .await?;
        Ok(out.success())
    }

    /// Recent Logs Insights queries of the account (all pages, up to `max_results`).
    pub async fn describe_queries(&self, opts: DescribeQueriesOptions) -> Result<Vec<QueryInfo>> {
        let op = "describeQueries".to_owned();
        if opts.max_results == Some(0) {
            return Ok(Vec::new());
        }
        let client = self.clone();
        let group = opts.group.clone();
        let status = opts
            .status
            .map(|s| aws_sdk_cloudwatchlogs::types::QueryStatus::from(s.as_str()));
        Paginator::new(opts.max_results, move |token, _| {
            let client = client.clone();
            let op = op.clone();
            let group = group.clone();
            let status = status.clone();
            async move {
                let sdk = client.logs(&op).await?;
                let call = sdk
                    .describe_queries()
                    .set_log_group_name(group)
                    .set_status(status)
                    .set_next_token(token)
                    .customize()
                    .interceptor(IntegerMillis)
                    .send();
                let out = client.run(&op, call).await?;
                let queries = out
                    .queries()
                    .iter()
                    .map(|q| QueryInfo {
                        query_id: q.query_id().unwrap_or_default().to_owned(),
                        query_string: q.query_string().unwrap_or_default().to_owned(),
                        status: q
                            .status()
                            .map(|s| QueryStatus::parse(s.as_str()))
                            .unwrap_or(QueryStatus::Unknown),
                        create_time: ms_time(q.create_time()),
                        log_group_name: q.log_group_name().map(str::to_owned),
                    })
                    .collect();
                Ok((queries, out.next_token().map(str::to_owned)))
            }
            .boxed()
        })
        .try_collect()
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ErrorCode;

    fn ev(ts: u64, id: &str) -> FilteredLogEvent {
        FilteredLogEvent {
            event_id: id.into(),
            log_stream_name: "s".into(),
            message: String::new(),
            timestamp: UNIX_EPOCH + Duration::from_millis(ts),
            ingestion_time: None,
        }
    }

    #[test]
    fn sorting_and_timestamps() {
        let mut v = vec![ev(2, "10"), ev(1, "b"), ev(2, "9"), ev(1, "a")];
        sort_events(&mut v, false);
        let ids: Vec<&str> = v.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids, ["a", "b", "9", "10"]);
        sort_events(&mut v, true);
        assert_eq!(v[0].event_id, "10");
        let t = parse_query_timestamp("2026-11-18 10:00:00.250").unwrap();
        assert_eq!(t.duration_since(UNIX_EPOCH).unwrap().subsec_millis(), 250);
        assert_eq!(
            parse_query_timestamp("1500").unwrap(),
            UNIX_EPOCH + Duration::from_millis(1500)
        );
        assert!(parse_query_timestamp("2026-11-18T10:00:00Z").is_ok());
        assert_eq!(
            parse_query_timestamp("nope").unwrap_err().code(),
            ErrorCode::Validation
        );
    }
}
