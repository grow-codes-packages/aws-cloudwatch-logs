//! Logging category: exporter, batching log client, search / list / wait.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use grow_aws_cloudwatch_logs as sdk;
use sdk::logging::{
    attrs, ExportOptions, LogClient, LogExporter, LogInput, LogQueryClient, LogRecord,
    SearchOptions, Severity, SortOrder, WaitOptions,
};
use sdk::{
    CloudWatchLogsClient, ErrorCode, ExporterOptions, LogClientOptions, OversizePolicy,
    ATTR_EVENT_ID, ATTR_LOG_STREAM,
};
use serde_json::{json, Value};
use support::*;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Emulates FilterLogEvents over a fixed event list (time range, limit, token = offset).
struct FakeFilter {
    events: Vec<(i64, String, String)>, // (timestamp ms, stream, message)
}

impl Respond for FakeFilter {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let start = body["startTime"].as_i64().unwrap_or(i64::MIN);
        let end = body["endTime"].as_i64().unwrap_or(i64::MAX);
        let pattern = body["filterPattern"]
            .as_str()
            .unwrap_or("")
            .trim_matches('"')
            .to_owned();
        let limit = body["limit"].as_u64().unwrap_or(10_000) as usize;
        let offset: usize = body["nextToken"]
            .as_str()
            .map(|t| t.parse().unwrap())
            .unwrap_or(0);
        let matching: Vec<Value> = self
            .events
            .iter()
            .enumerate()
            .filter(|(_, (ts, _, msg))| *ts >= start && *ts <= end && msg.contains(&pattern))
            .map(|(i, (ts, stream, msg))| {
                json!({"eventId": format!("{i}"), "timestamp": ts, "ingestionTime": ts + 1,
                       "logStreamName": stream, "message": msg})
            })
            .collect();
        let page: Vec<Value> = matching.iter().skip(offset).take(limit).cloned().collect();
        let next =
            (offset + page.len() < matching.len()).then(|| (offset + page.len()).to_string());
        json_response(200, json!({"events": page, "nextToken": next}))
    }
}

async fn fake_filter(server: &MockServer, events: Vec<(i64, String, String)>) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Logs_20140328.FilterLogEvents"))
        .respond_with(FakeFilter { events })
        .mount(server)
        .await;
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test]
async fn exporter_writes_normalized_json() {
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.ok").await;
    let c = group_client(&server, "app/web");
    let exp = c.exporter(ExporterOptions::default()).unwrap();
    assert_eq!((exp.group(), exp.stream()), ("app/web", "s1"));
    let rec = LogRecord::new(Severity::Error, "exported").with_attribute("k", "v");
    let res = exp
        .export(std::slice::from_ref(&rec), ExportOptions::new(true))
        .await
        .unwrap();
    assert_eq!((res.accepted, res.rejected), (1, 0));
    assert!(res.error.is_none());
    let sent = &bodies(&server, "PutLogEvents").await[0];
    let msg = sent["logEvents"][0]["message"].as_str().unwrap();
    let back = LogRecord::from_json(msg).unwrap();
    assert_eq!(
        (back.severity, back.body.as_str()),
        (Severity::Error, "exported")
    );
    exp.shutdown().await.unwrap();

    // oversize records: drop policy skips, truncate shortens the body
    let c2 = CloudWatchLogsClient::new(
        builder(&server)
            .log_group("app/web")
            .max_event_bytes(400)
            .oversize_policy(OversizePolicy::Drop)
            .build()
            .unwrap(),
    )
    .unwrap();
    let exp = c2
        .exporter(ExporterOptions {
            stream: Some("other".into()),
            ..Default::default()
        })
        .unwrap();
    let big = LogRecord::new(Severity::Info, "z".repeat(1000));
    let res = exp
        .export(&[big.clone(), rec.clone()], ExportOptions::default())
        .await
        .unwrap();
    assert_eq!((res.accepted, res.rejected), (1, 1));
    assert_eq!(res.error.unwrap().code(), ErrorCode::Validation);

    // rejected by the service
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.rejected-too-old-too-new").await;
    let exp = group_client(&server, "g")
        .exporter(ExporterOptions::default())
        .unwrap();
    let res = exp
        .export(
            &[rec.clone(), rec.clone(), rec.clone()],
            ExportOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!((res.accepted, res.rejected), (2, 1));
    // whole batch failed
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.access-denied").await;
    let exp = group_client(&server, "g")
        .exporter(ExporterOptions::default())
        .unwrap();
    let err = exp
        .export(&[rec], ExportOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::PermissionDenied);

    // config errors
    let c = client(&server);
    assert_eq!(
        c.exporter(ExporterOptions::default()).unwrap_err().code(),
        ErrorCode::Config
    );
    let err = c
        .exporter(ExporterOptions {
            group: Some("g".into()),
            stream: Some("a:b".into()),
            create_group: true,
        })
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert_eq!(
        c.log_query_client(None).unwrap_err().code(),
        ErrorCode::Config
    );
    assert_eq!(
        c.log_query_client(Some("bad name")).unwrap_err().code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn log_client_batches_redacts_and_closes_with_the_client() {
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.ok").await;
    let c = group_client(&server, "app/web");
    let logs = c
        .log_client(LogClientOptions {
            resource: attrs! { "service.name" => "svc" },
            ..Default::default()
        })
        .unwrap();
    let logger = logs.logger(Some("it"), attrs! { "run" => "r1" });
    logger.info("hello", attrs! { "password" => "hunter2-secret" });
    logs.send(LogInput::new("second").severity(Severity::Warn).into());
    logs.flush(None).await.unwrap();
    let st = logs.stats();
    assert_eq!((st.sent, st.failed), (2, 0));
    let sent = bodies(&server, "PutLogEvents").await;
    let all: String = sent.iter().map(|b| b.to_string()).collect();
    assert!(!all.contains("hunter2-secret"));
    assert!(all.contains("svc"));
    // closing the service client flushes and closes the log client
    logs.send(LogInput::new("third").into());
    c.close().await;
    assert_eq!(logs.stats().sent, 3);
    assert_eq!(
        c.log_client(LogClientOptions::default())
            .unwrap_err()
            .code(),
        ErrorCode::Cancelled
    );
}

#[tokio::test]
async fn search_ascending_with_cursor() {
    let server = MockServer::start().await;
    let base = now_ms() - 60_000;
    let rec = LogRecord::new(Severity::Warn, "structured marker").to_json();
    fake_filter(
        &server,
        vec![
            (base, "s1".into(), "one marker".into()),
            (base + 1, "s2".into(), rec),
            (base + 2, "s1".into(), "three marker".into()),
            (base + 3, "s1".into(), "unrelated".into()),
        ],
    )
    .await;
    let c = group_client(&server, "app/web");
    let q = c.log_query_client(None).unwrap();
    assert_eq!(q.group(), "app/web");
    let page = q
        .search(
            "marker",
            SearchOptions::default().order(SortOrder::Asc).limit(2),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].body, "one marker");
    assert_eq!(page.records[0].attributes[ATTR_LOG_STREAM], "s1");
    assert_eq!(page.records[0].attributes[ATTR_EVENT_ID], "0");
    assert_eq!(page.records[1].severity, Severity::Warn);
    let cursor = page.next_cursor.clone().expect("more");
    let page = q
        .search(
            "marker",
            SearchOptions::default()
                .order(SortOrder::Asc)
                .limit(2)
                .cursor(cursor),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(page.next_cursor.is_none());
    assert_eq!(
        bodies(&server, "FilterLogEvents").await[0]["filterPattern"],
        "\"marker\""
    );
}

#[tokio::test]
async fn search_descending_walks_slices() {
    let server = MockServer::start().await;
    let now = now_ms();
    // events spread over ~2 days: several slices, two share a timestamp
    let mut events: Vec<(i64, String, String)> = Vec::new();
    for (i, age) in [10_i64, 60_000, 60_000, 3_600_000, 40 * 3_600_000]
        .iter()
        .enumerate()
    {
        events.push((now - age, "s1".into(), format!("m{i} marker")));
    }
    events.push((now - 5, "s1".into(), "noise".into()));
    fake_filter(&server, events).await;
    on(
        &server,
        "DescribeLogGroups",
        json_response(
            200,
            json!({"logGroups": [{"logGroupName": "g", "creationTime": now - 50 * 3_600_000}]}),
        ),
    )
    .await;
    let c = group_client(&server, "g");
    let q = c.log_query_client(Some("g")).unwrap();
    let p1 = q
        .search("marker", SearchOptions::default().limit(2))
        .await
        .unwrap();
    let bodies1: Vec<&str> = p1.records.iter().map(|r| r.body.as_str()).collect();
    assert_eq!(bodies1[0], "m0 marker");
    assert_eq!(p1.records.len(), 2);
    let cur = p1.next_cursor.clone().expect("cursor");
    assert!(cur.starts_with("d1."));
    let p2 = q
        .search("marker", SearchOptions::default().limit(2).cursor(cur))
        .await
        .unwrap();
    let p3 = q
        .search(
            "marker",
            SearchOptions::default()
                .limit(2)
                .cursor(p2.next_cursor.clone().unwrap()),
        )
        .await
        .unwrap();
    let mut all: Vec<String> = p1
        .records
        .iter()
        .chain(&p2.records)
        .chain(&p3.records)
        .map(|r| r.body.clone())
        .collect();
    assert_eq!(all.len(), 5, "{all:?}");
    assert!(p3.next_cursor.is_none());
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 5);
    // newest first across pages
    let ts: Vec<SystemTime> = p1
        .records
        .iter()
        .chain(&p2.records)
        .chain(&p3.records)
        .map(|r| r.timestamp)
        .collect();
    assert!(ts.windows(2).all(|w| w[0] >= w[1]));

    // explicit start, invalid cursor, bad range, limit 0
    let page = q
        .search(
            "marker",
            SearchOptions::default().range(
                Some(UNIX_EPOCH + Duration::from_millis((now - 120_000) as u64)),
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 3);
    assert_eq!(
        q.search("x", SearchOptions::default().cursor("bogus"))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        q.search(
            "x",
            SearchOptions::default().range(Some(SystemTime::now()), Some(UNIX_EPOCH))
        )
        .await
        .unwrap_err()
        .code(),
        ErrorCode::Validation
    );
    assert!(q
        .search("x", SearchOptions::default().limit(0))
        .await
        .unwrap()
        .records
        .is_empty());

    // group lookup: missing group falls back to a 30-day window; other errors surface
    let server = MockServer::start().await;
    fake_filter(&server, vec![(now - 1000, "s".into(), "x marker".into())]).await;
    on(
        &server,
        "DescribeLogGroups",
        json_response(200, json!({"logGroups": []})),
    )
    .await;
    let q = group_client(&server, "g").log_query_client(None).unwrap();
    assert_eq!(
        q.search("marker", SearchOptions::default())
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    let server = MockServer::start().await;
    on(
        &server,
        "DescribeLogGroups",
        aws_error(400, "AccessDeniedException", "no"),
    )
    .await;
    let q = group_client(&server, "g").log_query_client(None).unwrap();
    assert_eq!(
        q.search("marker", SearchOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::PermissionDenied
    );
}

#[tokio::test]
async fn search_desc_narrows_dense_slices() {
    let server = MockServer::start().await;
    let now = now_ms();
    // 10,050 events within the newest 5-minute slice → the slice is narrowed
    let events: Vec<(i64, String, String)> = (0..10_050)
        .map(|i| {
            (
                now - 1000 - (i as i64 % 250_000),
                "s".to_owned(),
                format!("e{i}"),
            )
        })
        .collect();
    fake_filter(&server, events).await;
    let q = group_client(&server, "g").log_query_client(None).unwrap();
    let page = q
        .search(
            "",
            SearchOptions::default().limit(5).range(
                Some(UNIX_EPOCH + Duration::from_millis((now - 600_000) as u64)),
                Some(SystemTime::now()),
            ),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 5);
    assert!(page.next_cursor.is_some());
}

#[tokio::test]
async fn streams_labels_and_wait() {
    let server = MockServer::start().await;
    on_fixture(&server, "describeLogStreams.all").await;
    fake_filter(
        &server,
        vec![(now_ms() - 10, "s1".into(), "ready marker".into())],
    )
    .await;
    let c = group_client(&server, "g");
    let q = c.log_query_client(None).unwrap();
    let streams = q.list_streams().await.unwrap();
    assert!(!streams.is_empty());
    assert_eq!(
        q.list_labels().await.unwrap_err().code(),
        ErrorCode::Unsupported
    );
    let rec = q
        .wait_for_log(
            "ready",
            WaitOptions::default()
                .timeout(Duration::from_secs(5))
                .interval(Duration::from_millis(10)),
        )
        .await
        .unwrap();
    assert_eq!(rec.body, "ready marker");
    let err = q
        .wait_for_log(
            "absent",
            WaitOptions::default()
                .timeout(Duration::from_millis(50))
                .interval(Duration::from_millis(10)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Timeout);
    let _ = Arc::new(Mutex::new(()));
}
