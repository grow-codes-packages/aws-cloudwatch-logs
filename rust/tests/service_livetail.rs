//! Live tail over a fake event-stream HTTP client (real-AWS and Moto frame shapes).

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use grow_aws_cloudwatch_logs as sdk;
use sdk::logging::{LogQueryClient, TailOptions};
use sdk::{
    CloudWatchLogsClient, CloudWatchLogsConfig, ErrorCode, LiveTailEvent, LiveTailOptions,
    LiveTailUpdate,
};
use serde_json::{json, Value};
use support::*;

fn tail_client(fake: &FakeHttp, group: Option<&str>) -> CloudWatchLogsClient {
    let mut b = CloudWatchLogsConfig::builder()
        .sdk_client(fake.sdk_client())
        .account_id(ACCOUNT)
        .total_timeout(Duration::from_secs(2));
    if let Some(g) = group {
        b = b.log_group(g);
    }
    CloudWatchLogsClient::new(b.build().unwrap()).unwrap()
}

fn start() -> Vec<u8> {
    let mut v = frame("event", "initial-response", Value::Null);
    v.extend(frame(
        "event",
        "sessionStart",
        json!({"sessionId": "sess-1", "requestId": "r"}),
    ));
    v
}

fn update(messages: &[&str], sampled: bool) -> Vec<u8> {
    let results: Vec<Value> = messages
        .iter()
        .map(|m| {
            json!({"logStreamName": "t1", "logGroupIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:g",
                   "message": m, "timestamp": 1_795_034_969_000_i64, "ingestionTime": 1_795_034_972_331_i64})
        })
        .collect();
    frame(
        "event",
        "sessionUpdate",
        json!({"sessionMetadata": {"sampled": sampled}, "sessionResults": results}),
    )
}

type Seen = Arc<Mutex<Vec<LiveTailEvent>>>;

fn collector() -> (Seen, sdk::LiveTailCallback) {
    let seen: Seen = Arc::default();
    let s2 = seen.clone();
    (seen, Arc::new(move |e| s2.lock().unwrap().push(e)))
}

async fn wait_done(s: &sdk::LiveTailSession) {
    tokio::time::timeout(Duration::from_secs(5), s.done())
        .await
        .expect("session ends");
}

#[tokio::test]
async fn recorded_moto_stream() {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(
            fixture("startLiveTail.seed")["response"]["bodyBase64"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    let fake = FakeHttp::new(vec![Reply::Stream(raw)]);
    let c = tail_client(&fake, Some("grow-bootstrap/seed"));
    let (seen, cb) = collector();
    let updates: Arc<Mutex<Vec<LiveTailUpdate>>> = Arc::default();
    let u2 = updates.clone();
    let s = c
        .start_live_tail(
            LiveTailOptions::default().on_update(move |u| u2.lock().unwrap().push(u)),
            cb,
        )
        .await
        .unwrap();
    assert_eq!(s.session_id(), "dab2e97c-0f97-49b7-bc9d-d7a93d6341e3");
    wait_done(&s).await;
    assert!(s.is_done());
    assert!(s.error().is_none());
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert!(seen[0].message.contains("grow-seed-event 1"));
    assert_eq!(seen[0].log_stream_name, "seed-stream");
    assert_eq!(updates.lock().unwrap()[0].count, 3);
    let (uri, body) = fake.requests.lock().unwrap()[0].clone();
    assert!(uri.starts_with("http://streaming-localhost:1"), "{uri}");
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        body["logGroupIdentifiers"],
        json!(["arn:aws:logs:us-east-1:123456789012:log-group:grow-bootstrap/seed"])
    );
    assert!(format!("{s:?}").contains("LiveTailSession"));
}

#[tokio::test]
async fn aws_style_session_close_and_exceptions() {
    // open stream: updates (one empty keep-alive), then the caller closes it
    let mut bytes = start();
    bytes.extend(update(&["one", "two"], true));
    bytes.extend(update(&[], false));
    let fake = FakeHttp::new(vec![Reply::Open(bytes)]);
    let c = tail_client(&fake, None);
    let (seen, cb) = collector();
    let updates: Arc<Mutex<Vec<LiveTailUpdate>>> = Arc::default();
    let u2 = updates.clone();
    let s = c
        .start_live_tail(
            LiveTailOptions {
                groups: vec!["g".into()],
                streams: vec!["t1".into()],
                pattern: Some("one".into()),
                on_update: Some(Arc::new(move |u| u2.lock().unwrap().push(u))),
                ..Default::default()
            },
            cb,
        )
        .await
        .unwrap();
    assert_eq!(s.session_id(), "sess-1");
    for _ in 0..100 {
        if updates.lock().unwrap().len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!s.is_done());
    s.close();
    s.close();
    wait_done(&s).await;
    assert!(s.error().is_none());
    assert_eq!(seen.lock().unwrap().len(), 2);
    assert!(seen.lock().unwrap()[0].sampled);
    assert_eq!(
        *updates.lock().unwrap(),
        [
            LiveTailUpdate {
                sampled: true,
                count: 2
            },
            LiveTailUpdate {
                sampled: false,
                count: 0
            }
        ]
    );
    let body: Value = serde_json::from_str(&fake.requests.lock().unwrap()[0].1).unwrap();
    assert_eq!(body["logStreamNames"], json!(["t1"]));
    assert_eq!(body["logEventFilterPattern"], "one");

    // SessionStreamingException / SessionTimeoutException end the session with an error
    for (exception, code) in [
        ("SessionStreamingException", ErrorCode::Unavailable),
        ("SessionTimeoutException", ErrorCode::Timeout),
    ] {
        let mut bytes = start();
        bytes.extend(update(&["x"], false));
        bytes.extend(frame(
            "exception",
            exception,
            json!({"message": "stream broke"}),
        ));
        let fake = FakeHttp::new(vec![Reply::Stream(bytes)]);
        let c = tail_client(&fake, Some("g"));
        let errors: Arc<Mutex<Vec<ErrorCode>>> = Arc::default();
        let e2 = errors.clone();
        let (seen, cb) = collector();
        let s = c
            .start_live_tail(
                LiveTailOptions::default().on_error(move |e| e2.lock().unwrap().push(e.code())),
                cb,
            )
            .await
            .unwrap();
        wait_done(&s).await;
        let err = s.error().expect("session error");
        assert_eq!(err.code(), code);
        assert!(err.is_retryable());
        assert_eq!(sdk::aws_error_code(&err), Some(exception));
        assert_eq!(*errors.lock().unwrap(), [code]);
        assert_eq!(seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn first_frame_variants_and_failures() {
    // an update before sessionStart is delivered; session id stays empty
    let mut bytes = update(&["early"], false);
    bytes.extend(update(&["late"], false));
    let fake = FakeHttp::new(vec![Reply::Stream(bytes)]);
    let c = tail_client(&fake, Some("g"));
    let (seen, cb) = collector();
    let s = c
        .start_live_tail(LiveTailOptions::default(), cb)
        .await
        .unwrap();
    wait_done(&s).await;
    assert_eq!(s.session_id(), "");
    assert_eq!(seen.lock().unwrap().len(), 2);

    // the stream ends before the session started
    let fake = FakeHttp::new(vec![Reply::Stream(frame(
        "event",
        "initial-response",
        Value::Null,
    ))]);
    let (_, cb) = collector();
    let err = tail_client(&fake, Some("g"))
        .start_live_tail(LiveTailOptions::default(), cb)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);

    // an exception as the first frame
    let fake = FakeHttp::new(vec![Reply::Stream(frame(
        "exception",
        "SessionStreamingException",
        json!({"message": "no"}),
    ))]);
    let (_, cb) = collector();
    let err = tail_client(&fake, Some("g"))
        .start_live_tail(LiveTailOptions::default(), cb)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);

    // a broken frame
    let fake = FakeHttp::new(vec![Reply::Stream(
        b"\x00\x00\x00\x10garbage-bytes!!".to_vec(),
    )]);
    let (_, cb) = collector();
    let err = tail_client(&fake, Some("g"))
        .start_live_tail(LiveTailOptions::default(), cb)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);

    // HTTP error from StartLiveTail
    let fake = FakeHttp::new(vec![Reply::Status(
        400,
        r#"{"__type":"ResourceNotFoundException","message":"missing"}"#.into(),
    )]);
    let (_, cb) = collector();
    let err = tail_client(&fake, Some("g"))
        .start_live_tail(LiveTailOptions::default(), cb)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);

    // no sessionStart in time
    let fake = FakeHttp::new(vec![Reply::Open(frame(
        "event",
        "initial-response",
        Value::Null,
    ))]);
    let c = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .sdk_client(fake.sdk_client())
            .account_id(ACCOUNT)
            .log_group("g")
            .total_timeout(Duration::from_millis(100))
            .build()
            .unwrap(),
    )
    .unwrap();
    let (_, cb) = collector();
    let err = c
        .start_live_tail(LiveTailOptions::default(), cb)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Timeout);
}

#[tokio::test]
async fn validation_and_client_close() {
    let fake = FakeHttp::new(vec![]);
    let c = tail_client(&fake, None);
    let (_, cb) = collector();
    let cases = [
        (LiveTailOptions::default(), ErrorCode::Config),
        (
            LiveTailOptions::default().groups((0..11).map(|i| format!("g{i}"))),
            ErrorCode::Validation,
        ),
        (
            LiveTailOptions {
                groups: vec!["g".into()],
                streams: vec!["a".into()],
                stream_prefixes: vec!["b".into()],
                ..Default::default()
            },
            ErrorCode::Validation,
        ),
        (
            LiveTailOptions {
                groups: vec!["g".into(), "h".into()],
                stream_prefixes: vec!["b".into()],
                ..Default::default()
            },
            ErrorCode::Validation,
        ),
        (
            LiveTailOptions::default().groups(["bad name"]),
            ErrorCode::Validation,
        ),
    ];
    for (opts, code) in cases {
        let err = c
            .start_live_tail(opts.clone(), cb.clone())
            .await
            .unwrap_err();
        assert_eq!(err.code(), code, "{opts:?}");
    }

    // closing the client stops running sessions; later starts are CANCELLED
    let mut bytes = start();
    bytes.extend(update(&["a"], false));
    let fake = FakeHttp::new(vec![Reply::Open(bytes)]);
    let c = tail_client(&fake, Some("g"));
    let (_, cb) = collector();
    let s = c
        .start_live_tail(
            LiveTailOptions {
                stream_prefixes: vec!["t".into()],
                groups: vec!["arn:aws:logs:us-east-1:123456789012:log-group:g:*".into()],
                ..Default::default()
            },
            cb.clone(),
        )
        .await
        .unwrap();
    c.close().await;
    wait_done(&s).await;
    assert!(s.error().is_none());
    let err = c
        .start_live_tail(LiveTailOptions::default(), cb)
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Cancelled);
}

#[tokio::test]
async fn tail_through_the_query_client() {
    let mut bytes = start();
    bytes.extend(update(&["plain line"], false));
    let fake = FakeHttp::new(vec![Reply::Stream(bytes)]);
    let c = tail_client(&fake, Some("g"));
    let q = c.log_query_client(None).unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let r2 = records.clone();
    let handle = q
        .tail(
            "plain",
            Arc::new(move |r| r2.lock().unwrap().push(r)),
            TailOptions::default(),
        )
        .await
        .unwrap();
    // the service closes the stream → the handle stops by itself
    tokio::time::timeout(Duration::from_secs(5), handle.stopped())
        .await
        .unwrap();
    let records = records.lock().unwrap().clone();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, "plain line");
    assert_eq!(records[0].attributes[sdk::ATTR_LOG_STREAM], "t1");
    let body: Value = serde_json::from_str(&fake.requests.lock().unwrap()[0].1).unwrap();
    assert_eq!(body["logEventFilterPattern"], "\"plain\"");

    // stopping the handle closes the session
    let fake = FakeHttp::new(vec![Reply::Open(start())]);
    let c = tail_client(&fake, Some("g"));
    let handle = c
        .log_query_client(None)
        .unwrap()
        .tail("", Arc::new(|_| {}), TailOptions::default())
        .await
        .unwrap();
    handle.stop();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let body: Value = serde_json::from_str(&fake.requests.lock().unwrap()[0].1).unwrap();
    assert!(body.get("logEventFilterPattern").is_none());
}

#[tokio::test]
async fn panicking_callbacks_do_not_kill_the_session() {
    let mut bytes = start();
    bytes.extend(update(&["a", "b"], false));
    let fake = FakeHttp::new(vec![Reply::Stream(bytes)]);
    let c = tail_client(&fake, Some("g"));
    let count = Arc::new(Mutex::new(0));
    let c2 = count.clone();
    let s = c
        .start_live_tail(
            LiveTailOptions::default().on_update(|_| panic!("update callback")),
            Arc::new(move |_| {
                *c2.lock().unwrap() += 1;
                panic!("event callback");
            }),
        )
        .await
        .unwrap();
    wait_done(&s).await;
    assert_eq!(*count.lock().unwrap(), 2);
}
