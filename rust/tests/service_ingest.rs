//! put_log_events batching, auto-create, rejections and EMF.

mod support;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use grow_aws_cloudwatch_logs as sdk;
use sdk::{
    CloudWatchLogsClient, EmfOptions, ErrorCode, LogEvent, OversizePolicy, PutLogEventsOptions,
};
use serde_json::{json, Value};
use support::*;
use wiremock::MockServer;

fn at(ms: u64, msg: &str) -> LogEvent {
    LogEvent::at(UNIX_EPOCH + Duration::from_millis(ms), msg)
}

#[tokio::test]
async fn put_sorted_and_rejections() {
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.rejected-too-old-too-new").await;
    let c = client(&server);
    let res = c
        .put_log_events(
            "g",
            "s",
            vec![at(3, "too new"), at(1, "too old"), at(2, "accepted")],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap();
    // exclusive end index (same rule as Go/TS): tooOld 0 rejects nothing, tooNew 2 rejects one
    assert_eq!((res.accepted, res.rejected, res.batches), (2, 1, 1));
    assert_eq!(res.rejected_info[0].too_old_log_event_end_index, Some(0));
    let sent = &bodies(&server, "PutLogEvents").await[0];
    let msgs: Vec<&str> = sent["logEvents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["message"].as_str().unwrap())
        .collect();
    assert_eq!(msgs, ["too old", "accepted", "too new"]);
    assert!(sent.get("sequenceToken").is_none());

    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.ok").await;
    let c = client(&server);
    let res = c
        .put_log_events(
            "g",
            "s",
            vec![LogEvent::new("x")],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!((res.accepted, res.rejected), (1, 0));
    // empty input sends nothing
    let res = c
        .put_log_events("g", "s", vec![], PutLogEventsOptions::default())
        .await
        .unwrap();
    assert_eq!(res.batches, 0);
}

#[tokio::test]
async fn split_into_batches_and_oversize() {
    let server = MockServer::start().await;
    on(
        &server,
        "PutLogEvents",
        json_response(200, json!({"nextSequenceToken": "1"})),
    )
    .await;
    let c = client(&server);
    let many: Vec<LogEvent> = (0..10_001).map(|i| at(i, "x")).collect();
    let res = c
        .put_log_events("g", "s", many, PutLogEventsOptions::default())
        .await
        .unwrap();
    assert_eq!((res.batches, res.accepted), (2, 10_001));
    let big = "y".repeat(300_000);
    let res = c
        .put_log_events("g", "s", vec![at(1, &big)], PutLogEventsOptions::default())
        .await
        .unwrap();
    assert_eq!(res.truncated, 1);
    let sent = bodies(&server, "PutLogEvents").await;
    let msg = sent.last().unwrap()["logEvents"][0]["message"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(msg.len() <= sdk::DEFAULT_MAX_EVENT_BYTES && msg.contains("truncated"));
    let res = c
        .put_log_events(
            "g",
            "s",
            vec![at(1, &big), at(2, "ok")],
            PutLogEventsOptions {
                oversize_policy: Some(OversizePolicy::Drop),
                max_event_bytes: Some(1000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!((res.dropped, res.accepted), (1, 1));
    let err = c
        .put_log_events(
            "g",
            "s",
            vec![at(1, &big)],
            PutLogEventsOptions {
                oversize_policy: Some(OversizePolicy::Error),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    for bad in [0, sdk::MAX_EVENT_BYTES_LIMIT + 1] {
        let err = c
            .put_log_events(
                "g",
                "s",
                vec![],
                PutLogEventsOptions {
                    max_event_bytes: Some(bad),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }
    for (g, s) in [("bad name", "s"), ("g", "a:b")] {
        let err = c
            .put_log_events(g, s, vec![], PutLogEventsOptions::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
async fn auto_create_stream() {
    let server = MockServer::start().await;
    on_times(
        &server,
        "PutLogEvents",
        1,
        fixture_response("putLogEvents.stream-not-found"),
    )
    .await;
    on_fixture(&server, "putLogEvents.ok").await;
    on_fixture(&server, "ensureLogStream.create").await;
    let c = client(&server);
    let res = c
        .put_log_events(
            "g",
            "new",
            vec![LogEvent::new("x")],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(res.accepted, 1);
    assert_eq!(bodies(&server, "CreateLogStream").await.len(), 1);
    assert_eq!(bodies(&server, "PutLogEvents").await.len(), 2);

    // create_group: the group is created first
    let server = MockServer::start().await;
    on_times(
        &server,
        "PutLogEvents",
        1,
        fixture_response("putLogEvents.group-not-found"),
    )
    .await;
    on_fixture(&server, "putLogEvents.ok").await;
    on_fixture(&server, "ensureLogGroup.create").await;
    on(
        &server,
        "DescribeLogGroups",
        json_response(200, json!({"logGroups": [{"logGroupName": "g"}]})),
    )
    .await;
    on_fixture(&server, "ensureLogStream.create").await;
    let res = client(&server)
        .put_log_events(
            "g",
            "new",
            vec![LogEvent::new("x")],
            PutLogEventsOptions {
                create_group: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(res.accepted, 1);
    assert_eq!(bodies(&server, "CreateLogGroup").await.len(), 1);

    // auto-create off, or the group is missing
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.stream-not-found").await;
    on_fixture(&server, "ensureLogStream.group-not-found").await;
    let c =
        CloudWatchLogsClient::new(builder(&server).auto_create(false).build().unwrap()).unwrap();
    let err = c
        .put_log_events(
            "g",
            "s",
            vec![LogEvent::new("x")],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
    let err = client(&server)
        .put_log_events(
            "g",
            "s",
            vec![LogEvent::new("x")],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.access-denied").await;
    let err = client(&server)
        .put_log_events(
            "g",
            "s",
            vec![LogEvent::new("x")],
            PutLogEventsOptions {
                auto_create: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::PermissionDenied);
}

#[tokio::test]
async fn emf_writes_a_document() {
    let server = MockServer::start().await;
    on_fixture(&server, "putLogEvents.ok").await;
    let c = group_client(&server, "metrics/app");
    let dims: BTreeMap<String, String> = [("service".to_owned(), "api".to_owned())].into();
    let res = c
        .emf(
            "latency",
            42.0,
            &dims,
            EmfOptions::default()
                .namespace("GrowSdkIT")
                .unit("Milliseconds"),
        )
        .await
        .unwrap();
    assert_eq!(res.accepted, 1);
    let sent = &bodies(&server, "PutLogEvents").await[0];
    assert_eq!(sent["logGroupName"], "metrics/app");
    assert_eq!(sent["logStreamName"], "s1");
    let doc: Value =
        serde_json::from_str(sent["logEvents"][0]["message"].as_str().unwrap()).unwrap();
    assert_eq!(doc["latency"], 42);
    assert_eq!(
        doc["_aws"]["CloudWatchMetrics"][0]["Namespace"],
        "GrowSdkIT"
    );
    assert_eq!(doc["_aws"]["Timestamp"], sent["logEvents"][0]["timestamp"]);
    // explicit group/stream/timestamp
    let opts = EmfOptions {
        group: Some("other".into()),
        stream: Some("emf".into()),
        timestamp: Some(SystemTime::now()),
        ..Default::default()
    };
    c.emf("m", 1.0, &BTreeMap::new(), opts).await.unwrap();
    assert_eq!(
        bodies(&server, "PutLogEvents").await[1]["logGroupName"],
        "other"
    );
    // no group configured
    let err = client(&server)
        .emf("m", 1.0, &BTreeMap::new(), EmfOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Config);
    let err = c
        .emf("", 1.0, &BTreeMap::new(), EmfOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
}
