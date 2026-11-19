// Service-owned: replaces the sdk-kit 0.1.0 skeleton test (demo endpoints).
//! Blocking client (feature `blocking`): every method once against the mock endpoint.
#![cfg(feature = "blocking")]

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use grow_aws_cloudwatch_logs as sdk;
use sdk::blocking::CloudWatchLogsClient as Client;
use sdk::*;
use serde_json::json;
use support::*;
use wiremock::MockServer;

#[test]
fn blocking_methods() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        on_fixture(&server, "ping.ok").await;
        on_fixture(&server, "ensureLogGroup.create").await;
        on_fixture(&server, "ensureLogStream.create").await;
        on_fixture(&server, "putRetentionPolicy.ok").await;
        on_fixture(&server, "deleteLogGroup.ok").await;
        on_fixture(&server, "deleteLogStream.ok").await;
        on_fixture(&server, "describeLogStreams.all").await;
        on_fixture(&server, "putLogEvents.ok").await;
        on_fixture(&server, "filterLogEvents.term").await;
        on_fixture(&server, "getLogEvents.end-of-stream").await;
        on_fixture(&server, "startQuery.ok").await;
        on_fixture(&server, "getQueryResults.complete").await;
        on_fixture(&server, "describeQueries.ok").await;
        on_fixture(&server, "ensureLogGroup.list-tags").await;
        on(&server, "TagResource", json_response(200, json!({}))).await;
        on(&server, "UntagResource", json_response(200, json!({}))).await;
        on(
            &server,
            "StopQuery",
            json_response(200, json!({"success": false})),
        )
        .await;
        server
    });
    let c = Client::new(
        builder(&server)
            .log_group("grow-bootstrap/seed")
            .build()
            .unwrap(),
    )
    .unwrap();
    assert!(c.async_client().config().log_group().is_some());
    assert!(c.health().unwrap().healthy);
    c.ping().unwrap();
    assert_eq!(
        c.server_version().unwrap_err().code(),
        ErrorCode::Unsupported
    );
    let g = "grow-bootstrap/seed";
    assert!(c
        .ensure_log_group(g, EnsureLogGroupOptions::default())
        .is_ok());
    assert!(c.get_log_group(g).unwrap().is_some());
    assert!(
        c.ensure_log_stream(g, "s", EnsureLogStreamOptions::default())
            .unwrap()
            .created
    );
    c.put_retention_policy(g, Some(7)).unwrap();
    assert!(!c
        .describe_log_groups(DescribeLogGroupsOptions::default())
        .unwrap()
        .is_empty());
    assert!(!c
        .describe_log_streams(g, DescribeLogStreamsOptions::default())
        .unwrap()
        .is_empty());
    let tags: BTreeMap<String, String> = [("a".to_owned(), "b".to_owned())].into();
    c.tag_log_group(g, tags).unwrap();
    c.untag_log_group(g, vec!["a".into()]).unwrap();
    assert!(!c.list_log_group_tags(g).unwrap().is_empty());
    let res = c
        .put_log_events(
            g,
            "s",
            vec![LogEvent::new("x")],
            PutLogEventsOptions::default(),
        )
        .unwrap();
    assert_eq!(res.accepted, 1);
    assert!(c
        .emf("m", 1.0, &BTreeMap::new(), EmfOptions::default())
        .unwrap()
        .contains("\"m\":1"));
    assert!(!c
        .filter_log_events(FilterLogEventsOptions::default())
        .unwrap()
        .is_empty());
    assert!(c
        .filter_log_events_page(FilterLogEventsOptions::default(), None)
        .is_ok());
    assert!(c
        .get_log_events(
            g,
            "s",
            GetLogEventsOptions {
                start_from_head: true,
                ..Default::default()
            }
        )
        .unwrap()
        .is_empty());
    let id = c
        .start_query("fields @message", QueryOptions::default())
        .unwrap();
    assert_eq!(
        c.get_query_results(&id).unwrap().status,
        QueryStatus::Complete
    );
    assert_eq!(
        c.query("fields @message", QueryOptions::default())
            .unwrap()
            .status,
        QueryStatus::Complete
    );
    assert!(!c.stop_query(&id).unwrap());
    assert!(!c
        .describe_queries(DescribeQueriesOptions::default())
        .unwrap()
        .is_empty());
    assert!(c.log_group_arn(g).unwrap().ends_with(g));
    assert!(c
        .delete_log_stream(g, "s", DeleteOptions::default())
        .unwrap());
    assert!(c.delete_log_group(g, DeleteOptions::default()).unwrap());
    let q = c.block_on(async { c.async_client().log_query_client(None).unwrap() });
    assert_eq!(q.group(), g);
    c.close();
    c.close();
    assert_eq!(c.ping().unwrap_err().code(), ErrorCode::Cancelled);
    drop(rt);
}

#[test]
fn blocking_live_tail_and_from_env() {
    let mut bytes = frame("event", "sessionStart", json!({"sessionId": "b-1"}));
    bytes.extend(frame(
        "event",
        "sessionUpdate",
        json!({"sessionResults": [{"message": "m", "timestamp": 1, "logStreamName": "s"}]}),
    ));
    let fake = FakeHttp::new(vec![Reply::Stream(bytes)]);
    let c = Client::new(
        CloudWatchLogsConfig::builder()
            .sdk_client(fake.sdk_client())
            .account_id(ACCOUNT)
            .log_group("g")
            .build()
            .unwrap(),
    )
    .unwrap();
    let seen = Arc::new(std::sync::Mutex::new(0));
    let s2 = seen.clone();
    let s = c
        .start_live_tail(
            LiveTailOptions::default(),
            Arc::new(move |_| *s2.lock().unwrap() += 1),
        )
        .unwrap();
    c.block_on(s.done());
    assert_eq!(s.session_id(), "b-1");
    assert_eq!(*seen.lock().unwrap(), 1);
    drop(c);
    // from_env never panics; it fails or builds depending on the environment
    let _ = Client::from_env();
}
