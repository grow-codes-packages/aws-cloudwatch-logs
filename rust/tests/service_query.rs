//! FilterLogEvents, GetLogEvents and Logs Insights.

mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use grow_aws_cloudwatch_logs as sdk;
use sdk::{
    DescribeQueriesOptions, ErrorCode, FilterLogEventsOptions, GetLogEventsOptions, QueryOptions,
    QueryStatus,
};
use serde_json::json;
use support::*;
use wiremock::MockServer;

#[tokio::test]
async fn filter_log_events_iterates_pages() {
    let server = MockServer::start().await;
    on_times(
        &server,
        "FilterLogEvents",
        1,
        fixture_response("filterLogEvents.page1"),
    )
    .await;
    on(
        &server,
        "FilterLogEvents",
        fixture_response("filterLogEvents.page2"),
    )
    .await;
    let c = group_client(&server, "cwlv-0badc0de/app");
    let start = UNIX_EPOCH + Duration::from_millis(1000);
    let events = c
        .filter_log_events(FilterLogEventsOptions {
            streams: vec!["s1".into()],
            pattern: Some("marker".into()),
            start: Some(start),
            end: Some(SystemTime::now()),
            page_size: Some(2),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event_id, "3");
    assert_eq!(events[0].log_stream_name, "s1");
    assert!(events[0].ingestion_time.is_some());
    let sent = bodies(&server, "FilterLogEvents").await;
    assert_eq!(sent[0]["logGroupName"], "cwlv-0badc0de/app");
    assert_eq!(sent[0]["logStreamNames"], json!(["s1"]));
    assert_eq!(sent[0]["filterPattern"], "marker");
    assert_eq!(sent[0]["startTime"], 1000);
    assert_eq!(sent[0]["limit"], 2);
    assert_eq!(sent[1]["nextToken"], "cwlv-0badc0de/app@s1@4");

    // total limit shrinks the page size
    let server = MockServer::start().await;
    on(
        &server,
        "FilterLogEvents",
        fixture_response("filterLogEvents.page1"),
    )
    .await;
    let c = group_client(&server, "g");
    let events = c
        .filter_log_events(FilterLogEventsOptions {
            limit: Some(1),
            stream_prefix: Some("s".into()),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    let sent = bodies(&server, "FilterLogEvents").await;
    assert_eq!(sent[0]["limit"], 1);
    assert_eq!(sent[0]["logStreamNamePrefix"], "s");

    // bad token → empty page, end
    let server = MockServer::start().await;
    on_fixture(&server, "filterLogEvents.bad-token").await;
    let page = group_client(&server, "g")
        .filter_log_events_page(FilterLogEventsOptions::default(), Some("garbage"))
        .await
        .unwrap();
    assert!(page.events.is_empty());

    // validation
    let c = client(&server);
    let cases = [
        (FilterLogEventsOptions::default(), ErrorCode::Config),
        (
            FilterLogEventsOptions {
                group: Some("g".into()),
                streams: vec!["a".into()],
                stream_prefix: Some("b".into()),
                ..Default::default()
            },
            ErrorCode::Validation,
        ),
        (
            FilterLogEventsOptions {
                group: Some("g".into()),
                start: Some(UNIX_EPOCH + Duration::from_secs(10)),
                end: Some(UNIX_EPOCH),
                ..Default::default()
            },
            ErrorCode::Validation,
        ),
        (
            FilterLogEventsOptions {
                group: Some("g".into()),
                page_size: Some(10_001),
                ..Default::default()
            },
            ErrorCode::Validation,
        ),
        (
            FilterLogEventsOptions {
                group: Some("bad name".into()),
                ..Default::default()
            },
            ErrorCode::Validation,
        ),
    ];
    for (opts, code) in cases {
        let err = c
            .filter_log_events(opts.clone())
            .try_collect()
            .await
            .unwrap_err();
        assert_eq!(err.code(), code, "{opts:?}");
        assert_eq!(
            c.filter_log_events_page(opts, None)
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    let server = MockServer::start().await;
    on_fixture(&server, "filterLogEvents.not-found").await;
    let err = group_client(&server, "g")
        .filter_log_events(FilterLogEventsOptions::default())
        .try_collect()
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn filter_log_events_page_sorts_and_ends() {
    let server = MockServer::start().await;
    on(
        &server,
        "FilterLogEvents",
        json_response(
            200,
            json!({"events": [
                {"eventId": "10", "timestamp": 5, "message": "b", "logStreamName": "s2"},
                {"eventId": "9", "timestamp": 5, "message": "a", "logStreamName": "s1"},
                {"eventId": "1", "timestamp": 1, "message": "first", "logStreamName": "s1"}
            ], "nextToken": "tok"}),
        ),
    )
    .await;
    let c = group_client(&server, "g");
    let page = c
        .filter_log_events_page(FilterLogEventsOptions::default(), None)
        .await
        .unwrap();
    let ids: Vec<&str> = page.events.iter().map(|e| e.event_id.as_str()).collect();
    assert_eq!(ids, ["1", "9", "10"]);
    assert_eq!(page.next_token.as_deref(), Some("tok"));
    // the same token back means the end
    let page = c
        .filter_log_events_page(FilterLogEventsOptions::default(), Some("tok"))
        .await
        .unwrap();
    assert_eq!(page.next_token, None);
}

#[tokio::test]
async fn get_log_events_forward_and_backward() {
    let server = MockServer::start().await;
    on_times(
        &server,
        "GetLogEvents",
        1,
        fixture_response("getLogEvents.head"),
    )
    .await;
    on_times(
        &server,
        "GetLogEvents",
        1,
        fixture_response("getLogEvents.forward"),
    )
    .await;
    on(
        &server,
        "GetLogEvents",
        fixture_response("getLogEvents.end-of-stream"),
    )
    .await;
    let c = client(&server);
    let events = c
        .get_log_events(
            "cwlv-0badc0de/app",
            "s1",
            GetLogEventsOptions {
                start_from_head: true,
                page_size: Some(2),
                start: Some(UNIX_EPOCH + Duration::from_secs(1)),
                end: Some(SystemTime::now()),
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    let head = fixture("getLogEvents.head")["response"]["body"]["events"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(events.len(), head + 1);
    assert_eq!(events.last().unwrap().message, "plain info line");
    let sent = bodies(&server, "GetLogEvents").await;
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[0]["startFromHead"], true);
    assert_eq!(sent[0]["limit"], 2);
    assert!(sent[0]["startTime"].is_number() && sent[0]["endTime"].is_number());

    // backward (default): newest page, follows nextBackwardToken
    let server = MockServer::start().await;
    on_times(
        &server,
        "GetLogEvents",
        1,
        fixture_response("getLogEvents.tail"),
    )
    .await;
    on(
        &server,
        "GetLogEvents",
        json_response(
            200,
            json!({"events": [], "nextBackwardToken": "b/same", "nextForwardToken": "f/x"}),
        ),
    )
    .await;
    let c = client(&server);
    let events = c
        .get_log_events("g", "s", GetLogEventsOptions::default())
        .try_collect()
        .await
        .unwrap();
    assert!(!events.is_empty());
    let server = MockServer::start().await;
    on(
        &server,
        "GetLogEvents",
        fixture_response("getLogEvents.tail"),
    )
    .await;
    let c = client(&server);
    let events = c
        .get_log_events(
            "g",
            "s",
            GetLogEventsOptions {
                limit: Some(1),
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(events.len(), 1);

    for (g, s, opts) in [
        ("bad name", "s", GetLogEventsOptions::default()),
        ("g", "a*", GetLogEventsOptions::default()),
        (
            "g",
            "s",
            GetLogEventsOptions {
                page_size: Some(0),
                ..Default::default()
            },
        ),
    ] {
        let err = c
            .get_log_events(g, s, opts)
            .try_collect()
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }
    let server = MockServer::start().await;
    on_fixture(&server, "getLogEvents.stream-not-found").await;
    let err = client(&server)
        .get_log_events("g", "s", GetLogEventsOptions::default())
        .try_collect()
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn insights_start_results_stop_describe() {
    let server = MockServer::start().await;
    on_fixture(&server, "startQuery.ok").await;
    on_fixture(&server, "getQueryResults.complete").await;
    on_fixture(&server, "describeQueries.ok").await;
    on(
        &server,
        "StopQuery",
        json_response(200, json!({"success": true})),
    )
    .await;
    let c = group_client(&server, "cwlv-0badc0de/app");
    let start = UNIX_EPOCH + Duration::from_millis(10_500);
    let end = UNIX_EPOCH + Duration::from_millis(20_001);
    let id = c
        .start_query(
            "fields @message",
            QueryOptions::default()
                .range(Some(start), Some(end))
                .limit(3),
        )
        .await
        .unwrap();
    assert!(!id.is_empty());
    let sent = &bodies(&server, "StartQuery").await[0];
    assert_eq!(sent["logGroupNames"], json!(["cwlv-0badc0de/app"]));
    assert_eq!(
        (sent["startTime"].as_i64(), sent["endTime"].as_i64()),
        (Some(10), Some(21))
    );
    assert_eq!(sent["limit"], 3);
    // ARNs → identifiers
    c.start_query(
        "fields @message",
        QueryOptions::default().groups(["arn:aws:logs:us-east-1:1:log-group:g:*"]),
    )
    .await
    .unwrap();
    let sent = &bodies(&server, "StartQuery").await[1];
    assert_eq!(
        sent["logGroupIdentifiers"],
        json!(["arn:aws:logs:us-east-1:1:log-group:g"])
    );

    let res = c.get_query_results(&id).await.unwrap();
    assert_eq!(res.status, QueryStatus::Complete);
    assert_eq!(res.rows[0]["@message"], "accepted");
    assert!(sdk::parse_query_timestamp(&res.rows[0]["@timestamp"]).is_ok());
    let res = c
        .query("fields @message", QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(res.status, QueryStatus::Complete);
    assert!(c.stop_query(&id).await.unwrap());

    let qs = c
        .describe_queries(DescribeQueriesOptions {
            group: Some("cwlv-0badc0de/app".into()),
            status: Some(QueryStatus::Complete),
            max_results: None,
        })
        .await
        .unwrap();
    assert!(qs.len() >= 2);
    assert!(qs[0].create_time.is_some());
    assert_eq!(qs[0].status, QueryStatus::Complete);
    let one = c
        .describe_queries(DescribeQueriesOptions {
            max_results: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert!(c
        .describe_queries(DescribeQueriesOptions {
            max_results: Some(0),
            ..Default::default()
        })
        .await
        .unwrap()
        .is_empty());

    // validation
    let none = client(&server);
    assert_eq!(
        none.start_query("q", QueryOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Config
    );
    for opts in [
        QueryOptions::default().groups((0..51).map(|i| format!("g{i}"))),
        QueryOptions::default().groups(["g"]).limit(0),
        QueryOptions::default().groups(["bad name"]),
        QueryOptions::default()
            .groups(["g"])
            .range(Some(UNIX_EPOCH + Duration::from_secs(9)), Some(UNIX_EPOCH)),
    ] {
        assert_eq!(
            none.start_query("q", opts).await.unwrap_err().code(),
            ErrorCode::Validation
        );
    }
    assert_eq!(
        c.start_query("  ", QueryOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.query(" ", QueryOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.get_query_results("").await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.stop_query("").await.unwrap_err().code(),
        ErrorCode::Validation
    );

    // Moto quirks: StopQuery/unknown id → 500 → UNAVAILABLE
    let server = MockServer::start().await;
    on_fixture(&server, "stopQuery.moto-500").await;
    on_fixture(&server, "getQueryResults.unknown-id-moto-500").await;
    let c = CloudWatchLogsClientExt::no_retry(&server);
    let err = c.stop_query("x").await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert_eq!(
        c.get_query_results("x").await.unwrap_err().code(),
        ErrorCode::Unavailable
    );
    let server = MockServer::start().await;
    on_fixture(&server, "startQuery.not-found").await;
    let err = group_client(&server, "g")
        .start_query("q", QueryOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
}

struct CloudWatchLogsClientExt;
impl CloudWatchLogsClientExt {
    fn no_retry(server: &MockServer) -> sdk::CloudWatchLogsClient {
        sdk::CloudWatchLogsClient::new(builder(server).max_retries(0).build().unwrap()).unwrap()
    }
}

#[tokio::test]
async fn query_polls_and_handles_terminal_states() {
    let quick = |o: QueryOptions| o.poll_interval(Duration::from_millis(5));
    // Running → Complete with statistics
    let server = MockServer::start().await;
    on(
        &server,
        "StartQuery",
        json_response(200, json!({"queryId": "q1"})),
    )
    .await;
    on_times(
        &server,
        "GetQueryResults",
        1,
        json_response(200, json!({"status": "Running", "results": []})),
    )
    .await;
    on(
        &server,
        "GetQueryResults",
        json_response(
            200,
            json!({"status": "Complete", "results": [[{"field": "@message", "value": "m"}]],
            "statistics": {"recordsMatched": 1.0, "recordsScanned": 2.0, "bytesScanned": 3.0}}),
        ),
    )
    .await;
    let c = group_client(&server, "g");
    let res = c.query("q", quick(QueryOptions::default())).await.unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.statistics.unwrap().bytes_scanned, 3.0);

    for (status, code) in [
        ("Failed", ErrorCode::Unavailable),
        ("Cancelled", ErrorCode::Cancelled),
        ("Timeout", ErrorCode::Timeout),
        ("Unknown", ErrorCode::Unavailable),
    ] {
        let server = MockServer::start().await;
        on(
            &server,
            "StartQuery",
            json_response(200, json!({"queryId": "q2"})),
        )
        .await;
        on(
            &server,
            "GetQueryResults",
            json_response(200, json!({"status": status, "results": []})),
        )
        .await;
        let err = group_client(&server, "g")
            .query("q", quick(QueryOptions::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), code, "{status}");
        assert!(err.message().contains("q2"), "{err}");
    }
    // missing status → Unknown
    let server = MockServer::start().await;
    on(
        &server,
        "GetQueryResults",
        json_response(200, json!({"results": []})),
    )
    .await;
    assert_eq!(
        group_client(&server, "g")
            .get_query_results("q")
            .await
            .unwrap()
            .status,
        QueryStatus::Unknown
    );

    // deadline → best-effort StopQuery + TIMEOUT
    let server = MockServer::start().await;
    on(
        &server,
        "StartQuery",
        json_response(200, json!({"queryId": "q3"})),
    )
    .await;
    on(
        &server,
        "GetQueryResults",
        json_response(200, json!({"status": "Running", "results": []})),
    )
    .await;
    on(
        &server,
        "StopQuery",
        json_response(200, json!({"success": true})),
    )
    .await;
    let err = group_client(&server, "g")
        .query(
            "q",
            quick(QueryOptions::default()).timeout(Duration::from_millis(60)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Timeout);
    assert_eq!(bodies(&server, "StopQuery").await[0]["queryId"], "q3");

    // timeout before a query id exists
    let server = MockServer::start().await;
    on(
        &server,
        "StartQuery",
        json_response(200, json!({"queryId": "q4"})).set_delay(Duration::from_millis(300)),
    )
    .await;
    let err = group_client(&server, "g")
        .query(
            "q",
            QueryOptions::default().timeout(Duration::from_millis(30)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Timeout);

    // LimitExceededException is retried, other errors are not
    let server = MockServer::start().await;
    on_times(
        &server,
        "StartQuery",
        1,
        aws_error(400, "LimitExceededException", "too many"),
    )
    .await;
    on(
        &server,
        "StartQuery",
        json_response(200, json!({"queryId": "q5"})),
    )
    .await;
    on(
        &server,
        "GetQueryResults",
        json_response(200, json!({"status": "Complete", "results": []})),
    )
    .await;
    let res = group_client(&server, "g")
        .query("q", quick(QueryOptions::default()))
        .await
        .unwrap();
    assert_eq!(res.query_id, "q5");
    let server = MockServer::start().await;
    on(
        &server,
        "StartQuery",
        aws_error(400, "MalformedQueryException", "bad"),
    )
    .await;
    let err = group_client(&server, "g")
        .query("q", quick(QueryOptions::default()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let server = MockServer::start().await;
    on(
        &server,
        "StartQuery",
        json_response(200, json!({"queryId": "q6"})),
    )
    .await;
    on(
        &server,
        "GetQueryResults",
        aws_error(400, "ResourceNotFoundException", "gone"),
    )
    .await;
    let err = group_client(&server, "g")
        .query(
            "q",
            QueryOptions {
                timeout: Duration::ZERO,
                poll_interval: Duration::ZERO,
                ..QueryOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
}
