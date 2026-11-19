//! Bootstrap functions against recorded Moto responses.

mod support;

use std::collections::BTreeMap;

use grow_aws_cloudwatch_logs as sdk;
use sdk::{
    DeleteOptions, DescribeLogGroupsOptions, DescribeLogStreamsOptions, EnsureLogGroupOptions,
    EnsureLogStreamOptions, ErrorCode, StreamOrder,
};
use serde_json::json;
use support::*;
use wiremock::MockServer;

#[tokio::test]
async fn ensure_log_group_create_and_existing() {
    let server = MockServer::start().await;
    on_fixture(&server, "ensureLogGroup.create").await;
    on_fixture(&server, "describeLogGroups.prefix").await;
    on(&server, "PutRetentionPolicy", json_response(200, json!({}))).await;
    let c = client(&server);
    let group = fixture("describeLogGroups.prefix")["response"]["body"]["logGroups"][0]
        ["logGroupName"]
        .as_str()
        .unwrap()
        .to_owned();
    let r = c
        .ensure_log_group(
            &group,
            EnsureLogGroupOptions::default()
                .retention_days(7)
                .tag("team", "sdk"),
        )
        .await
        .unwrap();
    assert!(r.created);
    assert!(
        r.arn.starts_with("arn:aws:logs:") && !r.arn.ends_with(":*"),
        "{}",
        r.arn
    );
    let create = &bodies(&server, "CreateLogGroup").await[0];
    assert_eq!(create["logGroupName"], group);
    assert_eq!(create["tags"], json!({"team": "sdk"}));
    assert_eq!(
        bodies(&server, "PutRetentionPolicy").await[0]["retentionInDays"],
        7
    );

    // existing group: tags added, KMS checked
    let server = MockServer::start().await;
    on_fixture(&server, "ensureLogGroup.exists").await;
    on(
        &server,
        "DescribeLogGroups",
        json_response(
            200,
            json!({"logGroups": [
                {"logGroupName": "app/webx", "arn": "arn:aws:logs:us-east-1:1:log-group:app/webx:*"},
                {"logGroupName": "app/web", "arn": "arn:aws:logs:us-east-1:1:log-group:app/web:*",
                 "kmsKeyId": "arn:aws:kms:us-east-1:1:key/k1", "retentionInDays": 3, "creationTime": 5,
                 "storedBytes": 9, "metricFilterCount": 1, "logGroupClass": "STANDARD"}
            ]}),
        ),
    )
    .await;
    on(&server, "TagResource", json_response(200, json!({}))).await;
    let c = client(&server);
    let r = c
        .ensure_log_group(
            "app/web",
            EnsureLogGroupOptions::default()
                .kms_key_id("k1")
                .tag("a", "b"),
        )
        .await
        .unwrap();
    assert!(!r.created);
    assert_eq!(r.arn, "arn:aws:logs:us-east-1:1:log-group:app/web");
    assert_eq!(
        bodies(&server, "TagResource").await[0],
        json!({"resourceArn": "arn:aws:logs:us-east-1:1:log-group:app/web", "tags": {"a": "b"}})
    );
    // retention: unset leaves it unchanged, 0 removes it (never expire)
    on(
        &server,
        "DeleteRetentionPolicy",
        json_response(200, json!({})),
    )
    .await;
    assert!(bodies(&server, "PutRetentionPolicy").await.is_empty());
    assert!(bodies(&server, "DeleteRetentionPolicy").await.is_empty());
    c.ensure_log_group(
        "app/web",
        EnsureLogGroupOptions::default().retention_days(0),
    )
    .await
    .unwrap();
    assert_eq!(
        bodies(&server, "DeleteRetentionPolicy").await,
        vec![json!({"logGroupName": "app/web"})]
    );
    assert!(bodies(&server, "PutRetentionPolicy").await.is_empty());
    let err = c
        .ensure_log_group("app/web", EnsureLogGroupOptions::default().kms_key_id("k2"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Conflict);
    let g = c.get_log_group("app/web").await.unwrap().unwrap();
    assert_eq!(g.retention_days, Some(3));
    assert_eq!(g.class.as_deref(), Some("STANDARD"));
    assert_eq!(g.metric_filter_count, 1);
    assert!(g.creation_time.is_some());
    assert!(c.get_log_group("app/none").await.unwrap().is_none());

    // validation and denied
    let err = c
        .ensure_log_group("bad name!", EnsureLogGroupOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let err = c
        .ensure_log_group("g", EnsureLogGroupOptions::default().retention_days(8))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let server = MockServer::start().await;
    on_fixture(&server, "ensureLogGroup.access-denied").await;
    let err = client(&server)
        .ensure_log_group("g", EnsureLogGroupOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::PermissionDenied);
    assert_eq!(sdk::aws_error_code(&err), Some("AccessDenied"));
    // no DescribeLogGroups mock → plain HTTP 404 → NOT_FOUND → None
    assert!(client(&server).get_log_group("g").await.unwrap().is_none());
}

#[tokio::test]
async fn find_group_follows_pages() {
    let server = MockServer::start().await;
    on_times(
        &server,
        "DescribeLogGroups",
        1,
        json_response(
            200,
            json!({"logGroups": [{"logGroupName": "app/a"}], "nextToken": "t1"}),
        ),
    )
    .await;
    on(
        &server,
        "DescribeLogGroups",
        json_response(200, json!({"logGroups": [{"logGroupName": "app", "logGroupArn": "arn:x"}], "nextToken": null})),
    )
    .await;
    let g = client(&server).get_log_group("app").await.unwrap().unwrap();
    assert_eq!(g.arn, "arn:x");
    assert_eq!(
        bodies(&server, "DescribeLogGroups").await[1]["nextToken"],
        "t1"
    );
}

#[tokio::test]
async fn ensure_log_stream_cases() {
    let server = MockServer::start().await;
    on_fixture(&server, "ensureLogStream.create").await;
    let c = client(&server);
    let r = c
        .ensure_log_stream("g", "s", EnsureLogStreamOptions::default())
        .await
        .unwrap();
    assert!(r.created);
    let server = MockServer::start().await;
    on_fixture(&server, "ensureLogStream.exists").await;
    let r = client(&server)
        .ensure_log_stream("g", "s", EnsureLogStreamOptions::default())
        .await
        .unwrap();
    assert!(!r.created);
    let server = MockServer::start().await;
    on_fixture(&server, "ensureLogStream.group-not-found").await;
    let err = client(&server)
        .ensure_log_stream("g", "s", EnsureLogStreamOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
    // create_group
    let server = MockServer::start().await;
    on_fixture(&server, "ensureLogGroup.exists").await;
    on(
        &server,
        "DescribeLogGroups",
        json_response(200, json!({"logGroups": [{"logGroupName": "g"}]})),
    )
    .await;
    on_fixture(&server, "ensureLogStream.create").await;
    let r = client(&server)
        .ensure_log_stream("g", "s", EnsureLogStreamOptions { create_group: true })
        .await
        .unwrap();
    assert!(r.created);
    let c = client(&server);
    for (g, s) in [("bad name", "s"), ("g", "a:b")] {
        let err = c
            .ensure_log_stream(g, s, EnsureLogStreamOptions::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }
    let err = c
        .ensure_log_stream("g", "s", EnsureLogStreamOptions { create_group: true })
        .await;
    assert!(err.is_ok());
}

#[tokio::test]
async fn retention_and_deletes() {
    let server = MockServer::start().await;
    on_fixture(&server, "putRetentionPolicy.ok").await;
    on_fixture(&server, "putRetentionPolicy.delete").await;
    on_fixture(&server, "deleteLogGroup.ok").await;
    on_fixture(&server, "deleteLogStream.ok").await;
    let c = client(&server);
    c.put_retention_policy("g", Some(14)).await.unwrap();
    c.put_retention_policy("g", None).await.unwrap();
    c.put_retention_policy("g", Some(0)).await.unwrap();
    assert_eq!(bodies(&server, "DeleteRetentionPolicy").await.len(), 2);
    let err = c.put_retention_policy("g", Some(2)).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert_eq!(
        c.put_retention_policy("bad name", Some(1))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert!(c
        .delete_log_group("g", DeleteOptions::default())
        .await
        .unwrap());
    assert!(c
        .delete_log_stream("g", "s", DeleteOptions::default())
        .await
        .unwrap());
    assert_eq!(
        c.delete_log_group("bad name", DeleteOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.delete_log_stream("bad name", "s", DeleteOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.delete_log_stream("g", "a*", DeleteOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );

    let server = MockServer::start().await;
    on_fixture(&server, "deleteLogGroup.not-found").await;
    on_fixture(&server, "deleteLogStream.not-found").await;
    let c = client(&server);
    let ignore = DeleteOptions {
        ignore_missing: true,
    };
    assert!(!c.delete_log_group("g", ignore).await.unwrap());
    assert!(!c.delete_log_stream("g", "s", ignore).await.unwrap());
    assert_eq!(
        c.delete_log_stream("g", "s", DeleteOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn describe_iterators() {
    let server = MockServer::start().await;
    on_times(
        &server,
        "DescribeLogGroups",
        1,
        fixture_response("describeLogGroups.page1"),
    )
    .await;
    on(
        &server,
        "DescribeLogGroups",
        fixture_response("describeLogGroups.page2"),
    )
    .await;
    let c = client(&server);
    let p1 = fixture("describeLogGroups.page1")["response"]["body"]["logGroups"]
        .as_array()
        .unwrap()
        .len();
    let p2 = fixture("describeLogGroups.page2")["response"]["body"]["logGroups"]
        .as_array()
        .unwrap()
        .len();
    let groups = c
        .describe_log_groups(DescribeLogGroupsOptions {
            prefix: Some("cwlv".into()),
            page_size: Some(2),
            limit: None,
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(groups.len(), p1 + p2);
    let sent = bodies(&server, "DescribeLogGroups").await;
    assert_eq!(sent[0]["limit"], 2);
    assert_eq!(sent[0]["logGroupNamePrefix"], "cwlv");
    assert!(sent[1]["nextToken"].is_string());

    // limit, next(), next_page(), stream
    let server = MockServer::start().await;
    on(
        &server,
        "DescribeLogGroups",
        fixture_response("describeLogGroups.page1"),
    )
    .await;
    let c = client(&server);
    let mut it = c.describe_log_groups(DescribeLogGroupsOptions {
        limit: Some(1),
        ..Default::default()
    });
    assert!(format!("{it:?}").contains("Paginator"));
    assert!(it.next().await.unwrap().is_some());
    assert!(it.next().await.unwrap().is_none());
    assert!(it.next_page().await.unwrap().is_none());
    let none = c
        .describe_log_groups(DescribeLogGroupsOptions {
            limit: Some(0),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    assert!(none.is_empty());
    use futures::StreamExt;
    let streamed: Vec<_> = c
        .describe_log_groups(DescribeLogGroupsOptions {
            limit: Some(2),
            ..Default::default()
        })
        .into_stream()
        .collect()
        .await;
    assert_eq!(streamed.len(), 2);
    // validation is deferred to the first call
    let mut bad = c.describe_log_groups(DescribeLogGroupsOptions {
        page_size: Some(51),
        ..Default::default()
    });
    assert_eq!(bad.next().await.unwrap_err().code(), ErrorCode::Validation);
    assert!(bad.next().await.unwrap().is_none());
    let errs: Vec<_> = c
        .describe_log_groups(DescribeLogGroupsOptions {
            page_size: Some(0),
            ..Default::default()
        })
        .into_stream()
        .collect()
        .await;
    assert_eq!(errs.len(), 1);
    let server = MockServer::start().await;
    on_fixture(&server, "describeLogGroups.limit-too-big").await;
    let err = client(&server)
        .describe_log_groups(Default::default())
        .try_collect()
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);

    // streams
    let server = MockServer::start().await;
    on_times(
        &server,
        "DescribeLogStreams",
        1,
        fixture_response("describeLogStreams.page1"),
    )
    .await;
    on(
        &server,
        "DescribeLogStreams",
        fixture_response("describeLogStreams.page2"),
    )
    .await;
    let c = client(&server);
    let streams = c
        .describe_log_streams(
            "cwlv-0badc0de/app",
            DescribeLogStreamsOptions {
                page_size: Some(2),
                descending: true,
                order_by: StreamOrder::LastEventTime,
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert!(streams.len() >= 3);
    assert_eq!(streams[0].name, "big");
    assert!(streams[0].first_event_time.is_some() && streams[0].arn.contains("log-stream"));
    let sent = bodies(&server, "DescribeLogStreams").await;
    assert_eq!(sent[0]["orderBy"], "LastEventTime");
    assert_eq!(sent[0]["descending"], true);
    let streams = c
        .describe_log_streams(
            "g",
            DescribeLogStreamsOptions {
                prefix: Some("s".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(streams.len(), 1);
    for opts in [
        DescribeLogStreamsOptions {
            prefix: Some("p".into()),
            order_by: StreamOrder::LastEventTime,
            ..Default::default()
        },
        DescribeLogStreamsOptions {
            page_size: Some(99),
            ..Default::default()
        },
    ] {
        let err = c
            .describe_log_streams("g", opts)
            .try_collect()
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
    }
    let err = c
        .describe_log_streams("bad name", Default::default())
        .try_collect()
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let server = MockServer::start().await;
    on_fixture(&server, "describeLogStreams.not-found").await;
    let err = client(&server)
        .describe_log_streams("g", Default::default())
        .try_collect()
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn tags() {
    let server = MockServer::start().await;
    on(
        &server,
        "DescribeLogGroups",
        json_response(200, json!({"logGroups": [{"logGroupName": "g", "arn": "arn:aws:logs:us-east-1:1:log-group:g:*"}]})),
    )
    .await;
    on(&server, "TagResource", json_response(200, json!({}))).await;
    on(&server, "UntagResource", json_response(200, json!({}))).await;
    on_fixture(&server, "ensureLogGroup.list-tags").await;
    let c = client(&server);
    let tags: BTreeMap<String, String> = [("a".to_owned(), "1".to_owned())].into();
    c.tag_log_group("g", tags.clone()).await.unwrap();
    c.untag_log_group("arn:aws:logs:us-east-1:1:log-group:g:*", ["a"])
        .await
        .unwrap();
    let listed = c.list_log_group_tags("g").await.unwrap();
    assert!(!listed.is_empty());
    assert_eq!(
        bodies(&server, "TagResource").await[0]["resourceArn"],
        "arn:aws:logs:us-east-1:1:log-group:g"
    );
    assert_eq!(
        bodies(&server, "UntagResource").await[0],
        json!({"resourceArn": "arn:aws:logs:us-east-1:1:log-group:g", "tagKeys": ["a"]})
    );
    assert_eq!(
        c.tag_log_group("g", BTreeMap::new())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.untag_log_group("g", Vec::<String>::new())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.list_log_group_tags("bad name").await.unwrap_err().code(),
        ErrorCode::Validation
    );
    let server = MockServer::start().await;
    on(
        &server,
        "ListTagsForResource",
        json_response(200, json!({})),
    )
    .await;
    assert!(client(&server)
        .list_log_group_tags("arn:aws:logs:us-east-1:1:log-group:g")
        .await
        .unwrap()
        .is_empty());
}
