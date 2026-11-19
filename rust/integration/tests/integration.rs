//! Every public function of the Rust package against the real emulator (Moto, Docker), each with
//! a round-trip assertion (write → read back). Runs single-threaded (`--test-threads=1`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use grow_aws_cloudwatch_logs as sdk;
use grow_aws_cloudwatch_logs_integration::load_bootstrap_env;
use sdk::logging::{
    attrs, ExportOptions, LogClient, LogExporter, LogInput, LogQueryClient, LogRecord,
    SearchOptions, Severity, SortOrder, TailOptions, WaitOptions,
};
use sdk::*;

fn setup() -> (CloudWatchLogsClient, String) {
    load_bootstrap_env();
    let client = CloudWatchLogsClient::from_env().expect("client from env");
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    (client, format!("grow-it-rust/{nanos:x}"))
}

fn moto() -> bool {
    std::env::var("GROW_TEST_EMULATOR").as_deref() == Ok("moto")
}

#[tokio::test]
async fn core_functions() {
    let (c, _) = setup();
    let h = c.health().await.expect("health");
    assert!(h.healthy, "{h:?}");
    assert!(c.ping().await.expect("ping") > Duration::ZERO);
    assert_eq!(
        c.server_version().await.unwrap_err().code(),
        ErrorCode::Unsupported
    );
    assert!(!c.region().await.unwrap().is_empty());
    let sdk_client = c.sdk().await.unwrap();
    assert!(sdk_client
        .describe_log_groups()
        .limit(1)
        .send()
        .await
        .is_ok());
    c.close().await;
    c.close().await;
    assert!(c.is_closed());
    assert_eq!(c.ping().await.unwrap_err().code(), ErrorCode::Cancelled);
}

#[tokio::test]
async fn bootstrap_functions() {
    let (c, prefix) = setup();
    let group = format!("{prefix}/app");
    let r = c
        .ensure_log_group(
            &group,
            EnsureLogGroupOptions::default()
                .retention_days(7)
                .tag("team", "sdk"),
        )
        .await
        .unwrap();
    assert!(r.created && r.arn.ends_with(&group), "{r:?}");
    let g = c.get_log_group(&group).await.unwrap().expect("group");
    assert_eq!(g.retention_days, Some(7));
    // existing: tags merged, retention re-applied
    let r = c
        .ensure_log_group(
            &group,
            EnsureLogGroupOptions::default()
                .retention_days(14)
                .tag("env", "it"),
        )
        .await
        .unwrap();
    assert!(!r.created);
    let tags = c.list_log_group_tags(&group).await.unwrap();
    assert_eq!(tags.get("team").map(String::as_str), Some("sdk"));
    assert_eq!(tags.get("env").map(String::as_str), Some("it"));
    c.tag_log_group(&group, [("extra".to_owned(), "1".to_owned())].into())
        .await
        .unwrap();
    c.untag_log_group(&group, ["env"]).await.unwrap();
    let tags = c.list_log_group_tags(&r.arn).await.unwrap();
    assert!(
        tags.contains_key("extra") && !tags.contains_key("env"),
        "{tags:?}"
    );
    c.put_retention_policy(&group, None).await.unwrap();
    assert_eq!(
        c.get_log_group(&group)
            .await
            .unwrap()
            .unwrap()
            .retention_days,
        None
    );
    c.put_retention_policy(&group, Some(30)).await.unwrap();
    assert_eq!(
        c.get_log_group(&group)
            .await
            .unwrap()
            .unwrap()
            .retention_days,
        Some(30)
    );
    assert_eq!(
        c.put_retention_policy(&group, Some(2))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );

    for s in ["s1", "s2", "s3"] {
        assert!(
            c.ensure_log_stream(&group, s, EnsureLogStreamOptions::default())
                .await
                .unwrap()
                .created
        );
    }
    assert!(
        !c.ensure_log_stream(&group, "s1", EnsureLogStreamOptions::default())
            .await
            .unwrap()
            .created
    );
    let streams = c
        .describe_log_streams(
            &group,
            DescribeLogStreamsOptions {
                page_size: Some(2),
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        streams.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        ["s1", "s2", "s3"]
    );
    let desc = c
        .describe_log_streams(
            &group,
            DescribeLogStreamsOptions {
                descending: true,
                limit: Some(1),
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(desc[0].name, "s3");
    let groups = c
        .describe_log_groups(DescribeLogGroupsOptions {
            prefix: Some(prefix.clone()),
            page_size: Some(1),
            limit: None,
        })
        .try_collect()
        .await
        .unwrap();
    assert!(groups.iter().any(|g| g.name == group));
    assert!(c
        .delete_log_stream(&group, "s3", DeleteOptions::default())
        .await
        .unwrap());
    assert!(!c
        .delete_log_stream(
            &group,
            "s3",
            DeleteOptions {
                ignore_missing: true
            }
        )
        .await
        .unwrap());
    assert_eq!(
        c.delete_log_stream(&group, "s3", DeleteOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    // stream with create_group, then the group is deleted
    let other = format!("{prefix}/other");
    assert!(
        c.ensure_log_stream(&other, "x", EnsureLogStreamOptions { create_group: true })
            .await
            .unwrap()
            .created
    );
    assert!(c
        .delete_log_group(&other, DeleteOptions::default())
        .await
        .unwrap());
    assert!(c.get_log_group(&other).await.unwrap().is_none());
    assert_eq!(
        c.delete_log_group(&other, DeleteOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    // KMS mismatch
    let kms = format!("{prefix}/kms");
    c.ensure_log_group(
        &kms,
        EnsureLogGroupOptions::default().kms_key_id("arn:aws:kms:us-east-1:123456789012:key/k1"),
    )
    .await
    .unwrap();
    assert_eq!(
        c.ensure_log_group(&kms, EnsureLogGroupOptions::default().kms_key_id("k2"))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Conflict
    );
    assert!(c
        .delete_log_group(&kms, DeleteOptions::default())
        .await
        .unwrap());
    assert_eq!(
        c.ensure_log_group("bad name!", EnsureLogGroupOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert!(c
        .delete_log_group(&group, DeleteOptions::default())
        .await
        .unwrap());
}

#[tokio::test]
async fn ingest_and_query_functions() {
    let (c, prefix) = setup();
    let group = format!("{prefix}/data");
    c.ensure_log_group(&group, EnsureLogGroupOptions::default())
        .await
        .unwrap();
    c.ensure_log_stream(&group, "s1", EnsureLogStreamOptions::default())
        .await
        .unwrap();
    let marker = format!("marker-{}", prefix.replace('/', "-"));
    let base = SystemTime::now() - Duration::from_secs(60);
    let ms = |n: u64| base + Duration::from_millis(n * 1000);
    let res = c
        .put_log_events(
            &group,
            "s1",
            vec![
                LogEvent::at(ms(2), format!("third {marker} ERROR")),
                LogEvent::at(ms(0), format!("first {marker}")),
                LogEvent::at(ms(1), format!("second {marker} ünïcode ✓")),
            ],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!((res.accepted, res.batches), (3, 1));
    // auto-created stream
    let res = c
        .put_log_events(
            &group,
            "auto",
            vec![LogEvent::at(ms(0), format!("auto {marker}"))],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(res.accepted, 1);
    // oversize truncation round trip
    let big = format!("{}{marker}", "x".repeat(300_000));
    let res = c
        .put_log_events(
            &group,
            "s2",
            vec![LogEvent::at(ms(0), big)],
            PutLogEventsOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!((res.truncated, res.accepted), (1, 1));
    let back = c
        .get_log_events(
            &group,
            "s2",
            GetLogEventsOptions {
                start_from_head: true,
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(back.len(), 1);
    assert!(
        back[0].message.len() <= DEFAULT_MAX_EVENT_BYTES && back[0].message.contains("truncated")
    );
    // 10,001 events are split client side
    let many: Vec<LogEvent> = (0..10_001).map(|_| LogEvent::at(ms(0), "bulk")).collect();
    let res = c
        .put_log_events(&group, "bulk", many, PutLogEventsOptions::default())
        .await
        .unwrap();
    assert_eq!((res.batches, res.accepted), (2, 10_001));
    // EMF
    let dims: BTreeMap<String, String> = [("service".to_owned(), "api".to_owned())].into();
    let doc = build_emf(
        "latency",
        42.0,
        &dims,
        &EmfOptions::default()
            .namespace("GrowSdkIT")
            .unit("Milliseconds"),
    )
    .unwrap();
    assert!(doc.contains("\"CloudWatchMetrics\""));
    let res = c
        .emf(
            "latency",
            42.0,
            &dims,
            EmfOptions {
                group: Some(group.clone()),
                stream: Some("emf".into()),
                ..Default::default()
            }
            .property("marker", marker.clone()),
        )
        .await
        .unwrap();
    assert!(res.contains(&marker), "{res}");
    let emf = c
        .get_log_events(
            &group,
            "emf",
            GetLogEventsOptions {
                start_from_head: true,
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&emf[0].message).unwrap();
    assert_eq!(parsed["latency"], 42);
    assert_eq!(parsed["marker"], marker.as_str());
    assert_eq!(truncate_message("abcdef", 3).len(), 3);

    // GetLogEvents
    let head = c
        .get_log_events(
            &group,
            "s1",
            GetLogEventsOptions {
                start_from_head: true,
                page_size: Some(1),
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(head.len(), 3);
    assert!(head[0].message.starts_with("first") && head[2].message.starts_with("third"));
    assert!(head[1].message.contains("ünïcode ✓"));
    let latest = c
        .get_log_events(
            &group,
            "s1",
            GetLogEventsOptions {
                limit: Some(1),
                ..Default::default()
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(latest.len(), 1);
    // FilterLogEvents
    let filtered = c
        .filter_log_events(FilterLogEventsOptions {
            group: Some(group.clone()),
            streams: vec!["s1".into()],
            pattern: Some("ERROR".into()),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert!(filtered[0].message.starts_with("third") && filtered[0].log_stream_name == "s1");
    let paged = c
        .filter_log_events(FilterLogEventsOptions {
            group: Some(group.clone()),
            pattern: Some(format!("\"{marker}\"")),
            page_size: Some(1),
            start: Some(base - Duration::from_secs(1)),
            end: Some(SystemTime::now() + Duration::from_secs(60)),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    assert!(paged.len() >= 5, "{}", paged.len());
    let limited = c
        .filter_log_events(FilterLogEventsOptions {
            group: Some(group.clone()),
            pattern: Some(marker.clone()),
            limit: Some(2),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(limited.len(), 2);
    let opts = FilterLogEventsOptions {
        group: Some(group.clone()),
        streams: vec!["s1".into()],
        page_size: Some(2),
        ..Default::default()
    };
    let page = c.filter_log_events_page(opts.clone(), None).await.unwrap();
    assert_eq!(page.events.len(), 2);
    let page = c
        .filter_log_events_page(opts, page.next_token.as_deref())
        .await
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert!(page.events[0].message.starts_with("third"));

    // Logs Insights
    let range = QueryOptions::default().groups([group.clone()]).range(
        Some(base - Duration::from_secs(60)),
        Some(SystemTime::now() + Duration::from_secs(60)),
    );
    let id = c
        .start_query(
            "fields @timestamp, @message | sort @timestamp asc | limit 3",
            range.clone(),
        )
        .await
        .unwrap();
    assert!(!id.is_empty());
    let r = c.get_query_results(&id).await.unwrap();
    assert!(
        r.status.is_terminal()
            || r.status == QueryStatus::Running
            || r.status == QueryStatus::Scheduled
    );
    let res = c
        .query(
            "fields @timestamp, @message | sort @timestamp desc | limit 2",
            range,
        )
        .await
        .unwrap();
    assert_eq!(res.status, QueryStatus::Complete);
    assert_eq!(res.rows.len(), 2);
    assert!(!res.rows[0]["@message"].is_empty());
    parse_query_timestamp(&res.rows[0]["@timestamp"]).unwrap();
    let qs = c
        .describe_queries(DescribeQueriesOptions {
            group: Some(group.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(qs.iter().any(|q| q.query_id == id), "{qs:?}");
    // StopQuery is not implemented by Moto 5.2.4 (HTTP 500); unit tests cover it.
    if moto() {
        assert_eq!(
            c.stop_query(&id).await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
    } else {
        c.stop_query(&id).await.unwrap();
    }
    c.delete_log_group(&group, DeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn logging_category_functions() {
    let (c, prefix) = setup();
    let group = format!("{prefix}/logs");
    c.ensure_log_group(&group, EnsureLogGroupOptions::default())
        .await
        .unwrap();
    let marker = format!("marker-{}", prefix.replace('/', "-"));
    let lc = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .log_group(group.clone())
            .log_stream("logclient")
            .build_from_env()
            .unwrap(),
    )
    .unwrap();
    let logs = lc
        .log_client(LogClientOptions {
            resource: attrs! { "service.name" => "it-rust" },
            ..Default::default()
        })
        .unwrap();
    let q = lc.log_query_client(None).unwrap();
    assert_eq!(q.group(), group);
    let logger = logs.logger(Some("it"), attrs! { "run" => prefix.clone() });
    logger.info(
        format!("logger hello {marker}"),
        attrs! { "password" => format!("hunter2-{prefix}") },
    );
    logs.send(
        LogInput::new(format!("send hello {marker}"))
            .severity(Severity::Warn)
            .trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")
            .into(),
    );
    logs.flush(None).await.unwrap();
    let st = logs.stats();
    assert_eq!((st.sent, st.failed), (2, 0), "{st:?}");
    let wait = WaitOptions::default().timeout(Duration::from_secs(20));
    let rec = q
        .wait_for_log(&format!("send hello {marker}"), wait.clone())
        .await
        .unwrap();
    assert_eq!(rec.severity, Severity::Warn);
    assert_eq!(
        rec.trace_id.as_deref(),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    assert_eq!(rec.resource["service.name"], "it-rust");
    assert_eq!(rec.attributes[ATTR_LOG_STREAM], "logclient");
    let lrec = q
        .wait_for_log(&format!("logger hello {marker}"), wait)
        .await
        .unwrap();
    assert!(
        !lrec.to_json().contains(&format!("hunter2-{prefix}")),
        "password not redacted"
    );
    // exporter directly
    let exp = lc
        .exporter(ExporterOptions {
            stream: Some("exporter".into()),
            ..Default::default()
        })
        .unwrap();
    let er = exp
        .export(
            &[LogRecord::new(
                Severity::Error,
                format!("exported {marker}"),
            )],
            ExportOptions::new(true),
        )
        .await
        .unwrap();
    assert_eq!((er.accepted, er.rejected), (1, 0));
    // search: desc newest first, asc oldest first, cursors
    let desc = q
        .search(&marker, SearchOptions::default().limit(2))
        .await
        .unwrap();
    assert_eq!(desc.records.len(), 2);
    assert!(desc.next_cursor.is_some());
    assert!(desc.records[0].timestamp >= desc.records[1].timestamp);
    let mut all_desc = desc.records.clone();
    let mut cursor = desc.next_cursor.clone();
    while let Some(cur) = cursor {
        let p = q
            .search(&marker, SearchOptions::default().limit(2).cursor(cur))
            .await
            .unwrap();
        all_desc.extend(p.records);
        cursor = p.next_cursor;
    }
    let mut all_asc = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut o = SearchOptions::default().limit(2).order(SortOrder::Asc);
        if let Some(cur) = cursor.take() {
            o = o.cursor(cur);
        }
        let p = q.search(&marker, o).await.unwrap();
        all_asc.extend(p.records);
        match p.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(all_desc.len(), 3);
    assert_eq!(all_asc.len(), 3);
    assert!(all_asc[0].timestamp <= all_asc[2].timestamp);
    let streams = q.list_streams().await.unwrap();
    assert!(
        streams.contains(&"logclient".to_owned()) && streams.contains(&"exporter".to_owned()),
        "{streams:?}"
    );
    assert_eq!(
        q.list_labels().await.unwrap_err().code(),
        ErrorCode::Unsupported
    );
    // record helpers
    assert_eq!(filter_pattern("a b"), "\"a b\"");
    assert_eq!(
        message_to_record(&rec.to_json(), SystemTime::now(), None).severity,
        Severity::Warn
    );
    lc.close().await;
    assert_eq!(logs.stats().sent, 2);
    c.delete_log_group(&group, DeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn live_tail_functions() {
    // Moto resolves every logGroupIdentifier to the most recently created group and replays the
    // existing events once, so the group is created last and the events are put before tailing.
    let (c, prefix) = setup();
    let tail = format!("{prefix}/tail");
    c.ensure_log_group(&tail, EnsureLogGroupOptions::default())
        .await
        .unwrap();
    let marker = format!("marker-{}", prefix.replace('/', "-"));
    c.put_log_events(
        &tail,
        "t1",
        vec![
            LogEvent::new(format!("tail one {marker}")),
            LogEvent::new(format!("tail two {marker}")),
        ],
        PutLogEventsOptions::default(),
    )
    .await
    .unwrap();
    let arn = c.log_group_arn(&tail).await.unwrap();
    assert!(arn.ends_with(&format!(":log-group:{tail}")), "{arn}");
    let got: Arc<Mutex<Vec<LiveTailEvent>>> = Arc::default();
    let g2 = got.clone();
    let updates: Arc<Mutex<Vec<LiveTailUpdate>>> = Arc::default();
    let u2 = updates.clone();
    let s = c
        .start_live_tail(
            LiveTailOptions::default()
                .groups([tail.clone()])
                .pattern("tail")
                .on_update(move |u| u2.lock().unwrap().push(u)),
            Arc::new(move |e| g2.lock().unwrap().push(e)),
        )
        .await
        .unwrap();
    wait_for(|| got.lock().unwrap().len() >= 2 || s.is_done()).await;
    s.close();
    tokio::time::timeout(Duration::from_secs(10), s.done())
        .await
        .unwrap();
    {
        let got = got.lock().unwrap();
        assert!(got.len() >= 2, "{got:?}");
        assert!(got[0].message.contains("tail one") && got[0].log_stream_name == "t1");
    }
    assert!(!s.session_id().is_empty());
    assert!(!updates.lock().unwrap().is_empty());
    assert!(s.error().is_none(), "{:?}", s.error());
    // tail through the LogQueryClient
    let q = c.log_query_client(Some(&tail)).unwrap();
    let recs: Arc<Mutex<Vec<LogRecord>>> = Arc::default();
    let r2 = recs.clone();
    let h = q
        .tail(
            "tail two",
            Arc::new(move |r| r2.lock().unwrap().push(r)),
            TailOptions::default(),
        )
        .await
        .unwrap();
    wait_for(|| !recs.lock().unwrap().is_empty() || h.is_stopped()).await;
    h.stop();
    let recs = recs.lock().unwrap().clone();
    assert_eq!(recs.len(), 1, "{recs:?}");
    assert!(recs[0].body.contains("tail two"));
}

async fn wait_for(mut done: impl FnMut() -> bool) {
    for _ in 0..300 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn permission_denied_with_the_read_only_user() {
    load_bootstrap_env();
    let (Ok(ak), Ok(sk)) = (
        std::env::var("GROW_TEST_READONLY_ACCESS_KEY_ID"),
        std::env::var("GROW_TEST_READONLY_SECRET_ACCESS_KEY"),
    ) else {
        eprintln!("no read-only user in /bootstrap/env");
        return;
    };
    let (admin, prefix) = setup();
    let ro = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .access_key_id(ak)
            .secret_access_key(sk)
            .build_from_env()
            .unwrap(),
    )
    .unwrap();
    ro.ping().await.unwrap();
    let err = ro
        .ensure_log_group(&format!("{prefix}/ro"), EnsureLogGroupOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::PermissionDenied, "{err}");
    let seed =
        std::env::var("GROW_TEST_SEED_LOG_GROUP").unwrap_or_else(|_| "grow-bootstrap/seed".into());
    let err = ro
        .put_log_events(
            &seed,
            "seed-stream",
            vec![LogEvent::new("x")],
            PutLogEventsOptions {
                auto_create: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::PermissionDenied, "{err}");
    // reads still work: the seed events
    let marker =
        std::env::var("GROW_TEST_SEED_MARKER").unwrap_or_else(|_| "grow-seed-event".into());
    let seeded = ro
        .filter_log_events(FilterLogEventsOptions {
            group: Some(seed),
            pattern: Some(marker),
            ..Default::default()
        })
        .try_collect()
        .await
        .unwrap();
    assert!(seeded.len() >= 3);
    // bad credentials
    let bad = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .access_key_id("AKIAUNKNOWNKEY000000")
            .secret_access_key("wrong-secret-value")
            .build_from_env()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        bad.ping().await.unwrap_err().code(),
        ErrorCode::Authentication
    );
    let h = bad.health().await.unwrap();
    assert!(h.healthy && h.details.get("auth").map(String::as_str) == Some("failed"));
    admin.close().await;
}

#[test]
fn blocking_client_round_trip() {
    load_bootstrap_env();
    let c = sdk::blocking::CloudWatchLogsClient::from_env().expect("client");
    assert!(c.health().unwrap().healthy);
    c.ping().unwrap();
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let group = format!("grow-it-rust/{nanos:x}/blocking");
    assert!(
        c.ensure_log_group(&group, EnsureLogGroupOptions::default())
            .unwrap()
            .created
    );
    let res = c
        .put_log_events(
            &group,
            "b",
            vec![LogEvent::new("blocking hello")],
            PutLogEventsOptions::default(),
        )
        .unwrap();
    assert_eq!(res.accepted, 1);
    let back = c
        .get_log_events(
            &group,
            "b",
            GetLogEventsOptions {
                start_from_head: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(back[0].message, "blocking hello");
    assert!(c
        .delete_log_group(&group, DeleteOptions::default())
        .unwrap());
    c.close();
}
