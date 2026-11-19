// Created by sdk-kit 0.1.0; owned by this repository.
//! Walk-through of the Amazon CloudWatch Logs Rust SDK: configuration from the environment,
//! health and ping, creating a log group and stream, writing events, reading them back, a Logs
//! Insights query, an Embedded Metric Format metric, the batching LogClient with its Logger, and
//! search.
//!
//! It uses the standard AWS credential chain. Against the Docker emulator:
//!
//! ```sh
//! GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000 AWS_REGION=us-east-1 \
//! AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... cargo run
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use grow_aws_cloudwatch_logs as sdk;
use sdk::core::{ErrorInfo, Hooks};
use sdk::logging::{attrs, LogClient, LogQueryClient, SearchOptions};
use sdk::{
    CloudWatchLogsClient, CloudWatchLogsConfig, DeleteOptions, EmfOptions, EnsureLogGroupOptions,
    ErrorCode, FilterLogEventsOptions, GetLogEventsOptions, LogClientOptions, LogEvent,
    PutLogEventsOptions, QueryOptions,
};

/// Prints every retry.
struct RetryLogger;

impl Hooks for RetryLogger {
    fn on_error(&self, info: &ErrorInfo) {
        if info.will_retry {
            eprintln!("retrying {} (attempt {})", info.url, info.attempt);
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error [{}]: {err}", err.code());
        std::process::exit(1);
    }
}

async fn run() -> sdk::Result<()> {
    let stamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let group = format!("grow-example/rust-{stamp}");
    // explicit > GROW_AWS_CLOUDWATCH_LOGS_* > AWS_* / CLOUDWATCH_* > defaults; invalid values name
    // the variable, never its value.
    let config = CloudWatchLogsConfig::builder()
        .log_group(group.clone())
        .log_stream("example")
        .hooks(Arc::new(RetryLogger))
        .build_from_env()?;
    println!("config: {config:?}"); // credentials print as [REDACTED]
    let client = CloudWatchLogsClient::new(config)?;

    let health = client.health().await?; // never fails for an unreachable service
    println!(
        "health: healthy={} status={}",
        health.healthy, health.status
    );
    match client.ping().await {
        Ok(latency) => println!("ping: {latency:?}"),
        Err(e) if e.code() == ErrorCode::Authentication => {
            println!("check your AWS credentials: {e}");
            return Err(e);
        }
        Err(e) => return Err(e),
    }

    // Bootstrap: group (retention + tags) and stream; both calls are idempotent.
    let created = client
        .ensure_log_group(
            &group,
            EnsureLogGroupOptions::default()
                .retention_days(7)
                .tag("team", "sdk"),
        )
        .await?;
    println!(
        "log group {group} created={} arn={}",
        created.created, created.arn
    );
    let result = demo(&client, &group).await;
    client
        .delete_log_group(
            &group,
            DeleteOptions {
                ignore_missing: true,
            },
        )
        .await?;
    client.close().await;
    result
}

async fn demo(client: &CloudWatchLogsClient, group: &str) -> sdk::Result<()> {
    // Ingest: events are sorted, size-checked and split into valid batches.
    let now = SystemTime::now();
    let res = client
        .put_log_events(
            group,
            "raw",
            vec![
                LogEvent::at(now, "hello from Rust"),
                LogEvent::at(now + Duration::from_millis(1), "something failed: ERROR"),
            ],
            PutLogEventsOptions::default(),
        )
        .await?;
    println!(
        "put: accepted={} rejected={} batches={}",
        res.accepted, res.rejected, res.batches
    );

    // Read back: one stream, then a filter across the group.
    let mut events = client.get_log_events(
        group,
        "raw",
        GetLogEventsOptions {
            start_from_head: true,
            ..Default::default()
        },
    );
    while let Some(ev) = events.next().await? {
        println!("event: {}", ev.message);
    }
    let matches = client
        .filter_log_events(FilterLogEventsOptions {
            pattern: Some("ERROR".into()),
            ..Default::default()
        })
        .try_collect()
        .await?;
    for ev in matches {
        println!("match: {} {}", ev.log_stream_name, ev.message);
    }

    // Logs Insights: start, poll until complete.
    let q = client
        .query(
            "fields @timestamp, @message | sort @timestamp desc | limit 5",
            QueryOptions::default().range(Some(now - Duration::from_secs(3600)), None),
        )
        .await?;
    println!("insights: {}, {} rows", q.status, q.rows.len());

    // Embedded Metric Format: a metric published through a log event.
    let dims: BTreeMap<String, String> = [("service".to_owned(), "example".to_owned())].into();
    client
        .emf(
            "latency",
            42.0,
            &dims,
            EmfOptions::default().unit("Milliseconds"),
        )
        .await?;

    // Category API: batching LogClient + Logger, then search the same group.
    let logs = client.log_client(LogClientOptions {
        resource: attrs! { "service.name" => "example" },
        ..Default::default()
    })?;
    let logger = logs.logger(Some("example"), attrs! {});
    logger.info(
        "user signed in",
        attrs! { "user" => "ada", "password" => "never-sent" }, // password is redacted
    );
    logs.flush(None).await?;
    let search = client.log_query_client(None)?; // the configured log group
    let found = search
        .search("user signed in", SearchOptions::default().limit(10))
        .await?;
    for r in &found.records {
        println!("search: {} {} {:?}", r.severity, r.body, r.attributes);
    }
    Ok(())
}
