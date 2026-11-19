// Created by sdk-kit 0.1.0; owned by this repository.
//! Conformance scenarios for `core` and `logging` against the real service (Docker).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use grow_aws_cloudwatch_logs as sdk;
use grow_aws_cloudwatch_logs_integration as support;
use sdk::testing::logging::{LoggingHandler, LoggingTarget};
use sdk::testing::{ConformanceRunner, CoreHandler};
use sdk::{
    CloudWatchLogsClient as Client, CloudWatchLogsConfig as Config, EnsureLogGroupOptions,
    LogClientOptions,
};
use support::{conformance_dir, load_bootstrap_env};

#[tokio::test]
async fn conformance() {
    load_bootstrap_env();
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let counter = Arc::new(AtomicUsize::new(0));
    // Every logging scenario writes to its own log group, so runs never see each other.
    let logging = LoggingHandler::new(move |batch| {
        let n = counter.fetch_add(1, Ordering::SeqCst);
        async move {
            let group = format!("grow-conformance-rust/{run:x}-{n}");
            let client = Client::new(
                Config::builder()
                    .log_group(group.clone())
                    .log_stream(format!("conformance-{n}"))
                    .build_from_env()?,
            )?;
            client
                .ensure_log_group(&group, EnsureLogGroupOptions::default())
                .await?;
            let log_client = client.log_client(LogClientOptions {
                batch,
                ..Default::default()
            })?;
            Ok(LoggingTarget {
                client: Arc::new(log_client),
                query: Some(Arc::new(client.log_query_client(None)?)),
            })
        }
    });
    // "version" is left out: CloudWatch Logs exposes no server version (UNSUPPORTED, see
    // docs/compatibility.md).
    let report = ConformanceRunner::new(conformance_dir())
        .capabilities(["query"])
        .handler(CoreHandler::new(Client::from_env))
        .handler(logging)
        .run()
        .await
        .expect("scenarios load");
    println!("{report}");
    report.assert_success();
}
