# grow-aws-cloudwatch-logs (Rust)

Rust SDK for Amazon CloudWatch Logs by [Grow Codes](https://grow.codes), built on the official AWS SDK
for Rust (`aws-sdk-cloudwatchlogs`). Async on tokio, with an optional blocking client.

```toml
[dependencies]
grow-aws-cloudwatch-logs = "0.1.0"
# blocking client:   features = ["blocking"]
# test fakes/runner: features = ["testing"] (dev-dependency)
```

```rust,no_run
use grow_aws_cloudwatch_logs::{CloudWatchLogsClient, CloudWatchLogsConfig};

#[tokio::main]
async fn main() -> grow_aws_cloudwatch_logs::Result<()> {
    // explicit > GROW_AWS_CLOUDWATCH_LOGS_* > vendor variables > defaults
    let client = CloudWatchLogsClient::new(CloudWatchLogsConfig::from_env()?)?;
    println!("{:?}", client.health().await?);
    client.close().await;
    Ok(())
}
```

- Bootstrap: `ensure_log_group`, `ensure_log_stream`, `put_retention_policy`, `delete_log_group`,
  `delete_log_stream`, `describe_log_groups` / `describe_log_streams` (lazy `Paginator`s), tags.
- Ingest: `put_log_events` (client-side batching: 10,000 events / 1 MiB / 24 h, sorted, 256 KiB
  per-event policy), `emf` / `build_emf`.
- Query: `filter_log_events`, `get_log_events`, Logs Insights `start_query` / `get_query_results` /
  `query` / `stop_query` / `describe_queries`; live tail with `start_live_tail`.
- Logging category: `log_client` (batching `LogClient` + `Logger`), `exporter`, `log_query_client`
  (`search`, `tail`, `list_streams`, `wait_for_log`); `tracing` layer and `log` bridge.
- Errors: one `Error` enum with `code()`, `http_status()`, `request_id()`, `is_retryable()`,
  `retry_after()`; messages never contain credentials.
- AWS SDK standard retries (`max_retries + 1` attempts), connect/request/total timeouts, TLS and
  proxy on the crate's own HTTP client, cancellation by dropping the future, request hooks,
  `tracing` debug events. Credentials come from the config or the standard AWS chain.
- MSRV 1.88. Licence Apache-2.0. Documentation: https://grow-codes-packages.github.io/aws-cloudwatch-logs/

Development: `make deps build lint test coverage license pack docs` (see `Makefile`).
