# grow-aws-cloudwatch-logs (Rust)

Rust SDK for Amazon CloudWatch Logs by [Grow Codes](https://grow.codes). Async on tokio, with an optional
blocking client.

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

- Errors: one `Error` enum with `code()`, `http_status()`, `request_id()`, `is_retryable()`,
  `retry_after()`; messages never contain credentials.
- Retries with full-jitter backoff and `Retry-After`, connect/request/total timeouts,
  cancellation (drop the future or pass a `CancellationToken`), request hooks, `tracing` debug
  events.
- MSRV 1.88. Licence Apache-2.0. Documentation: https://grow-codes-packages.github.io/aws-cloudwatch-logs/

Development: `make deps build lint test coverage license pack docs` (see `Makefile`).
