// Service-owned (created from the sdk-kit 0.1.0 skeleton).
//! `CloudWatchLogsClient`: entry point of the SDK (SPEC §2.4), built on the official AWS SDK
//! for Rust (`aws-sdk-cloudwatchlogs`, SPEC §2.3 "Cloud SDK services").

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_sdk_cloudwatchlogs::error::{ProvideErrorMetadata, SdkError};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use futures::future::BoxFuture;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::aws_error::{map_sdk_error, MapOptions};
use crate::config::CloudWatchLogsConfig;
use crate::core::lifecycle::CloseGroup;
use crate::core::{Error, ErrorCode, HealthStatus, Result};
use crate::livetail::SessionControl;
use crate::logging::{BatchingLogClient, LogClient};
use crate::models::validate_group_name;
use crate::sdk::{build_sdk_clients, GrowHttpClient, SdkClients};

/// Upper bound of the health probe timeout.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Async client for Amazon CloudWatch Logs. Cheap to clone (clones share one SDK client).
///
/// The AWS SDK client is built from the configuration on the first call (loading the region,
/// profile and credential chain is asynchronous), or wraps the client given with
/// [`CloudWatchLogsClient::with_sdk_client`]. Calls are cancelled by dropping their future;
/// [`close`](CloudWatchLogsClient::close) flushes the log clients created by
/// [`log_client`](CloudWatchLogsClient::log_client), stops live-tail sessions, cancels in-flight
/// calls and rejects new ones with `CANCELLED`.
#[derive(Clone)]
pub struct CloudWatchLogsClient {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) config: CloudWatchLogsConfig,
    pub(crate) log_stream: String,
    http: Option<GrowHttpClient>,
    sdk: OnceCell<SdkClients>,
    pub(crate) known_secrets: Vec<String>,
    closing: AtomicBool,
    closed: AtomicBool,
    close_token: CancellationToken,
    on_close: CloseGroup,
    log_clients: Mutex<Vec<BatchingLogClient>>,
    pub(crate) sessions: Mutex<Vec<Arc<SessionControl>>>,
}

impl fmt::Debug for CloudWatchLogsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudWatchLogsClient")
            .field("config", &self.inner.config)
            .field("log_stream", &self.inner.log_stream)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// `<hostname>-<pid>-<random6>` with `:` and `*` replaced.
pub(crate) fn default_stream_name() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|h| h.trim().to_owned())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_else(|| "host".to_owned());
    let host: String = host
        .chars()
        .map(|c| if c == ':' || c == '*' { '-' } else { c })
        .collect();
    let random: u32 = rand::random::<u32>() & 0x00ff_ffff;
    format!("{host}-{}-{random:06x}", std::process::id())
}

impl CloudWatchLogsClient {
    /// Creates a client from a configuration (validates it and builds the HTTP client; the AWS
    /// configuration itself is loaded on the first call).
    pub fn new(config: CloudWatchLogsConfig) -> Result<Self> {
        config.validate()?;
        let sdk = OnceCell::new();
        let http = match config.sdk_client() {
            Some(client) => {
                let region = config
                    .region()
                    .map(str::to_owned)
                    .or_else(|| client.config().region().map(|r| r.to_string()));
                let _ = sdk.set(SdkClients {
                    logs: client.clone(),
                    region,
                });
                None
            }
            None => Some(GrowHttpClient::new(config.common())?),
        };
        let known_secrets = config.known_secrets();
        let log_stream = config
            .log_stream()
            .map(str::to_owned)
            .unwrap_or_else(default_stream_name);
        Ok(CloudWatchLogsClient {
            inner: Arc::new(Inner {
                config,
                log_stream,
                http,
                sdk,
                known_secrets,
                closing: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                close_token: CancellationToken::new(),
                on_close: CloseGroup::new(),
                log_clients: Mutex::new(Vec::new()),
                sessions: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Creates a client from the process environment (`GROW_AWS_CLOUDWATCH_LOGS_*`, the
    /// standard AWS variables and `CLOUDWATCH_LOG_GROUP` / `CLOUDWATCH_LOG_STREAM`).
    pub fn from_env() -> Result<Self> {
        Self::new(CloudWatchLogsConfig::from_env()?)
    }

    /// Wraps an existing AWS SDK client; `close()` leaves it usable. Grow hooks, TLS, proxy,
    /// headers and User-Agent settings do not apply to it.
    pub fn with_sdk_client(sdk_client: aws_sdk_cloudwatchlogs::Client) -> Result<Self> {
        Self::new(
            CloudWatchLogsConfig::builder()
                .sdk_client(sdk_client)
                .build()?,
        )
    }

    /// The configuration in use.
    pub fn config(&self) -> &CloudWatchLogsConfig {
        &self.inner.config
    }

    /// The stream the log client writes to by default (configured, or
    /// `<hostname>-<pid>-<random6>`).
    pub fn log_stream(&self) -> &str {
        &self.inner.log_stream
    }

    /// The underlying AWS SDK client (escape hatch for operations not wrapped here). Builds it
    /// on first use; `CONFIG` when no region can be found.
    pub async fn sdk(&self) -> Result<aws_sdk_cloudwatchlogs::Client> {
        Ok(self.clients("sdk").await?.logs.clone())
    }

    /// The resolved AWS region (`CONFIG` when none is known).
    pub async fn region(&self) -> Result<String> {
        self.clients("region")
            .await?
            .region
            .clone()
            .ok_or_else(|| Error::config("region: no AWS region is configured"))
    }

    /// Registers work that [`CloudWatchLogsClient::close`] runs (stop watchers, …).
    pub fn on_close<F>(&self, f: F)
    where
        F: FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
    {
        self.inner.on_close.register(f);
    }

    /// Liveness: a signed `DescribeLogGroups {limit: 1}` (prefix = the configured log group)
    /// with a short timeout (min(request timeout, 5 s)) and no retries. AWS has no
    /// unauthenticated health endpoint, so `ok` also when the credentials are rejected
    /// (`details.auth = "failed"`); `degraded` when throttled, `unhealthy` on HTTP 5xx,
    /// `unreachable` on network errors and timeouts. Fails only with `CANCELLED` and `CONFIG`.
    pub async fn health(&self) -> Result<HealthStatus> {
        let timeout = self.config().common().request_timeout().min(HEALTH_TIMEOUT);
        let result = async {
            let sdk = &self.clients("health").await?.logs;
            let overrides = aws_sdk_cloudwatchlogs::config::Builder::default()
                .retry_config(RetryConfig::disabled())
                .timeout_config(
                    TimeoutConfig::builder()
                        .operation_attempt_timeout(timeout)
                        .operation_timeout(timeout)
                        .build(),
                );
            let call = sdk
                .describe_log_groups()
                .limit(1)
                .set_log_group_name_prefix(self.config().log_group().map(str::to_owned))
                .customize()
                .config_override(overrides)
                .send();
            self.run("health", call).await
        }
        .await;
        let err = match result {
            Ok(_) => return Ok(HealthStatus::healthy("ok")),
            Err(err) => err,
        };
        let code = err.code();
        Ok(match code {
            ErrorCode::Cancelled | ErrorCode::Config => return Err(err),
            ErrorCode::RateLimited => {
                HealthStatus::healthy("degraded").with_detail("code", code.as_str())
            }
            ErrorCode::Authentication | ErrorCode::PermissionDenied => HealthStatus::healthy("ok")
                .with_detail("auth", "failed")
                .with_detail("code", code.as_str()),
            _ => match err.http_status() {
                None => HealthStatus::unhealthy("unreachable").with_detail("code", code.as_str()),
                Some(status) if status >= 500 => HealthStatus::unhealthy("unhealthy")
                    .with_detail("httpStatus", status.to_string())
                    .with_detail("code", code.as_str()),
                Some(status) => HealthStatus::healthy("ok")
                    .with_detail("httpStatus", status.to_string())
                    .with_detail("code", code.as_str()),
            },
        })
    }

    /// Authenticated round trip (`DescribeLogGroups {limit: 1}`); returns the latency.
    /// `AUTHENTICATION` for unknown keys or bad signatures, `PERMISSION_DENIED` when the
    /// principal may not call `logs:DescribeLogGroups`.
    pub async fn ping(&self) -> Result<Duration> {
        let started = Instant::now();
        let sdk = &self.clients("ping").await?.logs;
        let call = sdk
            .describe_log_groups()
            .limit(1)
            .set_log_group_name_prefix(self.config().log_group().map(str::to_owned))
            .send();
        self.run("ping", call).await?;
        Ok(started.elapsed().max(Duration::from_nanos(1)))
    }

    /// Always `UNSUPPORTED`: Amazon CloudWatch Logs exposes no server version (the API version
    /// is the constant [`API_VERSION`](crate::API_VERSION)).
    pub async fn server_version(&self) -> Result<String> {
        self.check_open("serverVersion")?;
        Err(Error::unsupported(
            "serverVersion: Amazon CloudWatch Logs does not expose a server version",
        ))
    }

    /// Flushes and closes every log client created by [`log_client`](Self::log_client), stops
    /// live-tail sessions, runs the registered close work, cancels in-flight calls and rejects
    /// new ones with `CANCELLED`. A caller-supplied SDK client stays usable. Idempotent.
    pub async fn close(&self) {
        if self.inner.closing.swap(true, Ordering::SeqCst) {
            // An earlier (or concurrent) close does the work.
            return;
        }
        let clients = std::mem::take(&mut *lock(&self.inner.log_clients));
        for client in clients {
            let _ = client.close().await;
        }
        let sessions = std::mem::take(&mut *lock(&self.inner.sessions));
        for session in sessions {
            session.stop();
        }
        self.inner.on_close.close().await;
        self.shutdown();
    }

    /// Whether [`CloudWatchLogsClient::close`] was called.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// ARN of a log group (without `:*`). For a name it is built from the region and the
    /// configured [`account_id`](crate::CloudWatchLogsConfigBuilder::account_id) when set,
    /// otherwise the group is looked up with `DescribeLogGroups` (`NOT_FOUND` when it does not
    /// exist); an ARN is returned without `:*`.
    pub async fn log_group_arn(&self, group: &str) -> Result<String> {
        self.group_arn("logGroupArn", group).await
    }

    // ------------------------------------------------------------------------------------
    // internals

    pub(crate) fn register_log_client(&self, client: BatchingLogClient) {
        lock(&self.inner.log_clients).push(client);
    }

    /// Marks the client closed and cancels in-flight calls (sync part of `close`).
    pub(crate) fn shutdown(&self) {
        self.inner.closing.store(true, Ordering::SeqCst);
        if !self.inner.closed.swap(true, Ordering::SeqCst) {
            self.inner.close_token.cancel();
        }
    }

    pub(crate) fn check_open(&self, operation: &str) -> Result<()> {
        if self.is_closed() {
            return Err(Error::cancelled(format!("{operation}: client is closed")));
        }
        Ok(())
    }

    pub(crate) fn close_token(&self) -> &CancellationToken {
        &self.inner.close_token
    }

    pub(crate) fn map_options(&self, operation: &str) -> MapOptions {
        MapOptions::for_op(&self.inner.known_secrets, operation)
    }

    /// The SDK clients, built on first use.
    pub(crate) async fn clients(&self, operation: &str) -> Result<&SdkClients> {
        self.check_open(operation)?;
        let inner = &self.inner;
        inner
            .sdk
            .get_or_try_init(|| async {
                // `http` is always set when no SDK client was supplied.
                let http = inner
                    .http
                    .clone()
                    .ok_or_else(|| Error::config("no HTTP client"))?;
                build_sdk_clients(&inner.config, http).await
            })
            .await
            .map_err(|e| {
                let message = format!("{operation}: {}", e.message());
                e.with_message(message)
            })
    }

    /// The CloudWatch Logs SDK client, built on first use.
    pub(crate) async fn logs(&self, operation: &str) -> Result<&aws_sdk_cloudwatchlogs::Client> {
        Ok(&self.clients(operation).await?.logs)
    }

    /// Runs an SDK call: cancellation on close and error mapping.
    pub(crate) async fn run<T, E, F>(&self, operation: &str, call: F) -> Result<T>
    where
        F: Future<Output = std::result::Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        tokio::select! {
            biased;
            _ = self.inner.close_token.cancelled() => {
                Err(Error::cancelled(format!("{operation}: client is closed")))
            }
            result = call => result.map_err(|e| map_sdk_error(operation, e, &self.map_options(operation))),
        }
    }

    /// ARN of a group given by name or ARN (see [`log_group_arn`](Self::log_group_arn)).
    pub(crate) async fn group_arn(&self, operation: &str, group: &str) -> Result<String> {
        if group.starts_with("arn:") {
            return Ok(crate::models::trim_star_arn(group).to_owned());
        }
        validate_group_name(operation, group)?;
        let Some(account) = self.config().account_id().map(str::to_owned) else {
            return Ok(self.find_group(operation, group).await?.arn);
        };
        let region = self
            .clients(operation)
            .await?
            .region
            .clone()
            .ok_or_else(|| {
                Error::config(format!(
                    "{operation}: region is required to build log group ARNs"
                ))
            })?;
        Ok(format!(
            "arn:{}:logs:{region}:{account}:log-group:{group}",
            partition_of(&region)
        ))
    }

    /// `group` or the configured log group, validated (`CONFIG` when neither is set).
    pub(crate) fn group_or_default(&self, operation: &str, group: Option<&str>) -> Result<String> {
        let group = group
            .filter(|g| !g.is_empty())
            .or_else(|| self.config().log_group())
            .ok_or_else(|| {
                Error::config(format!(
                    "{operation}: no log group given and logGroup is not set (GROW_AWS_CLOUDWATCH_LOGS_LOG_GROUP or CLOUDWATCH_LOG_GROUP)"
                ))
            })?;
        validate_group_name(operation, group)?;
        Ok(group.to_owned())
    }
}

/// The AWS partition of a region.
pub fn partition_of(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

/// Locks a mutex, recovering from poisoning (callbacks never run under these locks).
pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}
