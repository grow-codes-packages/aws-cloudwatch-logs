// Service-owned (created from the sdk-kit 0.1.0 skeleton).
//! # grow-aws-cloudwatch-logs
//!
//! Rust SDK for [Amazon CloudWatch Logs](https://grow-codes-packages.github.io/aws-cloudwatch-logs/)
//! by Grow Codes, built on the official AWS SDK for Rust (`aws-sdk-cloudwatchlogs`).
//!
//! ```no_run
//! # async fn demo() -> grow_aws_cloudwatch_logs::Result<()> {
//! use grow_aws_cloudwatch_logs::logging::{attrs, LogClient, LogQueryClient, SearchOptions};
//! use grow_aws_cloudwatch_logs::{
//!     CloudWatchLogsClient, CloudWatchLogsConfig, EnsureLogGroupOptions, LogClientOptions,
//!     LogEvent, PutLogEventsOptions,
//! };
//!
//! let client = CloudWatchLogsClient::new(CloudWatchLogsConfig::from_env()?)?;
//! client
//!     .ensure_log_group("app/web", EnsureLogGroupOptions::default().retention_days(14))
//!     .await?;
//! client
//!     .put_log_events("app/web", "web-1", vec![LogEvent::new("hello")], PutLogEventsOptions::default())
//!     .await?;
//!
//! // Logging category: batching LogClient + Logger facade, and search.
//! let logs = client.log_client(LogClientOptions::default())?;
//! logs.logger(Some("http"), attrs! {}).info("listening", attrs! { "port" => 8080 });
//! logs.flush(None).await?;
//! let found = client.log_query_client(None)?.search("listening", SearchOptions::default()).await?;
//! println!("{} records", found.records.len());
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! Features: `blocking` (a blocking client), `testing` (in-memory fakes and the conformance
//! runner), `tracing-layer` and `log-bridge` (logging adapters, on by default).
//!
//! Not affiliated with or endorsed by Amazon Web Services. Amazon CloudWatch and AWS are
//! trademarks of Amazon.com, Inc. or its affiliates.
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod core;

mod aws_error;
mod client;
mod config;
mod emf;
mod groups;
mod ingest;
mod livetail;
mod logs;
mod models;
mod paginator;
mod query;
mod sdk;

#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;

#[cfg(feature = "testing")]
#[cfg_attr(docsrs, doc(cfg(feature = "testing")))]
pub mod testing;

pub mod logging;

pub use crate::core::{Error, ErrorCode, HealthStatus, Result, RetryPolicy, Secret};

pub use crate::aws_error::{
    aws_error_code, error_code_for_aws, error_code_from_body, AwsErrorDetail, AWS_ERROR_CODES,
};
pub use crate::client::{partition_of, CloudWatchLogsClient};
pub use crate::config::{
    CloudWatchLogsConfig, CloudWatchLogsConfigBuilder, OversizePolicy, API_VERSION,
    DEFAULT_MAX_EVENT_BYTES, DEFAULT_ROLE_SESSION_NAME, DEFAULT_URL, MAX_EVENT_BYTES_LIMIT,
    VENDOR_LOG_GROUP_VARS, VENDOR_LOG_STREAM_VARS, VENDOR_PROFILE_VARS, VENDOR_REGION_VARS,
    VENDOR_URL_VARS,
};
pub use crate::emf::{build_emf, EmfOptions, DEFAULT_EMF_NAMESPACE, EMF_UNITS};
pub use crate::ingest::truncate_message;
pub use crate::livetail::LiveTailSession;
pub use crate::logs::{
    filter_pattern, filtered_event_to_record, live_tail_event_to_record, message_to_record,
    CloudWatchLogsExporter, CloudWatchLogsQueryClient, ExporterOptions, LogClientOptions,
};
pub use crate::models::*;
pub use crate::paginator::Paginator;
pub use crate::query::parse_query_timestamp;

/// The AWS SDK for Rust crate this package is built on (for `with_sdk_client` and the escape
/// hatch [`CloudWatchLogsClient::sdk`]).
pub use aws_sdk_cloudwatchlogs;

/// Service-prefixed alias of [`Error`].
pub type CloudWatchLogsError = Error;
