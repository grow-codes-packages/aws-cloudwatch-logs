// Service-owned (created from the sdk-kit 0.1.0 skeleton).
//! `CloudWatchLogsConfig`: common settings (SPEC §2.2) plus the AWS settings (region,
//! credentials, profile, assume-role helper, default log group and stream, ingest size policy,
//! an existing SDK client).

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};

use crate::core::redact::{redact_url, REDACTED};
use crate::core::{
    CommonConfig, CommonConfigBuilder, EnvReader, Error, Hooks, Result, RetryPolicy, Secret,
    TlsConfig,
};
use crate::models::{validate_group_name, validate_stream_name};

/// Informational endpoint used when no endpoint override is set and no region is known. The AWS
/// SDK resolves the real endpoint (`https://logs.<region>.amazonaws.com`) itself.
pub const DEFAULT_URL: &str = "https://logs.us-east-1.amazonaws.com";

/// Default STS role session name of the assume-role helper.
pub const DEFAULT_ROLE_SESSION_NAME: &str = "grow-sdk";

/// Amazon CloudWatch Logs API version this package targets.
pub const API_VERSION: &str = "2014-03-28";

/// Default per-event size policy in UTF-8 bytes (256 KiB).
pub const DEFAULT_MAX_EVENT_BYTES: usize = 262_144;

/// Hard per-event cap: 1 MiB minus the 26 bytes of per-event overhead CloudWatch Logs counts
/// against a `PutLogEvents` call.
pub const MAX_EVENT_BYTES_LIMIT: usize = 1_048_550;

/// Vendor variables checked (in order) after `GROW_AWS_CLOUDWATCH_LOGS_URL`.
pub const VENDOR_URL_VARS: &[&str] = &["AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "AWS_ENDPOINT_URL"];
/// Vendor variables checked (in order) after `GROW_AWS_CLOUDWATCH_LOGS_REGION`.
pub const VENDOR_REGION_VARS: &[&str] = &["AWS_REGION", "AWS_DEFAULT_REGION"];
/// Vendor variables checked after `GROW_AWS_CLOUDWATCH_LOGS_PROFILE`.
pub const VENDOR_PROFILE_VARS: &[&str] = &["AWS_PROFILE"];
/// Vendor variables checked after `GROW_AWS_CLOUDWATCH_LOGS_LOG_GROUP`.
pub const VENDOR_LOG_GROUP_VARS: &[&str] = &["CLOUDWATCH_LOG_GROUP"];
/// Vendor variables checked after `GROW_AWS_CLOUDWATCH_LOGS_LOG_STREAM`.
pub const VENDOR_LOG_STREAM_VARS: &[&str] = &["CLOUDWATCH_LOG_STREAM"];

/// What happens to an event larger than the configured maximum event size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OversizePolicy {
    /// Cut the message on a UTF-8 boundary and append ` …[truncated N bytes]` (default).
    /// Normalized log records are truncated in their body so the message stays valid JSON.
    #[default]
    Truncate,
    /// Drop the event (counted in [`PutLogEventsResult::dropped`](crate::PutLogEventsResult)).
    Drop,
    /// Fail the call with `VALIDATION`.
    Error,
}

impl OversizePolicy {
    /// `truncate`, `drop` or `error`.
    pub fn as_str(self) -> &'static str {
        match self {
            OversizePolicy::Truncate => "truncate",
            OversizePolicy::Drop => "drop",
            OversizePolicy::Error => "error",
        }
    }
}

impl fmt::Display for OversizePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OversizePolicy {
    type Err = Error;

    /// Parses `truncate`, `drop` or `error` (case-insensitive).
    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "truncate" => Ok(OversizePolicy::Truncate),
            "drop" => Ok(OversizePolicy::Drop),
            "error" => Ok(OversizePolicy::Error),
            _ => Err(Error::config(
                "oversizePolicy must be truncate, drop or error",
            )),
        }
    }
}

/// Immutable client configuration; build it with [`CloudWatchLogsConfig::builder`] or
/// [`CloudWatchLogsConfig::from_env`]. `Debug` output redacts every credential.
///
/// Credentials left unset come from the standard AWS chain (environment, shared config and SSO,
/// web identity, container and instance metadata).
#[derive(Clone)]
pub struct CloudWatchLogsConfig {
    common: CommonConfig,
    endpoint: Option<url::Url>,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<Secret>,
    session_token: Option<Secret>,
    profile: Option<String>,
    credentials: Option<SharedCredentialsProvider>,
    role_arn: Option<String>,
    role_session_name: String,
    external_id: Option<Secret>,
    log_group: Option<String>,
    log_stream: Option<String>,
    account_id: Option<String>,
    max_event_bytes: usize,
    oversize_policy: OversizePolicy,
    auto_create: bool,
    sdk_client: Option<aws_sdk_cloudwatchlogs::Client>,
}

impl CloudWatchLogsConfig {
    /// A builder with nothing set.
    pub fn builder() -> CloudWatchLogsConfigBuilder {
        CloudWatchLogsConfigBuilder::default()
    }

    /// Configuration from the process environment (explicit > `GROW_AWS_CLOUDWATCH_LOGS_*` >
    /// standard AWS / CloudWatch variables > defaults).
    pub fn from_env() -> Result<Self> {
        Self::builder().build_from_env()
    }

    /// Settings shared by every Grow Codes SDK.
    pub fn common(&self) -> &CommonConfig {
        &self.common
    }

    /// Effective endpoint: the override when set, otherwise the public AWS endpoint of the
    /// region (`us-east-1` when the region is left to the SDK). Informational.
    pub fn url(&self) -> &url::Url {
        self.common.url()
    }

    /// Explicit endpoint override passed to the AWS SDK (`None` = the SDK resolves it).
    pub fn endpoint(&self) -> Option<&url::Url> {
        self.endpoint.as_ref()
    }

    /// AWS region (`None` = the SDK chain: environment, shared config).
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    /// Static access key id.
    pub fn access_key_id(&self) -> Option<&str> {
        self.access_key_id.as_deref()
    }

    /// Static secret access key.
    pub fn secret_access_key(&self) -> Option<&Secret> {
        self.secret_access_key.as_ref()
    }

    /// Session token of temporary credentials.
    pub fn session_token(&self) -> Option<&Secret> {
        self.session_token.as_ref()
    }

    /// Shared config profile.
    pub fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    /// Explicit AWS SDK credentials provider.
    pub fn credentials(&self) -> Option<&SharedCredentialsProvider> {
        self.credentials.as_ref()
    }

    /// Role assumed with STS on top of the base credentials.
    pub fn role_arn(&self) -> Option<&str> {
        self.role_arn.as_deref()
    }

    /// STS session name of the assume-role helper (default `grow-sdk`).
    pub fn role_session_name(&self) -> &str {
        &self.role_session_name
    }

    /// STS external id.
    pub fn external_id(&self) -> Option<&Secret> {
        self.external_id.as_ref()
    }

    /// Default log group of the log client, `search`, `tail`, `list_streams`, queries and
    /// live tail.
    pub fn log_group(&self) -> Option<&str> {
        self.log_group.as_deref()
    }

    /// Stream the log client writes to (`None` here = the client picks
    /// `<hostname>-<pid>-<random6>`).
    pub fn log_stream(&self) -> Option<&str> {
        self.log_stream.as_deref()
    }

    /// AWS account used to build log group ARNs for live tail (`None` = STS
    /// `GetCallerIdentity`, once).
    pub fn account_id(&self) -> Option<&str> {
        self.account_id.as_deref()
    }

    /// Per-event size policy in UTF-8 bytes (default 262,144).
    pub fn max_event_bytes(&self) -> usize {
        self.max_event_bytes
    }

    /// What happens to oversize events (default truncate).
    pub fn oversize_policy(&self) -> OversizePolicy {
        self.oversize_policy
    }

    /// Whether `put_log_events` and the log client create a missing stream (default true).
    pub fn auto_create(&self) -> bool {
        self.auto_create
    }

    /// Caller-supplied AWS SDK client.
    pub fn sdk_client(&self) -> Option<&aws_sdk_cloudwatchlogs::Client> {
        self.sdk_client.as_ref()
    }

    /// Credential values that must never appear in errors, hooks or logs.
    pub fn known_secrets(&self) -> Vec<String> {
        let mut out: Vec<String> = [
            &self.secret_access_key,
            &self.session_token,
            &self.external_id,
        ]
        .into_iter()
        .flatten()
        .map(|s| s.expose_secret().to_owned())
        .collect();
        for (name, value) in self.common.headers() {
            if crate::core::redact::is_sensitive_header(name) {
                out.push(value.clone());
            }
        }
        out.retain(|s| s.chars().count() >= 4);
        out
    }

    /// Checks the service rules (common rules are checked when building); `CONFIG` on failure.
    pub fn validate(&self) -> Result<()> {
        let mut problems: Vec<String> = Vec::new();
        if let Some(region) = &self.region {
            let ok = !region.is_empty()
                && region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
            if !ok {
                problems.push("region must look like us-east-1".into());
            }
        }
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(_), None) | (None, Some(_)) => {
                problems.push("accessKeyId and secretAccessKey must be given together".into())
            }
            _ => {}
        }
        if self.session_token.is_some() && self.access_key_id.is_none() {
            problems.push("sessionToken requires accessKeyId and secretAccessKey".into());
        }
        if self.credentials.is_some() && self.access_key_id.is_some() {
            problems
                .push("credentials cannot be combined with accessKeyId and secretAccessKey".into());
        }
        if self.sdk_client.is_some() {
            let clash: Vec<&str> = [
                ("accessKeyId", self.access_key_id.is_some()),
                ("credentials", self.credentials.is_some()),
                ("profile", self.profile.is_some()),
                ("roleArn", self.role_arn.is_some()),
                ("url", self.endpoint.is_some()),
            ]
            .into_iter()
            .filter_map(|(name, set)| set.then_some(name))
            .collect();
            if !clash.is_empty() {
                problems.push(format!(
                    "sdkClient cannot be combined with {}",
                    clash.join(", ")
                ));
            }
        }
        if let Some(arn) = &self.role_arn {
            if !arn.starts_with("arn:") {
                problems.push("roleArn must be an IAM role ARN".into());
            }
        }
        if !(1..=MAX_EVENT_BYTES_LIMIT).contains(&self.max_event_bytes) {
            problems.push(format!(
                "maxEventBytes must be between 1 and {MAX_EVENT_BYTES_LIMIT}"
            ));
        }
        if let Some(group) = &self.log_group {
            if validate_group_name("config", group).is_err() {
                problems.push("logGroup is not a valid log group name".into());
            }
        }
        if let Some(stream) = &self.log_stream {
            if validate_stream_name("config", stream).is_err() {
                problems.push("logStream is not a valid log stream name".into());
            }
        }
        if let Some(account) = &self.account_id {
            if account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()) {
                problems.push("accountId must be 12 digits".into());
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(Error::config(format!(
                "invalid configuration: {}",
                problems.join("; ")
            )))
        }
    }
}

fn opt_redacted<T>(value: &Option<T>) -> Option<&'static str> {
    value.as_ref().map(|_| REDACTED)
}

/// Keeps the first four characters of an access key id (the key type, e.g. `AKIA`).
pub(crate) fn redact_key_id(id: &str) -> String {
    if id.chars().count() <= 4 {
        return REDACTED.to_owned();
    }
    let head: String = id.chars().take(4).collect();
    format!("{head}{REDACTED}")
}

impl fmt::Debug for CloudWatchLogsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudWatchLogsConfig")
            .field("common", &self.common)
            .field(
                "endpoint",
                &self.endpoint.as_ref().map(|u| redact_url(u.as_str())),
            )
            .field("region", &self.region)
            .field(
                "access_key_id",
                &self.access_key_id.as_deref().map(redact_key_id),
            )
            .field("secret_access_key", &opt_redacted(&self.secret_access_key))
            .field("session_token", &opt_redacted(&self.session_token))
            .field("profile", &self.profile)
            .field("credentials", &opt_redacted(&self.credentials))
            .field("role_arn", &self.role_arn)
            .field("role_session_name", &self.role_session_name)
            .field("external_id", &opt_redacted(&self.external_id))
            .field("log_group", &self.log_group)
            .field("log_stream", &self.log_stream)
            .field("account_id", &self.account_id)
            .field("max_event_bytes", &self.max_event_bytes)
            .field("oversize_policy", &self.oversize_policy)
            .field("auto_create", &self.auto_create)
            .field(
                "sdk_client",
                &self
                    .sdk_client
                    .as_ref()
                    .map(|_| "aws_sdk_cloudwatchlogs::Client"),
            )
            .finish()
    }
}

/// Builder for [`CloudWatchLogsConfig`]. Values set here win over the environment.
#[derive(Clone, Default)]
pub struct CloudWatchLogsConfigBuilder {
    common: CommonConfigBuilder,
    endpoint: Option<String>,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<Secret>,
    session_token: Option<Secret>,
    profile: Option<String>,
    credentials: Option<SharedCredentialsProvider>,
    role_arn: Option<String>,
    role_session_name: Option<String>,
    external_id: Option<Secret>,
    log_group: Option<String>,
    log_stream: Option<String>,
    account_id: Option<String>,
    max_event_bytes: Option<usize>,
    oversize_policy: Option<OversizePolicy>,
    auto_create: Option<bool>,
    sdk_client: Option<aws_sdk_cloudwatchlogs::Client>,
}

impl fmt::Debug for CloudWatchLogsConfigBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudWatchLogsConfigBuilder")
            .field("endpoint", &self.endpoint.as_deref().map(redact_url))
            .field("region", &self.region)
            .field(
                "access_key_id",
                &self.access_key_id.as_deref().map(redact_key_id),
            )
            .field("secret_access_key", &opt_redacted(&self.secret_access_key))
            .field("session_token", &opt_redacted(&self.session_token))
            .field("external_id", &opt_redacted(&self.external_id))
            .field("log_group", &self.log_group)
            .finish_non_exhaustive()
    }
}

fn blank_to_none(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.trim().is_empty())
}

fn secret_to_none(value: Option<Secret>) -> Option<Secret> {
    value.filter(|v| !v.is_empty())
}

impl CloudWatchLogsConfigBuilder {
    /// Endpoint override, e.g. `http://localhost:5000` for an emulator (env
    /// `GROW_AWS_CLOUDWATCH_LOGS_URL` > `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS` > `AWS_ENDPOINT_URL`).
    /// Unset: the AWS SDK resolves `https://logs.<region>.amazonaws.com`.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }
    /// AWS region (env `GROW_AWS_CLOUDWATCH_LOGS_REGION` > `AWS_REGION` > `AWS_DEFAULT_REGION`,
    /// then the profile's region).
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }
    /// Static access key id (env `GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID` > `AWS_ACCESS_KEY_ID`).
    pub fn access_key_id(mut self, id: impl Into<String>) -> Self {
        self.access_key_id = Some(id.into());
        self
    }
    /// Static secret access key (env `GROW_AWS_CLOUDWATCH_LOGS_SECRET_ACCESS_KEY` >
    /// `AWS_SECRET_ACCESS_KEY`).
    pub fn secret_access_key(mut self, key: impl Into<Secret>) -> Self {
        self.secret_access_key = Some(key.into());
        self
    }
    /// Session token of temporary credentials (env `GROW_AWS_CLOUDWATCH_LOGS_SESSION_TOKEN` >
    /// `AWS_SESSION_TOKEN`).
    pub fn session_token(mut self, token: impl Into<Secret>) -> Self {
        self.session_token = Some(token.into());
        self
    }
    /// Shared config/credentials profile (env `GROW_AWS_CLOUDWATCH_LOGS_PROFILE` > `AWS_PROFILE`).
    pub fn profile(mut self, profile: impl Into<String>) -> Self {
        self.profile = Some(profile.into());
        self
    }
    /// An AWS SDK credentials provider used instead of the key fields and the default chain.
    pub fn credentials_provider(mut self, provider: impl ProvideCredentials + 'static) -> Self {
        self.credentials = Some(SharedCredentialsProvider::new(provider));
        self
    }
    /// Role assumed with STS on top of the base credentials (env
    /// `GROW_AWS_CLOUDWATCH_LOGS_ROLE_ARN`; `AWS_ROLE_ARN` stays with the SDK web-identity chain).
    pub fn role_arn(mut self, arn: impl Into<String>) -> Self {
        self.role_arn = Some(arn.into());
        self
    }
    /// STS session name (default `grow-sdk`, env `GROW_AWS_CLOUDWATCH_LOGS_ROLE_SESSION_NAME`).
    pub fn role_session_name(mut self, name: impl Into<String>) -> Self {
        self.role_session_name = Some(name.into());
        self
    }
    /// STS external id (env `GROW_AWS_CLOUDWATCH_LOGS_EXTERNAL_ID`).
    pub fn external_id(mut self, id: impl Into<Secret>) -> Self {
        self.external_id = Some(id.into());
        self
    }
    /// Default log group (env `GROW_AWS_CLOUDWATCH_LOGS_LOG_GROUP` > `CLOUDWATCH_LOG_GROUP`).
    pub fn log_group(mut self, group: impl Into<String>) -> Self {
        self.log_group = Some(group.into());
        self
    }
    /// Stream the log client writes to (env `GROW_AWS_CLOUDWATCH_LOGS_LOG_STREAM` >
    /// `CLOUDWATCH_LOG_STREAM`; default `<hostname>-<pid>-<random6>`, created on demand).
    pub fn log_stream(mut self, stream: impl Into<String>) -> Self {
        self.log_stream = Some(stream.into());
        self
    }
    /// AWS account id for live-tail ARNs (env `GROW_AWS_CLOUDWATCH_LOGS_ACCOUNT_ID`; default:
    /// STS `GetCallerIdentity`).
    pub fn account_id(mut self, account: impl Into<String>) -> Self {
        self.account_id = Some(account.into());
        self
    }
    /// Per-event size policy in UTF-8 bytes, 1 to [`MAX_EVENT_BYTES_LIMIT`] (default 262,144,
    /// env `GROW_AWS_CLOUDWATCH_LOGS_MAX_EVENT_BYTES`).
    pub fn max_event_bytes(mut self, bytes: usize) -> Self {
        self.max_event_bytes = Some(bytes);
        self
    }
    /// What happens to oversize events (default truncate, env
    /// `GROW_AWS_CLOUDWATCH_LOGS_OVERSIZE_POLICY`).
    pub fn oversize_policy(mut self, policy: OversizePolicy) -> Self {
        self.oversize_policy = Some(policy);
        self
    }
    /// Whether a missing stream is created by `put_log_events` and the log client (default
    /// true, env `GROW_AWS_CLOUDWATCH_LOGS_AUTO_CREATE`).
    pub fn auto_create(mut self, on: bool) -> Self {
        self.auto_create = Some(on);
        self
    }
    /// An existing AWS SDK client used instead of building one. Mutually exclusive with the
    /// credential, profile, role and endpoint settings; `close()` never closes it and the
    /// Grow hooks, TLS, proxy, headers and User-Agent settings do not apply to it.
    pub fn sdk_client(mut self, client: aws_sdk_cloudwatchlogs::Client) -> Self {
        self.sdk_client = Some(client);
        self
    }
    /// Connect timeout (default 10 s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.connect_timeout(timeout);
        self
    }
    /// Per-attempt timeout (default 30 s; SDK `operation_attempt_timeout`).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.request_timeout(timeout);
        self
    }
    /// Whole-call timeout including retries (default 120 s; SDK `operation_timeout`).
    pub fn total_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.total_timeout(timeout);
        self
    }
    /// Retry policy (SDK standard retry mode with `max_attempts = max_retries + 1`).
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.common = self.common.retry(retry);
        self
    }
    /// Maximum retries (default 3).
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.common = self.common.max_retries(max_retries);
        self
    }
    /// TLS settings.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.common = self.common.tls(tls);
        self
    }
    /// Extra trusted CA certificates (PEM file).
    pub fn ca_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.common = self.common.ca_cert_path(path);
        self
    }
    /// Disables TLS verification (tests only).
    pub fn insecure_skip_verify(mut self, skip: bool) -> Self {
        self.common = self.common.insecure_skip_verify(skip);
        self
    }
    /// Proxy URL (environment proxies are ignored).
    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.common = self.common.proxy(proxy);
        self
    }
    /// Extra request header (added after signing, so it is not signed).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.common = self.common.header(name, value);
        self
    }
    /// Text appended to the User-Agent.
    pub fn user_agent_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.common = self.common.user_agent_suffix(suffix);
        self
    }
    /// Request hooks (fired from an AWS SDK interceptor for every attempt).
    pub fn hooks(mut self, hooks: Arc<dyn Hooks>) -> Self {
        self.common = self.common.hooks(hooks);
        self
    }
    /// Edits any other common setting (use [`CloudWatchLogsConfigBuilder::url`] for the
    /// endpoint).
    pub fn common(mut self, f: impl FnOnce(CommonConfigBuilder) -> CommonConfigBuilder) -> Self {
        self.common = f(self.common);
        self
    }

    /// Builds without reading the environment.
    pub fn build(self) -> Result<CloudWatchLogsConfig> {
        let endpoint_raw = blank_to_none(self.endpoint);
        let region = blank_to_none(self.region);
        let mut common = self.common;
        let fallback = format!(
            "https://logs.{}.amazonaws.com",
            region.as_deref().unwrap_or("us-east-1")
        );
        if let Some(raw) = &endpoint_raw {
            common = common.url(raw.clone());
        }
        let common = common.build(&fallback)?;
        let endpoint = endpoint_raw.map(|_| common.url().clone());
        let config = CloudWatchLogsConfig {
            common,
            endpoint,
            region,
            access_key_id: blank_to_none(self.access_key_id),
            secret_access_key: secret_to_none(self.secret_access_key),
            session_token: secret_to_none(self.session_token),
            profile: blank_to_none(self.profile),
            credentials: self.credentials,
            role_arn: blank_to_none(self.role_arn),
            role_session_name: blank_to_none(self.role_session_name)
                .unwrap_or_else(|| DEFAULT_ROLE_SESSION_NAME.to_owned()),
            external_id: secret_to_none(self.external_id),
            log_group: blank_to_none(self.log_group),
            log_stream: blank_to_none(self.log_stream),
            account_id: blank_to_none(self.account_id),
            max_event_bytes: self.max_event_bytes.unwrap_or(DEFAULT_MAX_EVENT_BYTES),
            oversize_policy: self.oversize_policy.unwrap_or_default(),
            auto_create: self.auto_create.unwrap_or(true),
            sdk_client: self.sdk_client,
        };
        config.validate()?;
        Ok(config)
    }

    /// Fills unset values from the process environment, then builds.
    pub fn build_from_env(self) -> Result<CloudWatchLogsConfig> {
        self.build_with_env(&EnvReader::new())
    }

    /// Fills unset values from `env`, then builds. Credentials are taken as one set: a
    /// `GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID` never pairs with an `AWS_SECRET_ACCESS_KEY`.
    /// With an explicit [`sdk_client`](Self::sdk_client) the credential, profile, role and
    /// endpoint variables are not read.
    pub fn build_with_env(mut self, env: &EnvReader) -> Result<CloudWatchLogsConfig> {
        let with_client = self.sdk_client.is_some();
        if self.endpoint.is_none() && !with_client {
            self.endpoint = env.string("URL", VENDOR_URL_VARS);
        }
        // URL handled above: nothing for the common reader to add.
        self.common = self.common.apply_env(env, &[])?;
        if self.region.is_none() {
            self.region = env.string("REGION", VENDOR_REGION_VARS);
        }
        if !with_client {
            let explicit_keys = self.access_key_id.is_some()
                || self.secret_access_key.is_some()
                || self.session_token.is_some();
            if self.credentials.is_none() && !explicit_keys {
                if let Some(id) = env.string("ACCESS_KEY_ID", &[]) {
                    self.access_key_id = Some(id);
                    self.secret_access_key = env.secret("SECRET_ACCESS_KEY", &[]);
                    self.session_token = env.secret("SESSION_TOKEN", &[]);
                } else {
                    let vendor = env.clone().with_prefix("");
                    self.access_key_id = vendor.string("AWS_ACCESS_KEY_ID", &[]);
                    self.secret_access_key = vendor.secret("AWS_SECRET_ACCESS_KEY", &[]);
                    self.session_token = vendor.secret("AWS_SESSION_TOKEN", &[]);
                }
            }
            if self.profile.is_none() {
                self.profile = env.string("PROFILE", VENDOR_PROFILE_VARS);
            }
            if self.role_arn.is_none() {
                self.role_arn = env.string("ROLE_ARN", &[]);
            }
            if self.role_session_name.is_none() {
                self.role_session_name = env.string("ROLE_SESSION_NAME", &[]);
            }
            if self.external_id.is_none() {
                self.external_id = env.secret("EXTERNAL_ID", &[]);
            }
        }
        if self.log_group.is_none() {
            self.log_group = env.string("LOG_GROUP", VENDOR_LOG_GROUP_VARS);
        }
        if self.log_stream.is_none() {
            self.log_stream = env.string("LOG_STREAM", VENDOR_LOG_STREAM_VARS);
        }
        if self.account_id.is_none() {
            self.account_id = env.string("ACCOUNT_ID", &[]);
        }
        if self.max_event_bytes.is_none() {
            if let Some(n) = env.int("MAX_EVENT_BYTES", &[])? {
                let n = usize::try_from(n).map_err(|_| {
                    Error::config(format!(
                        "environment variable {}MAX_EVENT_BYTES must be between 1 and {MAX_EVENT_BYTES_LIMIT}",
                        env.prefix()
                    ))
                })?;
                self.max_event_bytes = Some(n);
            }
        }
        if self.oversize_policy.is_none() {
            if let Some(v) = env.string("OVERSIZE_POLICY", &[]) {
                self.oversize_policy = Some(v.parse()?);
            }
        }
        if self.auto_create.is_none() {
            self.auto_create = env.bool("AUTO_CREATE", &[])?;
        }
        self.build()
    }
}
