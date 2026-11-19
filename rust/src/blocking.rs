// Service-owned (created from the sdk-kit 0.1.0 skeleton).
//! Blocking wrapper (feature `blocking`). Each client owns a multi-thread tokio runtime with one
//! worker, so background work (log batching, live-tail sessions) keeps running between calls.
//!
//! Do not use it from inside an async runtime (calls would panic); use the async client there.
//! Iterators of the async client return every item as a `Vec` here. For the logging category
//! (log client, query client) use [`CloudWatchLogsClient::block_on`] with the async API.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use crate::core::{Error, HealthStatus, Result};
use crate::models::*;
use crate::{CloudWatchLogsClient as AsyncClient, CloudWatchLogsConfig, EmfOptions};

/// Blocking client for Amazon CloudWatch Logs with the same methods as the async client.
#[derive(Debug)]
pub struct CloudWatchLogsClient {
    inner: AsyncClient,
    runtime: tokio::runtime::Runtime,
}

impl CloudWatchLogsClient {
    /// Creates a client from a configuration.
    pub fn new(config: CloudWatchLogsConfig) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| Error::config("cannot start the tokio runtime").with_source(e))?;
        let inner = {
            let _guard = runtime.enter();
            AsyncClient::new(config)?
        };
        Ok(CloudWatchLogsClient { inner, runtime })
    }

    /// Creates a client from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::new(CloudWatchLogsConfig::from_env()?)
    }

    /// The wrapped async client.
    pub fn async_client(&self) -> &AsyncClient {
        &self.inner
    }

    /// Runs a future of the async API to completion on this client's runtime.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// See [`AsyncClient::health`].
    pub fn health(&self) -> Result<HealthStatus> {
        self.block_on(self.inner.health())
    }

    /// See [`AsyncClient::ping`].
    pub fn ping(&self) -> Result<Duration> {
        self.block_on(self.inner.ping())
    }

    /// See [`AsyncClient::server_version`].
    pub fn server_version(&self) -> Result<String> {
        self.block_on(self.inner.server_version())
    }

    /// See [`AsyncClient::close`]. Idempotent.
    pub fn close(&self) {
        self.block_on(self.inner.close());
    }

    /// See [`AsyncClient::ensure_log_group`].
    pub fn ensure_log_group(
        &self,
        name: &str,
        opts: EnsureLogGroupOptions,
    ) -> Result<EnsureLogGroupResult> {
        self.block_on(self.inner.ensure_log_group(name, opts))
    }

    /// See [`AsyncClient::get_log_group`].
    pub fn get_log_group(&self, name: &str) -> Result<Option<LogGroup>> {
        self.block_on(self.inner.get_log_group(name))
    }

    /// See [`AsyncClient::ensure_log_stream`].
    pub fn ensure_log_stream(
        &self,
        group: &str,
        stream: &str,
        opts: EnsureLogStreamOptions,
    ) -> Result<EnsureLogStreamResult> {
        self.block_on(self.inner.ensure_log_stream(group, stream, opts))
    }

    /// See [`AsyncClient::put_retention_policy`].
    pub fn put_retention_policy(&self, group: &str, days: Option<u32>) -> Result<()> {
        self.block_on(self.inner.put_retention_policy(group, days))
    }

    /// See [`AsyncClient::delete_log_group`].
    pub fn delete_log_group(&self, name: &str, opts: DeleteOptions) -> Result<bool> {
        self.block_on(self.inner.delete_log_group(name, opts))
    }

    /// See [`AsyncClient::delete_log_stream`].
    pub fn delete_log_stream(
        &self,
        group: &str,
        stream: &str,
        opts: DeleteOptions,
    ) -> Result<bool> {
        self.block_on(self.inner.delete_log_stream(group, stream, opts))
    }

    /// See [`AsyncClient::describe_log_groups`] (every page).
    pub fn describe_log_groups(&self, opts: DescribeLogGroupsOptions) -> Result<Vec<LogGroup>> {
        self.block_on(self.inner.describe_log_groups(opts).try_collect())
    }

    /// See [`AsyncClient::describe_log_streams`] (every page).
    pub fn describe_log_streams(
        &self,
        group: &str,
        opts: DescribeLogStreamsOptions,
    ) -> Result<Vec<LogStream>> {
        self.block_on(self.inner.describe_log_streams(group, opts).try_collect())
    }

    /// See [`AsyncClient::tag_log_group`].
    pub fn tag_log_group(&self, group: &str, tags: BTreeMap<String, String>) -> Result<()> {
        self.block_on(self.inner.tag_log_group(group, tags))
    }

    /// See [`AsyncClient::untag_log_group`].
    pub fn untag_log_group(&self, group: &str, keys: Vec<String>) -> Result<()> {
        self.block_on(self.inner.untag_log_group(group, keys))
    }

    /// See [`AsyncClient::list_log_group_tags`].
    pub fn list_log_group_tags(&self, group: &str) -> Result<BTreeMap<String, String>> {
        self.block_on(self.inner.list_log_group_tags(group))
    }

    /// See [`AsyncClient::put_log_events`].
    pub fn put_log_events(
        &self,
        group: &str,
        stream: &str,
        events: Vec<LogEvent>,
        opts: PutLogEventsOptions,
    ) -> Result<PutLogEventsResult> {
        self.block_on(self.inner.put_log_events(group, stream, events, opts))
    }

    /// See [`AsyncClient::emf`].
    pub fn emf(
        &self,
        metric: &str,
        value: f64,
        dimensions: &BTreeMap<String, String>,
        opts: EmfOptions,
    ) -> Result<PutLogEventsResult> {
        self.block_on(self.inner.emf(metric, value, dimensions, opts))
    }

    /// See [`AsyncClient::filter_log_events`] (every page, up to `opts.limit`).
    pub fn filter_log_events(&self, opts: FilterLogEventsOptions) -> Result<Vec<FilteredLogEvent>> {
        self.block_on(self.inner.filter_log_events(opts).try_collect())
    }

    /// See [`AsyncClient::filter_log_events_page`].
    pub fn filter_log_events_page(
        &self,
        opts: FilterLogEventsOptions,
        next_token: Option<&str>,
    ) -> Result<FilterLogEventsPage> {
        self.block_on(self.inner.filter_log_events_page(opts, next_token))
    }

    /// See [`AsyncClient::get_log_events`] (every page, up to `opts.limit`; set `end` or a
    /// limit on live streams).
    pub fn get_log_events(
        &self,
        group: &str,
        stream: &str,
        opts: GetLogEventsOptions,
    ) -> Result<Vec<StreamLogEvent>> {
        self.block_on(self.inner.get_log_events(group, stream, opts).try_collect())
    }

    /// See [`AsyncClient::start_query`].
    pub fn start_query(&self, query: &str, opts: QueryOptions) -> Result<String> {
        self.block_on(self.inner.start_query(query, opts))
    }

    /// See [`AsyncClient::get_query_results`].
    pub fn get_query_results(&self, query_id: &str) -> Result<QueryResults> {
        self.block_on(self.inner.get_query_results(query_id))
    }

    /// See [`AsyncClient::query`].
    pub fn query(&self, query: &str, opts: QueryOptions) -> Result<QueryResults> {
        self.block_on(self.inner.query(query, opts))
    }

    /// See [`AsyncClient::stop_query`].
    pub fn stop_query(&self, query_id: &str) -> Result<bool> {
        self.block_on(self.inner.stop_query(query_id))
    }

    /// See [`AsyncClient::describe_queries`].
    pub fn describe_queries(&self, opts: DescribeQueriesOptions) -> Result<Vec<QueryInfo>> {
        self.block_on(self.inner.describe_queries(opts))
    }

    /// See [`AsyncClient::log_group_arn`].
    pub fn log_group_arn(&self, group: &str) -> Result<String> {
        self.block_on(self.inner.log_group_arn(group))
    }

    /// See [`AsyncClient::start_live_tail`]. The callback runs on this client's runtime.
    pub fn start_live_tail(
        &self,
        opts: LiveTailOptions,
        on_event: LiveTailCallback,
    ) -> Result<crate::LiveTailSession> {
        self.block_on(self.inner.start_live_tail(opts, on_event))
    }
}

impl Drop for CloudWatchLogsClient {
    fn drop(&mut self) {
        self.inner.shutdown();
    }
}
