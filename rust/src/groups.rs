//! Bootstrap functions: log groups, log streams, retention, tags.

use std::collections::{BTreeMap, HashMap};

use futures::FutureExt;

use crate::client::CloudWatchLogsClient;
use crate::core::{Error, ErrorCode, Result};
use crate::models::{
    clamp_i32, trim_star_arn, validate_group_name, validate_page_size, validate_retention,
    validate_stream_name, DeleteOptions, DescribeLogGroupsOptions, DescribeLogStreamsOptions,
    EnsureLogGroupOptions, EnsureLogGroupResult, EnsureLogStreamOptions, EnsureLogStreamResult,
    LogGroup, LogStream, StreamOrder, MAX_DESCRIBE_PAGE_SIZE,
};
use crate::paginator::Paginator;

/// Compares a group's key ARN with a requested key id, ARN or alias.
pub(crate) fn same_kms_key(existing: Option<&str>, requested: &str) -> bool {
    match existing {
        Some(e) if e == requested => true,
        Some(e) if !e.is_empty() => {
            e.ends_with(&format!("/{requested}")) || e.ends_with(&format!(":{requested}"))
        }
        _ => false,
    }
}

fn delete_result(result: Result<()>, opts: DeleteOptions) -> Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(e) if opts.ignore_missing && e.code() == ErrorCode::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

impl CloudWatchLogsClient {
    /// Creates the log group when missing. On an existing group it adds the requested tags
    /// (never removes any), re-applies the retention and checks the KMS key (`CONFLICT` when it
    /// differs). `retention_days`: `None` leaves the retention unchanged, `Some(0)` removes it
    /// (never expire). Invalid names and retention values are `VALIDATION` errors.
    pub async fn ensure_log_group(
        &self,
        name: &str,
        opts: EnsureLogGroupOptions,
    ) -> Result<EnsureLogGroupResult> {
        let op = format!("ensureLogGroup {name}");
        validate_group_name(&op, name)?;
        validate_retention(&op, opts.retention_days.unwrap_or(0))?;
        let sdk = self.logs(&op).await?;
        let tags: HashMap<String, String> = opts
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let call = sdk
            .create_log_group()
            .log_group_name(name)
            .set_kms_key_id(opts.kms_key_id.clone())
            .set_tags((!tags.is_empty()).then(|| tags.clone()))
            .send();
        let created = match self.run(&op, call).await {
            Ok(_) => true,
            Err(e) if e.code() == ErrorCode::Conflict => false,
            Err(e) => return Err(e),
        };
        let group = self.find_group(&op, name).await?;
        let result = EnsureLogGroupResult {
            created,
            arn: group.arn.clone(),
        };
        if !created {
            if let Some(key) = &opts.kms_key_id {
                if !same_kms_key(group.kms_key_id.as_deref(), key) {
                    return Err(Error::conflict(format!(
                        "{op}: the existing log group uses a different KMS key"
                    )));
                }
            }
            if !tags.is_empty() {
                let call = sdk
                    .tag_resource()
                    .resource_arn(&group.arn)
                    .set_tags(Some(tags))
                    .send();
                self.run(&op, call).await?;
            }
        }
        match opts.retention_days {
            None => {}
            Some(0) => {
                let call = sdk.delete_retention_policy().log_group_name(name).send();
                self.run(&op, call).await?;
            }
            Some(days) => {
                let call = sdk
                    .put_retention_policy()
                    .log_group_name(name)
                    .retention_in_days(days as i32)
                    .send();
                self.run(&op, call).await?;
            }
        }
        Ok(result)
    }

    /// The group named exactly `name` (`NOT_FOUND` otherwise).
    pub(crate) async fn find_group(&self, op: &str, name: &str) -> Result<LogGroup> {
        let sdk = self.logs(op).await?;
        let mut token: Option<String> = None;
        loop {
            let call = sdk
                .describe_log_groups()
                .log_group_name_prefix(name)
                .set_next_token(token.clone())
                .send();
            let out = self.run(op, call).await?;
            if let Some(g) = out
                .log_groups()
                .iter()
                .find(|g| g.log_group_name() == Some(name))
            {
                return Ok(LogGroup::from_sdk(g));
            }
            match out.next_token() {
                Some(next) if !next.is_empty() && Some(next) != token.as_deref() => {
                    token = Some(next.to_owned());
                }
                _ => return Err(Error::not_found(format!("{op}: log group does not exist"))),
            }
        }
    }

    /// The group named exactly `name`, or `None` when it does not exist.
    pub async fn get_log_group(&self, name: &str) -> Result<Option<LogGroup>> {
        let op = format!("getLogGroup {name}");
        validate_group_name(&op, name)?;
        match self.find_group(&op, name).await {
            Ok(g) => Ok(Some(g)),
            Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Creates the stream when missing. A missing group is `NOT_FOUND` unless
    /// `opts.create_group` is set.
    pub async fn ensure_log_stream(
        &self,
        group: &str,
        stream: &str,
        opts: EnsureLogStreamOptions,
    ) -> Result<EnsureLogStreamResult> {
        let op = format!("ensureLogStream {group}/{stream}");
        validate_group_name(&op, group)?;
        validate_stream_name(&op, stream)?;
        if opts.create_group {
            self.ensure_log_group(group, EnsureLogGroupOptions::default())
                .await?;
        }
        let sdk = self.logs(&op).await?;
        let call = sdk
            .create_log_stream()
            .log_group_name(group)
            .log_stream_name(stream)
            .send();
        match self.run(&op, call).await {
            Ok(_) => Ok(EnsureLogStreamResult { created: true }),
            Err(e) if e.code() == ErrorCode::Conflict => {
                Ok(EnsureLogStreamResult { created: false })
            }
            Err(e) => Err(e),
        }
    }

    /// Sets the retention of a group in days (one of
    /// [`RETENTION_DAYS`](crate::RETENTION_DAYS)); `None` or `Some(0)` removes the policy
    /// (events never expire).
    pub async fn put_retention_policy(&self, group: &str, days: Option<u32>) -> Result<()> {
        let op = format!("putRetentionPolicy {group}");
        validate_group_name(&op, group)?;
        let days = days.unwrap_or(0);
        validate_retention(&op, days)?;
        let sdk = self.logs(&op).await?;
        if days == 0 {
            let call = sdk.delete_retention_policy().log_group_name(group).send();
            self.run(&op, call).await?;
        } else {
            let call = sdk
                .put_retention_policy()
                .log_group_name(group)
                .retention_in_days(days as i32)
                .send();
            self.run(&op, call).await?;
        }
        Ok(())
    }

    /// Deletes a group with all its streams and events; `true` when something was deleted.
    pub async fn delete_log_group(&self, name: &str, opts: DeleteOptions) -> Result<bool> {
        let op = format!("deleteLogGroup {name}");
        validate_group_name(&op, name)?;
        let sdk = self.logs(&op).await?;
        let call = sdk.delete_log_group().log_group_name(name).send();
        delete_result(self.run(&op, call).await.map(|_| ()), opts)
    }

    /// Deletes a stream and its events; `true` when something was deleted.
    pub async fn delete_log_stream(
        &self,
        group: &str,
        stream: &str,
        opts: DeleteOptions,
    ) -> Result<bool> {
        let op = format!("deleteLogStream {group}/{stream}");
        validate_group_name(&op, group)?;
        validate_stream_name(&op, stream)?;
        let sdk = self.logs(&op).await?;
        let call = sdk
            .delete_log_stream()
            .log_group_name(group)
            .log_stream_name(stream)
            .send();
        delete_result(self.run(&op, call).await.map(|_| ()), opts)
    }

    /// Lazily iterates over the log groups (sorted by name) across all pages.
    pub fn describe_log_groups(&self, opts: DescribeLogGroupsOptions) -> Paginator<LogGroup> {
        let op = "describeLogGroups".to_owned();
        if let Err(e) = validate_page_size(&op, opts.page_size, MAX_DESCRIBE_PAGE_SIZE) {
            return Paginator::failed(e);
        }
        let client = self.clone();
        Paginator::new(opts.limit, move |token, _| {
            let client = client.clone();
            let op = op.clone();
            let prefix = opts.prefix.clone();
            let page_size = opts.page_size;
            async move {
                let sdk = client.logs(&op).await?;
                let call = sdk
                    .describe_log_groups()
                    .set_log_group_name_prefix(prefix)
                    .set_limit(page_size.map(clamp_i32))
                    .set_next_token(token)
                    .send();
                let out = client.run(&op, call).await?;
                let groups = out.log_groups().iter().map(LogGroup::from_sdk).collect();
                Ok((groups, out.next_token().map(str::to_owned)))
            }
            .boxed()
        })
    }

    /// Lazily iterates over the streams of a group across all pages.
    pub fn describe_log_streams(
        &self,
        group: &str,
        opts: DescribeLogStreamsOptions,
    ) -> Paginator<LogStream> {
        let op = format!("describeLogStreams {group}");
        let checks = validate_group_name(&op, group)
            .and_then(|_| validate_page_size(&op, opts.page_size, MAX_DESCRIBE_PAGE_SIZE))
            .and_then(|_| {
                if opts.order_by == StreamOrder::LastEventTime && opts.prefix.is_some() {
                    Err(Error::validation(format!(
                        "{op}: prefix cannot be combined with orderBy LastEventTime"
                    )))
                } else {
                    Ok(())
                }
            });
        if let Err(e) = checks {
            return Paginator::failed(e);
        }
        let order = match opts.order_by {
            StreamOrder::LogStreamName => aws_sdk_cloudwatchlogs::types::OrderBy::LogStreamName,
            StreamOrder::LastEventTime => aws_sdk_cloudwatchlogs::types::OrderBy::LastEventTime,
        };
        let client = self.clone();
        let group = group.to_owned();
        Paginator::new(opts.limit, move |token, _| {
            let client = client.clone();
            let op = op.clone();
            let group = group.clone();
            let prefix = opts.prefix.clone();
            let order = order.clone();
            let descending = opts.descending;
            let page_size = opts.page_size;
            async move {
                let sdk = client.logs(&op).await?;
                let call = sdk
                    .describe_log_streams()
                    .log_group_name(group)
                    .set_log_stream_name_prefix(prefix)
                    .order_by(order)
                    .set_descending(descending.then_some(true))
                    .set_limit(page_size.map(clamp_i32))
                    .set_next_token(token)
                    .send();
                let out = client.run(&op, call).await?;
                let streams = out.log_streams().iter().map(LogStream::from_sdk).collect();
                Ok((streams, out.next_token().map(str::to_owned)))
            }
            .boxed()
        })
    }

    async fn resolve_group_arn(&self, op: &str, group: &str) -> Result<String> {
        if group.starts_with("arn:") {
            return Ok(trim_star_arn(group).to_owned());
        }
        validate_group_name(op, group)?;
        Ok(self.find_group(op, group).await?.arn)
    }

    /// Adds or overwrites tags on a group given by name or ARN.
    pub async fn tag_log_group(&self, group: &str, tags: BTreeMap<String, String>) -> Result<()> {
        let op = format!("tagLogGroup {group}");
        if tags.is_empty() {
            return Err(Error::validation(format!(
                "{op}: at least one tag is required"
            )));
        }
        let arn = self.resolve_group_arn(&op, group).await?;
        let sdk = self.logs(&op).await?;
        let call = sdk
            .tag_resource()
            .resource_arn(arn)
            .set_tags(Some(tags.into_iter().collect()))
            .send();
        self.run(&op, call).await.map(|_| ())
    }

    /// Removes tag keys from a group given by name or ARN.
    pub async fn untag_log_group<I, S>(&self, group: &str, keys: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let op = format!("untagLogGroup {group}");
        let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
        if keys.is_empty() {
            return Err(Error::validation(format!(
                "{op}: at least one tag key is required"
            )));
        }
        let arn = self.resolve_group_arn(&op, group).await?;
        let sdk = self.logs(&op).await?;
        let call = sdk
            .untag_resource()
            .resource_arn(arn)
            .set_tag_keys(Some(keys))
            .send();
        self.run(&op, call).await.map(|_| ())
    }

    /// The tags of a group given by name or ARN.
    pub async fn list_log_group_tags(&self, group: &str) -> Result<BTreeMap<String, String>> {
        let op = format!("listLogGroupTags {group}");
        let arn = self.resolve_group_arn(&op, group).await?;
        let sdk = self.logs(&op).await?;
        let call = sdk.list_tags_for_resource().resource_arn(arn).send();
        let out = self.run(&op, call).await?;
        Ok(out
            .tags()
            .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kms_comparison() {
        let arn = "arn:aws:kms:us-east-1:123456789012:key/k1";
        assert!(same_kms_key(Some(arn), arn));
        assert!(same_kms_key(Some(arn), "k1"));
        assert!(!same_kms_key(Some(arn), "k2"));
        assert!(!same_kms_key(None, "k1"));
        assert!(!same_kms_key(Some(""), "k1"));
        assert!(same_kms_key(Some("arn:aws:kms:r:1:alias/a"), "alias/a"));
    }
}
