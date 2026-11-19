//! Live tail: `StartLiveTail` sessions (event stream).

use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use aws_sdk_cloudwatchlogs::types::{LiveTailSessionUpdate, StartLiveTailResponseStream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::aws_error::map_stream_error;
use crate::client::{lock, CloudWatchLogsClient};
use crate::core::{Error, ErrorCode, Result};
use crate::models::{
    ms_time, LiveTailCallback, LiveTailEvent, LiveTailOptions, LiveTailUpdate, MAX_LIVE_TAIL_GROUPS,
};

type Receiver = aws_sdk_cloudwatchlogs::primitives::event_stream::EventReceiver<
    StartLiveTailResponseStream,
    aws_sdk_cloudwatchlogs::types::error::StartLiveTailResponseStreamError,
>;

/// Shared state of a running session.
pub(crate) struct SessionControl {
    stop: CancellationToken,
    done: AtomicBool,
    done_notify: Notify,
    error: Mutex<Option<Error>>,
}

impl SessionControl {
    fn new() -> Self {
        SessionControl {
            stop: CancellationToken::new(),
            done: AtomicBool::new(false),
            done_notify: Notify::new(),
            error: Mutex::new(None),
        }
    }

    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }

    fn finish(&self) {
        self.done.store(true, Ordering::SeqCst);
        self.done_notify.notify_waiters();
    }
}

/// A running live-tail session. AWS streams events ingested after the session started, about
/// once per second, for up to 3 h. Dropping the handle does **not** stop the session; call
/// [`close`](LiveTailSession::close) (or close the client).
#[derive(Clone)]
pub struct LiveTailSession {
    control: Arc<SessionControl>,
    session_id: String,
}

impl fmt::Debug for LiveTailSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveTailSession")
            .field("session_id", &self.session_id)
            .field("done", &self.is_done())
            .finish()
    }
}

impl LiveTailSession {
    /// Id from the service's `sessionStart` frame.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Stops the session. Idempotent and safe to call from the event callback; await
    /// [`done`](LiveTailSession::done) to know when the last event was delivered.
    pub fn close(&self) {
        self.control.stop();
    }

    /// Whether the session has ended (service closed it, error or `close`).
    pub fn is_done(&self) -> bool {
        self.control.done.load(Ordering::SeqCst)
    }

    /// Resolves once the session has ended.
    pub async fn done(&self) {
        loop {
            let notified = self.control.done_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_done() {
                return;
            }
            notified.await;
        }
    }

    /// The error that ended the session (`None` after a normal end or `close`).
    /// `SessionStreamingException` gives `UNAVAILABLE` and `SessionTimeoutException` `TIMEOUT`
    /// (both retryable: start a new session).
    pub fn error(&self) -> Option<Error> {
        lock(&self.control.error).clone()
    }
}

fn to_events(update: &LiveTailSessionUpdate) -> (bool, Vec<LiveTailEvent>) {
    let sampled = update.session_metadata().is_some_and(|m| m.sampled());
    let events = update
        .session_results()
        .iter()
        .map(|r| LiveTailEvent {
            message: r.message().unwrap_or_default().to_owned(),
            timestamp: ms_time(r.timestamp()).unwrap_or(UNIX_EPOCH),
            ingestion_time: ms_time(r.ingestion_time()),
            log_stream_name: r.log_stream_name().unwrap_or_default().to_owned(),
            log_group_identifier: r.log_group_identifier().unwrap_or_default().to_owned(),
            sampled,
        })
        .collect();
    (sampled, events)
}

fn safe_call(f: impl FnOnce()) {
    let _ = std::panic::catch_unwind(AssertUnwindSafe(f));
}

impl CloudWatchLogsClient {
    /// Starts a live-tail session and calls `on_event` (from one task, in order) for every
    /// event until the session ends, [`LiveTailSession::close`] is called or the client is
    /// closed. Returns after the service confirmed the session (`sessionStart`). Group names
    /// become ARNs with the client's region and account (configuration or STS).
    pub async fn start_live_tail(
        &self,
        opts: LiveTailOptions,
        on_event: LiveTailCallback,
    ) -> Result<LiveTailSession> {
        let op = "startLiveTail";
        let mut groups = opts.groups.clone();
        if groups.is_empty() {
            if let Some(g) = self.config().log_group() {
                groups.push(g.to_owned());
            }
        }
        if groups.is_empty() {
            return Err(Error::config(format!(
                "{op}: no log groups given and logGroup is not set"
            )));
        }
        if groups.len() > MAX_LIVE_TAIL_GROUPS {
            return Err(Error::validation(format!("{op}: at most 10 log groups")));
        }
        if !opts.streams.is_empty() && !opts.stream_prefixes.is_empty() {
            return Err(Error::validation(format!(
                "{op}: streams and streamPrefixes are mutually exclusive"
            )));
        }
        if (!opts.streams.is_empty() || !opts.stream_prefixes.is_empty()) && groups.len() != 1 {
            return Err(Error::validation(format!(
                "{op}: streams and streamPrefixes need exactly one log group"
            )));
        }
        let setup = async {
            let mut arns = Vec::with_capacity(groups.len());
            for g in &groups {
                arns.push(self.group_arn(op, g).await?);
            }
            let sdk = self.logs(op).await?;
            let call = sdk
                .start_live_tail()
                .set_log_group_identifiers(Some(arns))
                .set_log_stream_names((!opts.streams.is_empty()).then(|| opts.streams.clone()))
                .set_log_stream_name_prefixes(
                    (!opts.stream_prefixes.is_empty()).then(|| opts.stream_prefixes.clone()),
                )
                .set_log_event_filter_pattern(opts.pattern.clone().filter(|p| !p.is_empty()))
                .send();
            let out = self.run(op, call).await?;
            let mut receiver = out.response_stream;
            // Wait for sessionStart (the first frame) before returning.
            loop {
                match receiver.recv().await {
                    Ok(Some(StartLiveTailResponseStream::SessionStart(start))) => {
                        let id = start.session_id().unwrap_or_default().to_owned();
                        return Ok((receiver, id, None));
                    }
                    Ok(Some(StartLiveTailResponseStream::SessionUpdate(u))) => {
                        return Ok((receiver, String::new(), Some(u)));
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => {
                        return Err(Error::unavailable(format!(
                            "{op}: the stream ended before the session started"
                        )))
                    }
                    Err(e) => return Err(map_stream_error(op, e, &self.map_options(op))),
                }
            }
        };
        let timeout = self.config().common().total_timeout();
        let (receiver, session_id, pending) = tokio::select! {
            biased;
            _ = self.close_token().cancelled() => {
                return Err(Error::cancelled(format!("{op}: client is closed")));
            }
            r = tokio::time::timeout(timeout, setup) => match r {
                Ok(r) => r?,
                Err(_) => return Err(Error::timeout(format!("{op}: the session did not start in time"))),
            },
        };
        let control = Arc::new(SessionControl::new());
        lock(&self.inner.sessions).push(control.clone());
        let session = LiveTailSession {
            control: control.clone(),
            session_id: session_id.clone(),
        };
        let client = self.clone();
        tokio::spawn(run_session(
            client,
            control,
            receiver,
            pending,
            on_event,
            opts,
            format!("{op} {session_id}"),
        ));
        Ok(session)
    }
}

async fn run_session(
    client: CloudWatchLogsClient,
    control: Arc<SessionControl>,
    mut receiver: Receiver,
    pending: Option<LiveTailSessionUpdate>,
    on_event: LiveTailCallback,
    opts: LiveTailOptions,
    op: String,
) {
    let update = |u: &LiveTailSessionUpdate| {
        let (sampled, events) = to_events(u);
        if let Some(cb) = &opts.on_update {
            let info = LiveTailUpdate {
                sampled,
                count: events.len(),
            };
            safe_call(|| cb(info));
        }
        for e in events {
            if control.stop.is_cancelled() {
                break;
            }
            safe_call(|| on_event(e));
        }
    };
    if let Some(u) = &pending {
        update(u);
    }
    let close_token = client.close_token().clone();
    let failure = loop {
        let next = tokio::select! {
            biased;
            _ = control.stop.cancelled() => break None,
            _ = close_token.cancelled() => break None,
            next = receiver.recv() => next,
        };
        match next {
            Ok(Some(StartLiveTailResponseStream::SessionUpdate(u))) => update(&u),
            Ok(Some(_)) => {}
            Ok(None) => break None,
            Err(e) => break Some(map_stream_error(&op, e, &client.map_options(&op))),
        }
    };
    if let Some(err) = failure.filter(|e| e.code() != ErrorCode::Cancelled) {
        *lock(&control.error) = Some(err.clone());
        if let Some(cb) = &opts.on_error {
            safe_call(|| cb(&err));
        }
    }
    lock(&client.inner.sessions).retain(|c| !Arc::ptr_eq(c, &control));
    drop(receiver);
    control.finish();
}
