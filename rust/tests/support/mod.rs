//! Service test helpers: a mock AWS endpoint (wiremock) fed with the recorded Moto fixtures,
//! and a fake SDK HTTP client for event streams (live tail).
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use grow_aws_cloudwatch_logs as sdk;
use sdk::aws_sdk_cloudwatchlogs;
use sdk::{CloudWatchLogsClient, CloudWatchLogsConfig, CloudWatchLogsConfigBuilder, RetryPolicy};
use serde_json::Value;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

pub const ACCOUNT: &str = "123456789012";
pub const SECRET: &str = "wJalrXUtnFEMI-test-secret";

/// A recorded fixture (`tests/fixtures/aws/<name>.json`).
pub fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/tests/fixtures/aws/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("fixture {path}"));
    serde_json::from_str(&text).expect("fixture JSON")
}

/// The fixture's response as a wiremock template.
pub fn fixture_response(name: &str) -> ResponseTemplate {
    let f = fixture(name);
    let resp = &f["response"];
    let status = resp["status"].as_u64().unwrap() as u16;
    let mut t = ResponseTemplate::new(status);
    let mut content_type = "application/x-amz-json-1.1".to_owned();
    for (k, v) in resp["headers"].as_object().unwrap() {
        let k = k.to_ascii_lowercase();
        if k == "content-length" || k == "status" {
            continue;
        }
        if k == "content-type" {
            content_type = v.as_str().unwrap().to_owned();
            continue;
        }
        t = t.insert_header(k.as_str(), v.as_str().unwrap());
    }
    match &resp["body"] {
        Value::Null => t.set_body_raw(Vec::<u8>::new(), &content_type),
        Value::String(s) => t.set_body_raw(s.clone().into_bytes(), &content_type),
        other => t.set_body_raw(other.to_string().into_bytes(), &content_type),
    }
}

/// JSON response for an operation.
pub fn json_response(status: u16, body: Value) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .insert_header("x-amzn-requestid", "req-1")
        .set_body_raw(body.to_string().into_bytes(), "application/x-amz-json-1.1")
}

/// AWS JSON error response.
pub fn aws_error(status: u16, code: &str, message: &str) -> ResponseTemplate {
    json_response(
        status,
        serde_json::json!({"__type": code, "message": message}),
    )
    .insert_header("x-amzn-errortype", code)
}

/// Mounts `response` for `Logs_20140328.<op>`.
pub async fn on(server: &MockServer, op: &str, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(header(
            "x-amz-target",
            format!("Logs_20140328.{op}").as_str(),
        ))
        .respond_with(response)
        .mount(server)
        .await;
}

/// Mounts `response` for `op`, used at most `times` times (then later mocks apply).
pub async fn on_times(server: &MockServer, op: &str, times: u64, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(header(
            "x-amz-target",
            format!("Logs_20140328.{op}").as_str(),
        ))
        .respond_with(response)
        .up_to_n_times(times)
        .with_priority(1)
        .mount(server)
        .await;
}

/// Mounts the fixture's response for its operation.
pub async fn on_fixture(server: &MockServer, name: &str) {
    let op = fixture(name)["operation"].as_str().unwrap().to_owned();
    on(server, &op, fixture_response(name)).await;
}

/// JSON bodies of the requests for `op`.
pub async fn bodies(server: &MockServer, op: &str) -> Vec<Value> {
    let target = format!("Logs_20140328.{op}");
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r: &Request| {
            r.headers.get("x-amz-target").and_then(|v| v.to_str().ok()) == Some(target.as_str())
        })
        .map(|r| serde_json::from_slice(&r.body).unwrap_or(Value::Null))
        .collect()
}

/// Fast retries for tests.
pub fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 1,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        ..RetryPolicy::default()
    }
}

/// Config builder pointing at the mock server.
pub fn builder(server: &MockServer) -> CloudWatchLogsConfigBuilder {
    CloudWatchLogsConfig::builder()
        .url(server.uri())
        .region("us-east-1")
        .access_key_id("AKIDEXAMPLE")
        .secret_access_key(SECRET)
        .account_id(ACCOUNT)
        .retry(fast_retry())
}

/// Client pointing at the mock server.
pub fn client(server: &MockServer) -> CloudWatchLogsClient {
    CloudWatchLogsClient::new(builder(server).build().unwrap()).unwrap()
}

/// Client with a default log group.
pub fn group_client(server: &MockServer, group: &str) -> CloudWatchLogsClient {
    CloudWatchLogsClient::new(
        builder(server)
            .log_group(group)
            .log_stream("s1")
            .build()
            .unwrap(),
    )
    .unwrap()
}

// ---------------------------------------------------------------------------------------
// event streams

/// One event-stream frame.
pub fn frame(message_type: &str, event_type: &str, payload: Value) -> Vec<u8> {
    let type_header = if message_type == "exception" {
        ":exception-type"
    } else {
        ":event-type"
    };
    let body = if payload.is_null() {
        Vec::new()
    } else {
        payload.to_string().into_bytes()
    };
    let msg = Message::new(body)
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String(message_type.to_owned().into()),
        ))
        .add_header(Header::new(
            type_header,
            HeaderValue::String(event_type.to_owned().into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ));
    let mut out = Vec::new();
    write_message_to(&msg, &mut out).unwrap();
    out
}

/// What the fake event-stream client answers.
#[derive(Clone)]
pub enum Reply {
    /// A complete event-stream body.
    Stream(Vec<u8>),
    /// Frames, then the body stays open until the test ends.
    Open(Vec<u8>),
    /// An HTTP error.
    Status(u16, String),
}

/// Fake SDK HTTP client: every request gets the next reply; requests are recorded.
#[derive(Clone)]
pub struct FakeHttp {
    replies: Arc<Mutex<Vec<Reply>>>,
    pub requests: Arc<Mutex<Vec<(String, String)>>>,
}

impl std::fmt::Debug for FakeHttp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FakeHttp")
    }
}

impl FakeHttp {
    pub fn new(replies: Vec<Reply>) -> Self {
        FakeHttp {
            replies: Arc::new(Mutex::new(replies)),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// An SDK client using this fake (endpoint `http://localhost:1`, region `us-east-1`).
    pub fn sdk_client(&self) -> aws_sdk_cloudwatchlogs::Client {
        let conf = aws_sdk_cloudwatchlogs::Config::builder()
            .behavior_version(aws_sdk_cloudwatchlogs::config::BehaviorVersion::latest())
            .region(aws_sdk_cloudwatchlogs::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_cloudwatchlogs::config::Credentials::new(
                "AKID", "secret", None, None, "test",
            ))
            .endpoint_url("http://localhost:1")
            .http_client(SharedHttpClient::new(self.clone()))
            .build();
        aws_sdk_cloudwatchlogs::Client::from_conf(conf)
    }
}

struct Pending;

impl http_body_1::Body for Pending {
    type Data = bytes::Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body_1::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Pending
    }
}

impl HttpConnector for FakeHttp {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let body = request
            .body()
            .bytes()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default();
        self.requests
            .lock()
            .unwrap()
            .push((request.uri().to_owned(), body));
        let reply = {
            let mut r = self.replies.lock().unwrap();
            if r.is_empty() {
                Reply::Status(500, "no reply".into())
            } else {
                r.remove(0)
            }
        };
        HttpConnectorFuture::ready(Ok(match reply {
            Reply::Stream(bytes) => {
                let mut resp =
                    HttpResponse::new(StatusCode::try_from(200).unwrap(), SdkBody::from(bytes));
                resp.headers_mut()
                    .insert("content-type", "application/vnd.amazon.eventstream");
                resp
            }
            Reply::Open(bytes) => {
                use http_body_util::BodyExt;
                let head = http_body_util::Full::new(bytes::Bytes::from(bytes))
                    .map_err(|never| match never {});
                let chained = StreamThenPending { head: Some(head) };
                let mut resp = HttpResponse::new(
                    StatusCode::try_from(200).unwrap(),
                    SdkBody::from_body_1_x(chained),
                );
                resp.headers_mut()
                    .insert("content-type", "application/vnd.amazon.eventstream");
                resp
            }
            Reply::Status(status, text) => {
                let mut resp =
                    HttpResponse::new(StatusCode::try_from(status).unwrap(), SdkBody::from(text));
                resp.headers_mut()
                    .insert("content-type", "application/x-amz-json-1.1");
                resp
            }
        }))
    }
}

/// Yields `head` once, then never finishes.
struct StreamThenPending<B> {
    head: Option<B>,
}

impl<B> http_body_1::Body for StreamThenPending<B>
where
    B: http_body_1::Body<Data = bytes::Bytes, Error = std::io::Error> + Unpin,
{
    type Data = bytes::Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body_1::Frame<Self::Data>, Self::Error>>> {
        if let Some(head) = self.head.as_mut() {
            match std::pin::Pin::new(head).poll_frame(cx) {
                std::task::Poll::Ready(None) => self.head = None,
                other => return other,
            }
        }
        std::pin::Pin::new(&mut Pending).poll_frame(cx)
    }
}

impl HttpClient for FakeHttp {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}
