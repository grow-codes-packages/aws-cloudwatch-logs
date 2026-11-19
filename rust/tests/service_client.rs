// Service-owned: replaces the skeleton `tests/client.rs` (demo endpoints) of sdk-kit 0.1.0.
//! Configuration, client lifecycle, health/ping, error mapping and hooks.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use grow_aws_cloudwatch_logs as sdk;
use sdk::core::{EnvReader, ErrorInfo, Hooks, RequestInfo, ResponseInfo};
use sdk::{
    aws_error_code, error_code_for_aws, error_code_from_body, partition_of, CloudWatchLogsClient,
    CloudWatchLogsConfig, ErrorCode, OversizePolicy, API_VERSION,
};
use serde_json::json;
use support::*;
use wiremock::matchers::{body_string_contains, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn env(vars: &[(&str, &str)]) -> EnvReader {
    EnvReader::from_map(vars.iter().map(|(k, v)| (k.to_string(), v.to_string())))
}

#[test]
fn config_from_env_precedence() {
    let e = env(&[
        ("GROW_AWS_CLOUDWATCH_LOGS_URL", "http://grow:5000"),
        ("AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "http://vendor:5000"),
        ("AWS_REGION", "eu-west-1"),
        ("AWS_ACCESS_KEY_ID", "AKIAVENDOR"),
        ("AWS_SECRET_ACCESS_KEY", "vendor-secret-value"),
        ("AWS_SESSION_TOKEN", "vendor-session-token"),
        ("AWS_PROFILE", "dev"),
        ("CLOUDWATCH_LOG_GROUP", "app/web"),
        ("CLOUDWATCH_LOG_STREAM", "web-1"),
        ("GROW_AWS_CLOUDWATCH_LOGS_ACCOUNT_ID", ACCOUNT),
        ("GROW_AWS_CLOUDWATCH_LOGS_MAX_EVENT_BYTES", "1000"),
        ("GROW_AWS_CLOUDWATCH_LOGS_OVERSIZE_POLICY", "Drop"),
        ("GROW_AWS_CLOUDWATCH_LOGS_AUTO_CREATE", "false"),
        (
            "GROW_AWS_CLOUDWATCH_LOGS_ROLE_ARN",
            "arn:aws:iam::123456789012:role/r",
        ),
        ("GROW_AWS_CLOUDWATCH_LOGS_ROLE_SESSION_NAME", "sess"),
        ("GROW_AWS_CLOUDWATCH_LOGS_EXTERNAL_ID", "ext-id-value"),
    ]);
    let c = CloudWatchLogsConfig::builder().build_with_env(&e).unwrap();
    assert_eq!(c.endpoint().unwrap().as_str(), "http://grow:5000/");
    assert_eq!(c.url().as_str(), "http://grow:5000/");
    assert_eq!(c.region(), Some("eu-west-1"));
    assert_eq!(c.access_key_id(), Some("AKIAVENDOR"));
    assert_eq!(
        c.secret_access_key().unwrap().expose_secret(),
        "vendor-secret-value"
    );
    assert!(c.session_token().is_some());
    assert_eq!(c.profile(), Some("dev"));
    assert_eq!(c.log_group(), Some("app/web"));
    assert_eq!(c.log_stream(), Some("web-1"));
    assert_eq!(c.account_id(), Some(ACCOUNT));
    assert_eq!(c.max_event_bytes(), 1000);
    assert_eq!(c.oversize_policy(), OversizePolicy::Drop);
    assert!(!c.auto_create());
    assert_eq!(c.role_arn(), Some("arn:aws:iam::123456789012:role/r"));
    assert_eq!(c.role_session_name(), "sess");
    assert!(c.external_id().is_some());
    assert!(c.credentials().is_none());
    assert!(c.sdk_client().is_none());
    assert_eq!(c.known_secrets().len(), 3);
    let dbg = format!("{c:?}");
    assert!(
        !dbg.contains("vendor-secret-value") && !dbg.contains("ext-id-value"),
        "{dbg}"
    );
    assert!(dbg.contains("AKIA[REDACTED]"), "{dbg}");

    // GROW_* keys are taken as one set; explicit values win.
    let e = env(&[
        ("GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID", "AKIAGROW"),
        (
            "GROW_AWS_CLOUDWATCH_LOGS_SECRET_ACCESS_KEY",
            "grow-secret-value",
        ),
        ("AWS_SECRET_ACCESS_KEY", "vendor"),
        ("AWS_ENDPOINT_URL", "http://generic:1"),
        ("GROW_AWS_CLOUDWATCH_LOGS_LOG_GROUP", "grow/group"),
        ("CLOUDWATCH_LOG_GROUP", "vendor/group"),
    ]);
    let c = CloudWatchLogsConfig::builder()
        .region("us-west-2")
        .build_with_env(&e)
        .unwrap();
    assert_eq!(c.access_key_id(), Some("AKIAGROW"));
    assert_eq!(
        c.secret_access_key().unwrap().expose_secret(),
        "grow-secret-value"
    );
    assert_eq!(c.region(), Some("us-west-2"));
    assert_eq!(c.endpoint().unwrap().as_str(), "http://generic:1/");
    assert_eq!(c.log_group(), Some("grow/group"));
    assert_eq!(c.max_event_bytes(), sdk::DEFAULT_MAX_EVENT_BYTES);
    assert!(c.auto_create());

    // without endpoint: informational regional URL
    let c = CloudWatchLogsConfig::builder()
        .region("ap-south-1")
        .build_with_env(&env(&[]))
        .unwrap();
    assert!(c.endpoint().is_none());
    assert_eq!(c.url().as_str(), "https://logs.ap-south-1.amazonaws.com/");
    let c = CloudWatchLogsConfig::builder().build().unwrap();
    assert_eq!(c.url().as_str(), "https://logs.us-east-1.amazonaws.com/");
    let _ = CloudWatchLogsConfig::from_env();

    // env errors
    for (k, v) in [
        ("GROW_AWS_CLOUDWATCH_LOGS_MAX_EVENT_BYTES", "-1"),
        ("GROW_AWS_CLOUDWATCH_LOGS_MAX_EVENT_BYTES", "x"),
        ("GROW_AWS_CLOUDWATCH_LOGS_OVERSIZE_POLICY", "nope"),
        ("GROW_AWS_CLOUDWATCH_LOGS_AUTO_CREATE", "maybe"),
    ] {
        let err = CloudWatchLogsConfig::builder()
            .build_with_env(&env(&[(k, v)]))
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Config, "{k}={v}");
    }
}

#[test]
fn config_validation() {
    let b = CloudWatchLogsConfig::builder;
    let bad = [
        b().region("Bad Region"),
        b().access_key_id("AKIA1234"),
        b().secret_access_key("only-secret"),
        b().session_token("token-only-value"),
        b().role_arn("not-an-arn"),
        b().max_event_bytes(0),
        b().max_event_bytes(sdk::MAX_EVENT_BYTES_LIMIT + 1),
        b().log_group("bad group!"),
        b().log_stream("a:b"),
        b().account_id("12"),
        b().access_key_id("AKIA1234")
            .secret_access_key("s3cr3t-value")
            .credentials_provider(aws_credential_types::Credentials::new(
                "AK", "sk", None, None, "t",
            )),
    ];
    for builder in bad {
        let dbg = format!("{builder:?}");
        let err = builder.build().unwrap_err();
        assert_eq!(err.code(), ErrorCode::Config, "{dbg}");
    }
    let fake = FakeHttp::new(vec![]);
    let err = b()
        .sdk_client(fake.sdk_client())
        .profile("p")
        .url("http://x")
        .build()
        .unwrap_err();
    assert!(
        err.message()
            .contains("sdkClient cannot be combined with profile, url"),
        "{err}"
    );
    // sdk client reads only the non-credential settings from the env
    let c = b()
        .sdk_client(fake.sdk_client())
        .build_with_env(&env(&[
            ("AWS_ACCESS_KEY_ID", "AKIAX"),
            ("AWS_REGION", "us-east-1"),
        ]))
        .unwrap();
    assert!(c.access_key_id().is_none());
    assert!(format!("{c:?}").contains("aws_sdk_cloudwatchlogs::Client"));
    // all the builder setters
    let c = b()
        .url("http://localhost:5000")
        .region("us-east-1")
        .access_key_id("AK")
        .secret_access_key("secret-key-value")
        .session_token("session-token-value")
        .role_arn("arn:aws:iam::1:role/r")
        .role_session_name("s")
        .external_id("ext-value")
        .log_group("g")
        .log_stream("s")
        .account_id(ACCOUNT)
        .max_event_bytes(100)
        .oversize_policy(OversizePolicy::Error)
        .auto_create(false)
        .connect_timeout(Duration::from_secs(1))
        .request_timeout(Duration::from_secs(2))
        .total_timeout(Duration::from_secs(3))
        .max_retries(0)
        .retry(fast_retry())
        .tls(Default::default())
        .ca_cert_path("/nonexistent.pem")
        .insecure_skip_verify(true)
        .proxy("http://proxy:3128")
        .header("x-extra", "1")
        .header("authorization", "Bearer header-secret")
        .user_agent_suffix("it/1")
        .common(|c| c)
        .build()
        .unwrap();
    assert!(c
        .known_secrets()
        .iter()
        .any(|s| s == "Bearer header-secret"));
    assert_eq!(format!("{}", OversizePolicy::Truncate), "truncate");
    assert_eq!(
        "ERROR".parse::<OversizePolicy>().unwrap(),
        OversizePolicy::Error
    );
    assert_eq!(
        format!("{:?}", b().access_key_id("AK").secret_access_key("x")),
        format!("{:?}", b().access_key_id("AK").secret_access_key("x"))
    );
}

#[tokio::test]
async fn health_ping_version_close() {
    let server = MockServer::start().await;
    on_fixture(&server, "ping.ok").await;
    let c = client(&server);
    assert!(format!("{c:?}").contains("CloudWatchLogsClient"));
    let h = c.health().await.unwrap();
    assert!(h.healthy);
    assert_eq!(h.status, "ok");
    assert!(c.ping().await.unwrap() > Duration::ZERO);
    let err = c.server_version().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unsupported);
    assert_eq!(API_VERSION, "2014-03-28");
    assert_eq!(c.region().await.unwrap(), "us-east-1");
    assert!(c.sdk().await.is_ok());
    assert!(!c.log_stream().is_empty());
    let flag = Arc::new(Mutex::new(false));
    let f2 = flag.clone();
    c.on_close(move || {
        Box::pin(async move {
            *f2.lock().unwrap() = true;
        })
    });
    c.close().await;
    c.close().await;
    assert!(*flag.lock().unwrap());
    assert!(c.is_closed());
    assert_eq!(c.ping().await.unwrap_err().code(), ErrorCode::Cancelled);
    assert_eq!(c.health().await.unwrap_err().code(), ErrorCode::Cancelled);
    assert_eq!(
        c.server_version().await.unwrap_err().code(),
        ErrorCode::Cancelled
    );
    let bodies = bodies(&server, "DescribeLogGroups").await;
    assert_eq!(bodies[0], json!({"limit": 1}));
}

#[tokio::test]
async fn health_statuses() {
    // auth failure → ok with details
    let server = MockServer::start().await;
    on_fixture(&server, "ping.auth-unknown-key").await;
    let c = client(&server);
    let h = c.health().await.unwrap();
    assert!(h.healthy);
    assert_eq!(h.details.get("auth").map(String::as_str), Some("failed"));
    let e = c.ping().await.unwrap_err();
    assert_eq!(e.code(), ErrorCode::Authentication);
    assert_eq!(aws_error_code(&e), Some("InvalidClientTokenId"));
    assert!(e.request_id().is_some());

    let server = MockServer::start().await;
    on_fixture(&server, "ping.auth-bad-signature").await;
    assert_eq!(
        client(&server).ping().await.unwrap_err().code(),
        ErrorCode::Authentication
    );

    // throttled → degraded
    let server = MockServer::start().await;
    on(
        &server,
        "DescribeLogGroups",
        aws_error(400, "ThrottlingException", "slow down"),
    )
    .await;
    let h = client(&server).health().await.unwrap();
    assert_eq!((h.healthy, h.status.as_str()), (true, "degraded"));
    // 5xx → unhealthy
    let server = MockServer::start().await;
    on(&server, "DescribeLogGroups", ResponseTemplate::new(503)).await;
    let h = client(&server).health().await.unwrap();
    assert_eq!((h.healthy, h.status.as_str()), (false, "unhealthy"));
    // other 4xx → ok
    let server = MockServer::start().await;
    on(&server, "DescribeLogGroups", ResponseTemplate::new(404)).await;
    let h = client(&server).health().await.unwrap();
    assert!(h.healthy);
    // unreachable
    let c = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .url("http://127.0.0.1:1")
            .region("us-east-1")
            .access_key_id("AK")
            .secret_access_key("secret-value")
            .max_retries(0)
            .build()
            .unwrap(),
    )
    .unwrap();
    let h = c.health().await.unwrap();
    assert_eq!((h.healthy, h.status.as_str()), (false, "unreachable"));
    assert_eq!(c.ping().await.unwrap_err().code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn error_mapping_from_fixtures() {
    let cases = [
        ("deleteLogGroup.not-found", ErrorCode::NotFound),
        ("deleteLogGroup.access-denied", ErrorCode::PermissionDenied),
        ("putRetentionPolicy.not-found", ErrorCode::NotFound),
    ];
    for (name, code) in cases {
        let server = MockServer::start().await;
        on_fixture(&server, name).await;
        let c = client(&server);
        let err = match name {
            n if n.starts_with("deleteLogGroup") => c
                .delete_log_group("g", Default::default())
                .await
                .unwrap_err(),
            _ => c.put_retention_policy("g", Some(7)).await.unwrap_err(),
        };
        assert_eq!(err.code(), code, "{name}: {err}");
        assert!(!err.message().contains(SECRET));
    }
    // retryable codes and overrides
    let server = MockServer::start().await;
    on(
        &server,
        "DeleteLogGroup",
        aws_error(400, "OperationAbortedException", "busy"),
    )
    .await;
    let err = client(&server)
        .delete_log_group("g", Default::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Conflict);
    assert!(err.is_retryable());
    let server = MockServer::start().await;
    on(
        &server,
        "DeleteLogGroup",
        aws_error(400, "ThrottlingException", "slow").insert_header("retry-after", "1"),
    )
    .await;
    let err = client(&server)
        .delete_log_group("g", Default::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::RateLimited);
    // 200 with an unparseable body
    let server = MockServer::start().await;
    on(
        &server,
        "DescribeLogGroups",
        ResponseTemplate::new(200).set_body_raw("{not json", "application/json"),
    )
    .await;
    let err = client(&server).ping().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    // known secrets never leak into messages
    let server = MockServer::start().await;
    on(
        &server,
        "DeleteLogGroup",
        aws_error(400, "InvalidParameterException", &format!("bad {SECRET}")),
    )
    .await;
    let err = client(&server)
        .delete_log_group("g", Default::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(!err.message().contains(SECRET), "{err}");
    assert!(err.message().contains("[REDACTED]"), "{err}");

    assert_eq!(
        error_code_for_aws("SessionTimeoutException"),
        Some(ErrorCode::Timeout)
    );
    assert_eq!(
        error_code_from_body(r#"{"__type":"com.amazonaws.logs#ResourceNotFoundException"}"#)
            .as_deref(),
        Some("ResourceNotFoundException")
    );
    assert_eq!(partition_of("cn-north-1"), "aws-cn");
    assert_eq!(partition_of("us-gov-west-1"), "aws-us-gov");
    assert_eq!(partition_of("us-iso-east-1"), "aws-iso");
    assert_eq!(partition_of("us-isob-east-1"), "aws-iso-b");
    assert_eq!(partition_of("eusc-de-east-1"), "aws-eusc");
    assert_eq!(partition_of("eu-west-1"), "aws");
}

#[derive(Default)]
struct Recorder {
    requests: Mutex<Vec<RequestInfo>>,
    responses: Mutex<Vec<ResponseInfo>>,
    errors: Mutex<Vec<ErrorInfo>>,
}

impl Hooks for Recorder {
    fn on_request(&self, info: &RequestInfo) {
        self.requests.lock().unwrap().push(info.clone());
    }
    fn on_response(&self, info: &ResponseInfo) {
        self.responses.lock().unwrap().push(info.clone());
    }
    fn on_error(&self, info: &ErrorInfo) {
        self.errors.lock().unwrap().push(info.clone());
    }
}

#[tokio::test]
async fn hooks_user_agent_headers_and_retries() {
    let server = MockServer::start().await;
    on_times(&server, "DescribeLogGroups", 1, ResponseTemplate::new(503)).await;
    on_fixture(&server, "ping.ok").await;
    let rec = Arc::new(Recorder::default());
    let c = CloudWatchLogsClient::new(
        builder(&server)
            .hooks(rec.clone())
            .header("x-extra", "yes")
            .user_agent_suffix("suite/1")
            .build()
            .unwrap(),
    )
    .unwrap();
    c.ping().await.unwrap();
    let reqs = rec.requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[1].attempt, 2);
    assert_eq!(reqs[0].method, "POST");
    assert!(reqs[0]
        .headers
        .get("authorization")
        .is_some_and(|v| v.contains("REDACTED")));
    let resps = rec.responses.lock().unwrap().clone();
    assert_eq!(
        resps.iter().map(|r| r.status).collect::<Vec<_>>(),
        [503, 200]
    );
    assert_eq!(resps[1].request_id.as_deref().map(str::len), Some(52));
    let errs = rec.errors.lock().unwrap().clone();
    assert_eq!(errs.len(), 1);
    assert!(errs[0].will_retry);
    let received = server.received_requests().await.unwrap();
    let ua = received[0]
        .headers
        .get("user-agent")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ua.contains("grow-codes-aws-cloudwatch-logs/") && ua.contains("suite/1"),
        "{ua}"
    );
    assert_eq!(received[0].headers.get("x-extra").unwrap(), "yes");

    // network failure reaches on_error
    let rec = Arc::new(Recorder::default());
    let c = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .url("http://127.0.0.1:1")
            .region("us-east-1")
            .access_key_id("AK")
            .secret_access_key("secret-value")
            .max_retries(0)
            .hooks(rec.clone())
            .build()
            .unwrap(),
    )
    .unwrap();
    assert!(c.ping().await.is_err());
    assert_eq!(rec.errors.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn account_lookup_with_sts() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("Action=GetCallerIdentity"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult><Arn>arn:aws:iam::210987654321:user/x</Arn><UserId>AID</UserId><Account>210987654321</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></GetCallerIdentityResponse>"#,
            "text/xml",
        ))
        .expect(1)
        .mount(&server)
        .await;
    let c = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .url(server.uri())
            .region("eu-central-1")
            .access_key_id("AK")
            .secret_access_key("secret-value")
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        c.log_group_arn("app/web").await.unwrap(),
        "arn:aws:logs:eu-central-1:210987654321:log-group:app/web"
    );
    // cached
    assert!(c
        .log_group_arn("other")
        .await
        .unwrap()
        .contains("210987654321"));
    assert_eq!(
        c.log_group_arn("arn:aws:logs:x:1:log-group:g:*")
            .await
            .unwrap(),
        "arn:aws:logs:x:1:log-group:g"
    );
    assert_eq!(
        c.log_group_arn("bad name").await.unwrap_err().code(),
        ErrorCode::Validation
    );

    // STS answering without an account
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"<GetCallerIdentityResponse><GetCallerIdentityResult></GetCallerIdentityResult></GetCallerIdentityResponse>"#,
            "text/xml",
        ))
        .mount(&server)
        .await;
    let c = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .url(server.uri())
            .region("eu-central-1")
            .access_key_id("AK")
            .secret_access_key("secret-value")
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        c.log_group_arn("g").await.unwrap_err().code(),
        ErrorCode::Unavailable
    );
}

#[tokio::test]
async fn sdk_client_injection() {
    let fake = FakeHttp::new(vec![Reply::Status(200, r#"{"logGroups":[]}"#.into())]);
    let c = CloudWatchLogsClient::with_sdk_client(fake.sdk_client()).unwrap();
    c.ping().await.unwrap();
    assert_eq!(c.region().await.unwrap(), "us-east-1");
    // no STS without account id
    assert_eq!(
        c.log_group_arn("g").await.unwrap_err().code(),
        ErrorCode::Config
    );
    c.close().await;
    // the caller's SDK client still works
    let fake2 = FakeHttp::new(vec![Reply::Status(200, r#"{"logGroups":[]}"#.into())]);
    let sdk_client = fake2.sdk_client();
    let c = CloudWatchLogsClient::with_sdk_client(sdk_client.clone()).unwrap();
    c.close().await;
    assert!(sdk_client.describe_log_groups().send().await.is_ok());
}

#[tokio::test]
async fn missing_region_is_config() {
    // A region is required; keep the SDK away from instance metadata (slow off EC2).
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    let c = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .url("http://127.0.0.1:1")
            .access_key_id("AK")
            .secret_access_key("secret-value")
            .profile("grow-sdk-test-profile-that-does-not-exist")
            .build()
            .unwrap(),
    )
    .unwrap();
    if std::env::var_os("AWS_REGION").is_none() && std::env::var_os("AWS_DEFAULT_REGION").is_none()
    {
        let err = c.ping().await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Config, "{err}");
        assert!(err.message().starts_with("ping: "));
    }
}

#[tokio::test]
async fn assume_role_uses_sts_first() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("Action=AssumeRole"))
        .and(body_string_contains("ExternalId=ext-id"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>ASIAROLE</AccessKeyId><SecretAccessKey>role-secret</SecretAccessKey><SessionToken>role-token</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::1:assumed-role/r/grow-sdk</Arn><AssumedRoleId>AROA:grow-sdk</AssumedRoleId></AssumedRoleUser></AssumeRoleResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></AssumeRoleResponse>"#,
            "text/xml",
        ))
        .expect(1..)
        .mount(&server)
        .await;
    on_fixture(&server, "ping.ok").await;
    let c = CloudWatchLogsClient::new(
        builder(&server)
            .role_arn("arn:aws:iam::123456789012:role/r")
            .external_id("ext-id")
            .build()
            .unwrap(),
    )
    .unwrap();
    c.ping().await.unwrap();
    let received = server.received_requests().await.unwrap();
    let logs_call = received
        .iter()
        .find(|r| r.headers.get("x-amz-target").is_some())
        .unwrap();
    let auth = logs_call.headers.get("authorization").unwrap().to_str().unwrap();
    assert!(auth.contains("Credential=ASIAROLE/"), "{auth}");
}
