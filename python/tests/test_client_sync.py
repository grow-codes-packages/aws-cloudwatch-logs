"""CloudWatchLogsClient (boto3) against a local AWS JSON mock with the recorded fixtures."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import grow_aws_cloudwatch_logs as sdk
from _support import INTERNALS, TEST_SECRET, AwsMock, aws_config, fixture

G = "cwlv-0badc0de/app"
ARN = "arn:aws:logs:us-east-1:123456789012:log-group:" + G


def client(aws: AwsMock, **kw: Any) -> sdk.CloudWatchLogsClient:
    internals = kw.pop("internals", INTERNALS)
    url = kw.pop("url", aws.url)
    return sdk.CloudWatchLogsClient(aws_config(url, **kw), internals=internals)


def group(name: str = G, **extra: Any) -> dict[str, Any]:
    return {
        "logGroupName": name,
        "arn": f"arn:aws:logs:us-east-1:123456789012:log-group:{name}:*",
        "logGroupArn": f"arn:aws:logs:us-east-1:123456789012:log-group:{name}",
        "creationTime": 1795034976104,
        **extra,
    }


# --------------------------------------------------------------------------- core


def test_health_ping_server_version_close(aws: AwsMock) -> None:
    aws.fixture("ping.ok").fixture("ping.ok")
    with client(aws) as c:
        assert c.health() == sdk.HealthStatus(True, "ok", {})
        assert c.ping() >= 0
        with pytest.raises(sdk.UnsupportedError):
            c.server_version()
        assert c.region == "us-east-1"
        assert c.sdk is c.sdk
        assert "CloudWatchLogsClient(" in repr(c)
        assert TEST_SECRET not in repr(c)
    # health uses the log group as prefix
    assert aws.bodies("DescribeLogGroups")[0] == {"limit": 1, "logGroupNamePrefix": "app/main"}
    c.close()
    with pytest.raises(sdk.CancelledError, match="client is closed"):
        c.ping()


@pytest.mark.parametrize(
    ("name", "status", "details"),
    [
        ("ping.auth-unknown-key", "ok", {"auth": "failed", "code": "AUTHENTICATION"}),
        ("ping.auth-bad-signature", "ok", {"auth": "failed", "code": "AUTHENTICATION"}),
        ("deleteLogGroup.access-denied", "ok", {"auth": "failed", "code": "PERMISSION_DENIED"}),
    ],
)
def test_health_auth_failures_count_as_healthy(
    aws: AwsMock, name: str, status: str, details: dict[str, str]
) -> None:
    aws.fixture(name, op="DescribeLogGroups")
    with client(aws) as c:
        assert c.health() == sdk.HealthStatus(True, status, details)


def test_health_degraded_unhealthy_unreachable(aws: AwsMock) -> None:
    aws.error("DescribeLogGroups", "ThrottlingException")
    aws.fixture("stopQuery.moto-500", op="DescribeLogGroups")
    with client(aws) as c:
        assert c.health().status == "degraded"
        h = c.health()
        assert (h.healthy, h.status, h.details["httpStatus"]) == (False, "unhealthy", "500")
    with client(aws, url="http://127.0.0.1:9") as c:
        h = c.health()
        assert (h.healthy, h.status) == (False, "unreachable")


def test_health_raises_config_and_cancelled(aws: AwsMock, monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_CONFIG_FILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    with client(aws, region=None) as c, pytest.raises(sdk.ConfigError, match="region"):
        c.health()
    token = sdk.CancelToken()
    token.cancel()
    with client(aws) as c, pytest.raises(sdk.CancelledError):
        c.health(cancel=token)


def test_ping_auth_errors_and_request_id(aws: AwsMock) -> None:
    aws.fixture("ping.auth-unknown-key").fixture(
        "deleteLogGroup.access-denied", op="DescribeLogGroups"
    )
    with client(aws) as c:
        with pytest.raises(sdk.AuthenticationError) as info:
            c.ping()
        assert info.value.http_status == 403
        assert (
            info.value.request_id
            == fixture("ping.auth-unknown-key")["response"]["headers"]["x-amzn-requestid"]
        )
        assert TEST_SECRET not in str(info.value)
        assert isinstance(info.value.cause, sdk.AwsSdkError)
        with pytest.raises(sdk.PermissionDeniedError):
            c.ping()


def test_hooks_logger_headers_and_user_agent(aws: AwsMock) -> None:
    seen: list[Any] = []
    logs: list[tuple[str, Any]] = []

    class Log:
        def debug(self, msg: str, fields: Any = None) -> None:
            logs.append((msg, fields))

        info = warn = error = debug

    hooks = sdk.Hooks(on_request=seen.append, on_response=seen.append, on_error=seen.append)
    aws.fixture("ping.ok")
    with client(
        aws, hooks=hooks, logger=Log(), headers={"X-Extra": "1"}, user_agent_suffix="it/1"
    ) as c:
        c.ping()
    request = aws.seen[-1]
    assert request.headers["X-Extra"] == "1"
    assert "grow-codes-aws-cloudwatch-logs/" in request.headers["User-Agent"]
    assert request.headers["User-Agent"].endswith("(python) it/1")
    assert isinstance(seen[0], sdk.RequestInfo) and seen[0].attempt == 1
    assert seen[0].headers["Authorization"] == "[REDACTED]"
    assert isinstance(seen[1], sdk.ResponseInfo) and seen[1].status == 200
    assert seen[1].request_id == fixture("ping.ok")["response"]["headers"]["x-amzn-requestid"]
    assert logs and TEST_SECRET not in repr(logs)


def test_on_error_hook_on_network_error() -> None:
    errors: list[sdk.ErrorInfo] = []
    cfg = aws_config("http://127.0.0.1:9", hooks=sdk.Hooks(on_error=errors.append))
    with (
        sdk.CloudWatchLogsClient(cfg, internals=INTERNALS) as c,
        pytest.raises(sdk.UnavailableError),
    ):
        c.ping()
    assert errors and errors[0].will_retry is False
    assert isinstance(errors[0].error, sdk.UnavailableError)


def test_retries_on_throttling(aws: AwsMock) -> None:
    aws.error("DescribeLogGroups", "ThrottlingException").fixture("ping.ok")
    cfg = aws_config(aws.url, retry=sdk.RetryPolicy(max_retries=2))
    with sdk.CloudWatchLogsClient(cfg, internals=INTERNALS) as c:
        assert c.ping() >= 0
    assert len(aws.bodies("DescribeLogGroups")) == 2


def test_total_timeout_and_cancel_token(aws: AwsMock) -> None:
    aws.replies["DescribeLogGroups"].append(
        __import__("_support").AwsReply(400, {"__type": "ThrottlingException"}, delay=0.3)
    )
    aws.reply("DescribeLogGroups", {"logGroups": []})
    with client(aws, total_timeout=0.2, retry=sdk.RetryPolicy(max_retries=3)) as c:
        with pytest.raises(sdk.TimeoutError):
            # the first attempt ends after the deadline: the retry attempt is refused
            c.ping()
    aws.replies["DescribeLogGroups"].clear()
    aws.reply("DescribeLogGroups", {"logGroups": []}, delay=2)
    token = sdk.CancelToken()
    threading.Timer(0.1, token.cancel).start()
    started = time.monotonic()
    with client(aws) as c, pytest.raises(sdk.CancelledError):
        c.ping(cancel=token)
    assert time.monotonic() - started < 1.5


def test_cancel_token_success_path(aws: AwsMock) -> None:
    aws.fixture("ping.ok")
    with client(aws) as c:
        assert c.ping(cancel=sdk.CancelToken()) >= 0


def test_cancel_token_error_path(aws: AwsMock) -> None:
    aws.fixture("deleteLogGroup.not-found")
    with client(aws) as c, pytest.raises(sdk.NotFoundError):
        c.delete_log_group("x", cancel=sdk.CancelToken())


def test_from_env_and_profile_config_error(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        "GROW_AWS_CLOUDWATCH_LOGS_URL": "http://127.0.0.1:9",
        "AWS_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "AKIAENV",
        "AWS_SECRET_ACCESS_KEY": "env-secret-value",
    }
    with sdk.CloudWatchLogsClient.from_env(env) as c:
        assert c.config.region == "eu-west-1"
        assert c.region == "eu-west-1"
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
    cfg = sdk.CloudWatchLogsConfig(url="http://127.0.0.1:9", profile="nope", region="us-east-1")
    with sdk.CloudWatchLogsClient(cfg) as c, pytest.raises(sdk.ConfigError):
        c.ping()


def test_no_credentials_is_authentication_error(
    monkeypatch: pytest.MonkeyPatch, aws: AwsMock
) -> None:
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    cfg = sdk.CloudWatchLogsConfig(url=aws.url, region="us-east-1")
    with sdk.CloudWatchLogsClient(cfg) as c, pytest.raises(sdk.AuthenticationError):
        c.ping()


def test_role_arn_builds_refreshable_credentials(aws: AwsMock) -> None:
    cfg = aws_config(
        aws.url,
        role_arn="arn:aws:iam::123456789012:role/r",
        external_id="ext-123456",
        session_token="sess-token-123",
    )
    with sdk.CloudWatchLogsClient(cfg, internals=INTERNALS) as c:
        creds = c.sdk._request_signer._credentials
        assert type(creds).__name__ == "DeferredRefreshableCredentials"


def test_tls_proxy_and_ca_pem(aws: AwsMock, tmp_path: Any) -> None:
    cfg = aws_config(
        aws.url,
        tls=sdk.TlsConfig(
            ca_cert_pem="-----BEGIN CERTIFICATE-----\n", client_cert_path="/c", client_key_path="/k"
        ),
        proxy="http://127.0.0.1:9",
    )
    c = sdk.CloudWatchLogsClient(cfg, internals=INTERNALS)
    sdk_client = c.sdk
    assert sdk_client.meta.config.proxies == {
        "http": "http://127.0.0.1:9",
        "https": "http://127.0.0.1:9",
    }
    assert sdk_client.meta.config.client_cert == ("/c", "/k")
    path = c._tls.verify  # type: ignore[union-attr]
    assert isinstance(path, str)
    with open(path) as handle:
        assert handle.read().startswith("-----BEGIN")
    c.close()
    import os

    assert not os.path.exists(path)
    insecure = aws_config(aws.url, tls=sdk.TlsConfig(insecure_skip_verify=True))
    with sdk.CloudWatchLogsClient(insecure) as c2:
        assert c2._client_kwargs(False)["verify"] is False
    ca = aws_config(aws.url, tls=sdk.TlsConfig(ca_cert_path="/ca.pem"))
    with sdk.CloudWatchLogsClient(ca) as c3:
        assert c3._client_kwargs(False)["verify"] == "/ca.pem"


def test_caller_supplied_sdk_client_is_not_closed(aws: AwsMock) -> None:
    import boto3
    from botocore.config import Config

    raw = boto3.client(
        "logs",
        endpoint_url=aws.url,
        region_name="us-east-1",
        aws_access_key_id="AKIA",
        aws_secret_access_key="x",
        config=Config(inject_host_prefix=False),
    )
    aws.fixture("ping.ok").fixture("ping.ok")
    c = sdk.CloudWatchLogsClient(sdk.CloudWatchLogsConfig(sdk_client=raw, log_group=G))
    assert c.sdk is raw
    assert c.health().healthy
    c.close()
    raw.describe_log_groups()  # still usable


# --------------------------------------------------------------------------- bootstrap


def test_get_log_group_pages_and_missing(aws: AwsMock) -> None:
    aws.reply("DescribeLogGroups", {"logGroups": [group(G + "x")], "nextToken": "t1"})
    aws.reply("DescribeLogGroups", {"logGroups": [group(G, retentionInDays=7, kmsKeyId="k")]})
    aws.reply("DescribeLogGroups", {"logGroups": []})
    with client(aws) as c:
        g = c.get_log_group(G)
        assert g is not None
        assert (g.name, g.arn, g.retention_days, g.kms_key_id) == (G, ARN, 7, "k")
        assert g.creation_time == datetime(2026, 11, 18, 20, 49, 36, 104000, tzinfo=timezone.utc)
        assert c.get_log_group("missing") is None
        with pytest.raises(sdk.ValidationError):
            c.get_log_group("bad name!")
    assert aws.bodies("DescribeLogGroups")[1]["nextToken"] == "t1"


def test_ensure_log_group_create_then_exists(aws: AwsMock) -> None:
    aws.fixture("ensureLogGroup.create")
    aws.reply("DescribeLogGroups", {"logGroups": [group()]})
    aws.fixture("putRetentionPolicy.ok")
    with client(aws) as c:
        res = c.ensure_log_group(G, retention_days=7, tags={"team": "sdk"})
    assert res == sdk.EnsureLogGroupResult(created=True, arn=ARN)
    assert aws.bodies("CreateLogGroup")[0] == {"logGroupName": G, "tags": {"team": "sdk"}}
    assert aws.bodies("PutRetentionPolicy")[0] == {"logGroupName": G, "retentionInDays": 7}

    aws.fixture("ensureLogGroup.exists")
    aws.reply(
        "DescribeLogGroups", {"logGroups": [group(kmsKeyId="arn:aws:kms:us-east-1:1:key/k1")]}
    )
    aws.fixture("ensureLogGroup.tag-reconcile")
    aws.fixture("putRetentionPolicy.delete")
    with client(aws) as c:
        res = c.ensure_log_group(G, tags={"env": "it"}, kms_key_id="k1", retention_days=0)
    assert res.created is False
    assert aws.bodies("TagResource")[-1] == {"resourceArn": ARN, "tags": {"env": "it"}}
    assert aws.bodies("DeleteRetentionPolicy") == [{"logGroupName": G}]


def test_ensure_log_group_kms_conflict_validation_and_errors(aws: AwsMock) -> None:
    aws.fixture("ensureLogGroup.exists")
    aws.reply(
        "DescribeLogGroups", {"logGroups": [group(kmsKeyId="arn:aws:kms:us-east-1:1:key/k1")]}
    )
    aws.error("CreateLogGroup", "OperationAbortedException")
    aws.fixture("ensureLogGroup.access-denied")
    with client(aws) as c:
        with pytest.raises(sdk.ConflictError, match="KMS"):
            c.ensure_log_group(G, kms_key_id="k2")
        with pytest.raises(sdk.ConflictError) as info:
            c.ensure_log_group(G)
        assert info.value.retryable is True
        assert sdk.aws_error_code(info.value) == "OperationAbortedException"
        with pytest.raises(sdk.PermissionDeniedError):
            c.ensure_log_group(G)
        with pytest.raises(sdk.ValidationError):
            c.ensure_log_group("bad name!")
        with pytest.raises(sdk.ValidationError, match="retention_days"):
            c.ensure_log_group(G, retention_days=8)
        with pytest.raises(sdk.ValidationError):
            c.ensure_log_group(G, retention_days=True)  # type: ignore[arg-type]


def test_ensure_log_group_vanished_group(aws: AwsMock) -> None:
    aws.fixture("ensureLogGroup.create")
    aws.reply("DescribeLogGroups", {"logGroups": []})
    with client(aws) as c, pytest.raises(sdk.NotFoundError):
        c.ensure_log_group(G)


def test_ensure_log_stream(aws: AwsMock) -> None:
    aws.fixture("ensureLogStream.create").fixture("ensureLogStream.exists")
    aws.fixture("ensureLogStream.group-not-found")
    aws.error("CreateLogStream", "OperationAbortedException")
    with client(aws) as c:
        assert c.ensure_log_stream(G, "s1").created is True
        assert c.ensure_log_stream(G, "s1").created is False
        with pytest.raises(sdk.NotFoundError):
            c.ensure_log_stream(G, "s1")
        with pytest.raises(sdk.ConflictError):
            c.ensure_log_stream(G, "s1")
        with pytest.raises(sdk.ValidationError):
            c.ensure_log_stream(G, "bad:stream")
    aws.fixture("ensureLogGroup.create").reply("DescribeLogGroups", {"logGroups": [group()]})
    aws.fixture("ensureLogStream.create")
    with client(aws) as c:
        assert c.ensure_log_stream(G, "x", create_group=True).created


def test_put_retention_policy(aws: AwsMock) -> None:
    aws.fixture("putRetentionPolicy.ok").fixture("putRetentionPolicy.delete")
    aws.fixture("putRetentionPolicy.delete").fixture("putRetentionPolicy.not-found")
    with client(aws) as c:
        c.put_retention_policy(G, 30)
        c.put_retention_policy(G, None)
        c.put_retention_policy(G, 0)
        with pytest.raises(sdk.NotFoundError):
            c.put_retention_policy(G, 7)
        with pytest.raises(sdk.ValidationError):
            c.put_retention_policy(G, 2)


def test_delete_group_and_stream(aws: AwsMock) -> None:
    aws.fixture("deleteLogGroup.ok").fixture("deleteLogGroup.not-found")
    aws.fixture("deleteLogGroup.not-found").fixture("deleteLogGroup.access-denied")
    aws.fixture("deleteLogStream.ok").fixture("deleteLogStream.not-found")
    aws.fixture("deleteLogStream.not-found")
    with client(aws) as c:
        assert c.delete_log_group(G) is True
        assert c.delete_log_group(G, ignore_missing=True) is False
        with pytest.raises(sdk.NotFoundError):
            c.delete_log_group(G)
        with pytest.raises(sdk.PermissionDeniedError):
            c.delete_log_group(G, ignore_missing=True)
        assert c.delete_log_stream(G, "s1") is True
        assert c.delete_log_stream(G, "s1", ignore_missing=True) is False
        with pytest.raises(sdk.NotFoundError):
            c.delete_log_stream(G, "s1")


def test_describe_log_groups_pages_limit_and_validation(aws: AwsMock) -> None:
    aws.fixture("describeLogGroups.page1").fixture("describeLogGroups.page2")
    aws.fixture("describeLogGroups.page1")
    aws.fixture("describeLogGroups.limit-too-big")
    with client(aws) as c:
        groups = list(c.describe_log_groups(prefix="cwlv-0badc0de/", page_size=1))
        assert [g.name for g in groups] == [G, "cwlv-0badc0de/other"]
        assert groups[0].retention_days == 7 and groups[1].kms_key_id
        assert groups[0].metric_filter_count == 0 and groups[0].stored_bytes == 0
        assert len(list(c.describe_log_groups(page_size=1, limit=1))) == 1
        with pytest.raises(sdk.ValidationError):
            list(c.describe_log_groups(page_size=51))
        with pytest.raises(sdk.ValidationError):
            list(c.describe_log_groups(limit=0))
    body = aws.bodies("DescribeLogGroups")
    assert body[1]["nextToken"] and body[0] == {"logGroupNamePrefix": "cwlv-0badc0de/", "limit": 1}


def test_describe_log_groups_error_and_early_break(aws: AwsMock) -> None:
    aws.fixture("describeLogGroups.limit-too-big")
    aws.fixture("describeLogGroups.page1")
    with client(aws) as c:
        it = c.describe_log_groups()
        with pytest.raises(sdk.ValidationError):
            next(it)
        for g in c.describe_log_groups(page_size=1):
            assert g.name == G
            break


def test_describe_log_streams(aws: AwsMock) -> None:
    aws.fixture("describeLogStreams.page1").fixture("describeLogStreams.page2")
    aws.reply("DescribeLogStreams", {"logStreams": [{"logStreamName": "s3"}]})
    aws.fixture("describeLogStreams.last-event-desc")
    aws.reply("DescribeLogStreams", {"logStreams": [], "nextToken": None})
    aws.fixture("describeLogStreams.not-found")
    aws.fixture("describeLogStreams.page1")
    with client(aws) as c:
        names = [s.name for s in c.describe_log_streams(G, page_size=2)]
        assert names == ["big", "bulk", "bulk2", "s1", "s3"]
        desc = list(c.describe_log_streams(G, order_by="LastEventTime", descending=True))
        assert desc and desc[0].last_event_time is not None and desc[0].arn
        with pytest.raises(sdk.NotFoundError):
            list(c.describe_log_streams(G))
        assert len(list(c.describe_log_streams(G, prefix="b", limit=1))) == 1
        with pytest.raises(sdk.ValidationError):
            list(c.describe_log_streams(G, prefix="x", order_by="LastEventTime"))
        with pytest.raises(sdk.ValidationError):
            list(c.describe_log_streams(G, order_by="Size"))  # type: ignore[arg-type]
    bodies = aws.bodies("DescribeLogStreams")
    assert bodies[3]["orderBy"] == "LastEventTime" and bodies[3]["descending"] is True
    assert bodies[-1]["logStreamNamePrefix"] == "b"


def test_tags_and_arn(aws: AwsMock) -> None:
    aws.reply("DescribeLogGroups", {"logGroups": [group()]})
    aws.reply("TagResource", {})
    aws.reply("UntagResource", {})
    aws.reply("DescribeLogGroups", {"logGroups": [group()]})
    aws.fixture("ensureLogGroup.list-tags")
    aws.reply("DescribeLogGroups", {"logGroups": []})
    aws.reply("DescribeLogGroups", {"logGroups": [group()]})
    with client(aws) as c:
        c.tag_log_group(G, {"a": "1"})
        c.untag_log_group(ARN + ":*", ["a"])
        assert c.list_log_group_tags(G) == {"team": "sdk", "env": "test"}
        with pytest.raises(sdk.NotFoundError):
            c.list_log_group_tags("missing")
        assert c.log_group_arn(G) == ARN
        assert c.log_group_arn(ARN + ":*") == ARN
    assert aws.bodies("UntagResource")[0] == {"resourceArn": ARN, "tagKeys": ["a"]}
    with client(aws, account_id="123456789012") as c:
        assert c.log_group_arn(G) == ARN
    with client(aws, account_id="123456789012", region="cn-north-1") as c:
        assert c.log_group_arn(G).startswith("arn:aws-cn:logs:cn-north-1:")


# --------------------------------------------------------------------------- ingest


def test_put_log_events_sorts_and_reports(aws: AwsMock) -> None:
    aws.fixture("putLogEvents.ok")
    aws.fixture("putLogEvents.rejected-too-old-too-new")
    now = datetime.now(timezone.utc)
    with client(aws) as c:
        res = c.put_log_events(
            G,
            "s1",
            [
                sdk.InputLogEvent("b", now),
                {"message": "a", "timestamp": now - timedelta(seconds=1)},
                sdk.InputLogEvent("c"),
            ],
        )
        assert (res.accepted, res.rejected, res.batches) == (3, 0, 1)
        sent = aws.bodies("PutLogEvents")[0]
        assert [e["message"] for e in sent["logEvents"]][:2] == ["a", "b"]
        assert "sequenceToken" not in sent
        res = c.put_log_events(G, "s3", [{"message": m, "timestamp": 1} for m in "xyz"])
        assert (res.accepted, res.rejected) == (1, 2)
        assert res.rejected_info == [
            sdk.RejectedLogEventsInfo(
                batch=0, too_old_log_event_end_index=0, too_new_log_event_start_index=2
            )
        ]


def test_put_log_events_expired_index(aws: AwsMock) -> None:
    aws.reply(
        "PutLogEvents",
        {"nextSequenceToken": "1", "rejectedLogEventsInfo": {"expiredLogEventEndIndex": 1}},
    )
    with client(aws) as c:
        res = c.put_log_events(G, "s1", [{"message": "a", "timestamp": i} for i in range(4)])
    assert (res.accepted, res.rejected) == (2, 2)
    assert res.rejected_info[0].expired_log_event_end_index == 1


def test_put_log_events_auto_create(aws: AwsMock) -> None:
    aws.fixture("putLogEvents.stream-not-found").fixture("ensureLogStream.create")
    aws.fixture("putLogEvents.ok")
    with client(aws) as c:
        assert c.put_log_events(G, "s9", [sdk.InputLogEvent("x", 1)]).accepted == 1
    assert aws.ops() == ["PutLogEvents", "CreateLogStream", "PutLogEvents"]
    # group missing: the original error is raised
    aws.fixture("putLogEvents.group-not-found").fixture("ensureLogStream.group-not-found")
    with client(aws) as c, pytest.raises(sdk.NotFoundError):
        c.put_log_events(G, "s9", [sdk.InputLogEvent("x", 1)])
    # auto_create off
    aws.fixture("putLogEvents.stream-not-found")
    with client(aws, auto_create=False) as c, pytest.raises(sdk.NotFoundError):
        c.put_log_events(G, "s9", [sdk.InputLogEvent("x", 1)])
    # create errors other than not found propagate
    aws.fixture("putLogEvents.stream-not-found").fixture(
        "ensureLogGroup.access-denied", op="CreateLogStream"
    )
    with client(aws) as c, pytest.raises(sdk.PermissionDeniedError):
        c.put_log_events(G, "s9", [sdk.InputLogEvent("x", 1)])
    # create_group creates the group too
    aws.fixture("putLogEvents.group-not-found").fixture("ensureLogGroup.create")
    aws.reply("DescribeLogGroups", {"logGroups": [group()]}).fixture("ensureLogStream.create")
    aws.fixture("putLogEvents.ok")
    with client(aws) as c:
        assert c.put_log_events(G, "s9", [sdk.InputLogEvent("x", 1)], create_group=True).accepted


def test_put_log_events_size_policies(aws: AwsMock) -> None:
    aws.fixture("putLogEvents.ok").fixture("putLogEvents.ok")
    big = "é" * 200
    with client(aws, max_event_bytes=100) as c:
        res = c.put_log_events(G, "s1", [sdk.InputLogEvent(big, 1)])
        assert res.truncated == 1
        sent = aws.bodies("PutLogEvents")[0]["logEvents"][0]["message"]
        assert len(sent.encode()) <= 100 and "truncated" in sent
        res = c.put_log_events(
            G, "s1", [sdk.InputLogEvent(big, 1), sdk.InputLogEvent("ok", 2)], oversize_policy="drop"
        )
        assert (res.dropped, res.accepted) == (1, 1)
        with pytest.raises(sdk.ValidationError):
            c.put_log_events(G, "s1", [sdk.InputLogEvent(big, 1)], oversize_policy="error")
        res = c.put_log_events(G, "s1", [sdk.InputLogEvent(big, 1)], oversize_policy="drop")
        assert (res.batches, res.dropped) == (0, 1)
        with pytest.raises(sdk.ValidationError):
            c.put_log_events("bad name", "s1", [])
        with pytest.raises(sdk.ValidationError):
            c.put_log_events(G, "s:1", [])


def test_put_log_events_access_denied(aws: AwsMock) -> None:
    aws.fixture("putLogEvents.access-denied")
    with client(aws) as c, pytest.raises(sdk.PermissionDeniedError):
        c.put_log_events(G, "s1", [sdk.InputLogEvent("x", 1)])


def test_emf(aws: AwsMock) -> None:
    aws.fixture("emf.put")
    when = datetime(2026, 11, 18, tzinfo=timezone.utc)
    with client(aws) as c:
        doc = c.emf(
            "latency",
            42,
            {"service": "api"},
            namespace="GrowSdk",
            unit="Milliseconds",
            timestamp=when,
            properties={"marker": "m"},
        )
    assert doc["latency"] == 42 and doc["service"] == "api" and doc["marker"] == "m"
    body = aws.bodies("PutLogEvents")[0]
    assert body["logGroupName"] == "app/main" and body["logStreamName"] == "s1"
    assert body["logEvents"][0]["timestamp"] == int(when.timestamp() * 1000)
    aws.reply("PutLogEvents", {"rejectedLogEventsInfo": {"tooOldLogEventEndIndex": 0}})
    with client(aws) as c, pytest.raises(sdk.ValidationError, match="rejected"):
        c.emf("m", 1, group="g", stream="st")
    with client(aws, log_group=None) as c, pytest.raises(sdk.ConfigError):
        c.emf("m", 1)


# --------------------------------------------------------------------------- query


def test_filter_log_events_pages_and_page_api(aws: AwsMock) -> None:
    aws.fixture("filterLogEvents.page1").fixture("filterLogEvents.page2")
    aws.fixture("filterLogEvents.page1")
    aws.fixture("filterLogEvents.page1")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with client(aws) as c:
        events = list(
            c.filter_log_events(
                group=G, streams=["s1"], page_size=2, pattern="x", start=start, end=5
            )
        )
        assert [e.event_id for e in events] == ["3", "4", "5"]
        assert events[0].log_stream_name == "s1" and events[0].ingestion_time
        assert len(list(c.filter_log_events(group=G, limit=1))) == 1
        page = c.filter_log_events_page(group=ARN + ":*", stream_prefix="s", next_token="t")
        assert page.next_token == "cwlv-0badc0de/app@s1@4" and len(page.events) == 2
    bodies = aws.bodies("FilterLogEvents")
    assert bodies[0] == {
        "logGroupName": G,
        "logStreamNames": ["s1"],
        "filterPattern": "x",
        "startTime": int(start.timestamp() * 1000),
        "endTime": 5,
        "limit": 2,
    }
    assert bodies[2]["limit"] == 1
    assert bodies[3] == {"logGroupIdentifier": ARN, "logStreamNamePrefix": "s", "nextToken": "t"}


def test_filter_log_events_sorts_and_validates(aws: AwsMock) -> None:
    aws.reply(
        "FilterLogEvents",
        {
            "events": [
                {"eventId": "b", "timestamp": 2, "message": "2"},
                {"eventId": "a", "timestamp": 2, "message": "1"},
                {"eventId": "c", "timestamp": 1, "message": "0"},
            ],
        },
    )
    aws.fixture("filterLogEvents.not-found")
    with client(aws) as c:
        assert [e.message for e in c.filter_log_events()] == ["0", "1", "2"]
        with pytest.raises(sdk.NotFoundError):
            list(c.filter_log_events())
        for kwargs in (
            {"streams": ["a"], "stream_prefix": "b"},
            {"streams": []},
            {"page_size": 0},
            {"limit": 0},
            {"start": "yesterday"},
            {"end": float("nan")},
        ):
            with pytest.raises(sdk.ValidationError):
                list(c.filter_log_events(**kwargs))  # type: ignore[arg-type]
    with client(aws, log_group=None) as c, pytest.raises(sdk.ConfigError):
        list(c.filter_log_events())


def test_get_log_events_forward_backward(aws: AwsMock) -> None:
    aws.fixture("getLogEvents.head").fixture("getLogEvents.forward")
    aws.fixture("getLogEvents.end-of-stream")
    aws.fixture("getLogEvents.tail")
    aws.reply(
        "GetLogEvents", {"events": [{"timestamp": 1, "message": "old"}], "nextBackwardToken": "b/0"}
    )
    aws.reply("GetLogEvents", {"events": [], "nextBackwardToken": "b/0"})
    aws.fixture("getLogEvents.head")
    with client(aws) as c:
        head = list(c.get_log_events(G, "s1", start_from_head=True, page_size=2))
        assert [e.message for e in head][2] == "plain info line" and len(head) == 3
        back = list(c.get_log_events(G, "s1", page_size=1, start=0, end=datetime.now(timezone.utc)))
        assert [e.message for e in back] == ["plain info line", "old"]
        assert len(list(c.get_log_events(G, "s1", start_from_head=True, limit=1))) == 1
        with pytest.raises(sdk.ValidationError):
            list(c.get_log_events(G, "s1", page_size=10_001))
    bodies = aws.bodies("GetLogEvents")
    assert bodies[1]["nextToken"] == "f/00000000000000000000000000000000000000000000000000000001"
    assert bodies[3]["startFromHead"] is False and bodies[3]["startTime"] == 0
    assert bodies[4]["nextToken"] == "b/00000000000000000000000000000000000000000000000000000002"


def test_get_log_events_not_found(aws: AwsMock) -> None:
    aws.fixture("getLogEvents.stream-not-found").fixture("getLogEvents.group-not-found")
    with client(aws) as c:
        for _ in range(2):
            with pytest.raises(sdk.NotFoundError):
                list(c.get_log_events(G, "nope"))


def test_start_query_and_results(aws: AwsMock) -> None:
    aws.fixture("startQuery.ok").fixture("getQueryResults.complete")
    aws.fixture("startQuery.not-found")
    aws.reply("StartQuery", {"queryId": "q-arn"})
    aws.reply("StartQuery", {})
    start = datetime(2026, 11, 18, 0, 0, 0, 500000, tzinfo=timezone.utc)
    with client(aws) as c:
        qid = c.start_query(
            "fields @message",
            groups=[G],
            start=start,
            end=start + timedelta(seconds=10, milliseconds=1),
            limit=5,
        )
        assert qid == "9e4e6e71-0edf-1bf2-b267-9c7c4ffdc4e4"
        res = c.get_query_results(qid)
        assert res.status == "Complete" and res.rows[0]["@message"] == "accepted"
        assert res.statistics is None
        with pytest.raises(sdk.NotFoundError):
            c.start_query("fields @message")
        assert c.start_query("fields @message", groups=[ARN + ":*"]) == "q-arn"
        with pytest.raises(sdk.UnavailableError):
            c.start_query("fields @message")
        for kwargs in ({"groups": []}, {"limit": 0}, {"start": start, "end": start - timedelta(1)}):
            with pytest.raises(sdk.ValidationError):
                c.start_query("x", **kwargs)  # type: ignore[arg-type]
        with pytest.raises(sdk.ValidationError):
            c.start_query("  ")
        with pytest.raises(sdk.ValidationError):
            c.get_query_results("")
    body = aws.bodies("StartQuery")
    assert (
        body[0]["startTime"] == int(start.timestamp())
        and body[0]["endTime"] == int(start.timestamp()) + 11
    )
    assert body[0]["logGroupNames"] == [G] and body[0]["limit"] == 5
    assert body[1]["logGroupNames"] == ["app/main"]
    assert body[1]["endTime"] - body[1]["startTime"] in (3600, 3601)
    assert body[2]["logGroupIdentifiers"] == [ARN]


def test_get_query_results_statistics_and_moto_500(aws: AwsMock) -> None:
    aws.reply(
        "GetQueryResults",
        {
            "status": "Running",
            "results": [[{"field": "@message", "value": "x"}, {"value": "no-field"}]],
            "statistics": {"recordsMatched": 1.0, "recordsScanned": 2.0, "bytesScanned": 3.0},
        },
    )
    aws.fixture("getQueryResults.unknown-id-moto-500")
    with client(aws) as c:
        res = c.get_query_results("q")
        assert res.rows == [{"@message": "x"}]
        assert res.statistics == sdk.QueryStatistics(1.0, 2.0, 3.0)
        with pytest.raises(sdk.UnavailableError):
            c.get_query_results("nope")


def test_stop_query(aws: AwsMock) -> None:
    aws.reply("StopQuery", {"success": True}).reply("StopQuery", {"success": False})
    aws.fixture("stopQuery.moto-500")
    with client(aws) as c:
        assert c.stop_query("q") is True
        assert c.stop_query("q") is False
        with pytest.raises(sdk.UnavailableError):
            c.stop_query("q")
        with pytest.raises(sdk.ValidationError):
            c.stop_query("")


def running(n: int = 1) -> list[dict[str, Any]]:
    return [{"status": "Running", "results": []}] * n


def test_query_polls_until_complete(aws: AwsMock) -> None:
    slept: list[float] = []
    aws.reply("StartQuery", {"queryId": "q1"})
    for body in running(3):
        aws.reply("GetQueryResults", body)
    aws.fixture("getQueryResults.complete")
    internals = sdk.ClientInternals(inject_host_prefix=False, sleep=slept.append)
    with client(aws, internals=internals) as c:
        res = c.query("fields @message", groups=[G], poll_interval_s=timedelta(seconds=2))
    assert res.status == "Complete" and res.query_id == "q1"
    assert slept == [2.0, 4.0, 5.0]


@pytest.mark.parametrize(
    ("status", "error"),
    [
        ("Failed", sdk.UnavailableError),
        ("Unknown", sdk.UnavailableError),
        ("Timeout", sdk.TimeoutError),
        ("Cancelled", sdk.CancelledError),
    ],
)
def test_query_terminal_failures(aws: AwsMock, status: str, error: type[Exception]) -> None:
    aws.reply("StartQuery", {"queryId": "q1"}).reply("GetQueryResults", {"status": status})
    with client(aws) as c, pytest.raises(error, match="q1") as info:
        c.query("fields @message")
    if status == "Failed":
        assert info.value.retryable is False  # type: ignore[attr-defined]


def test_query_deadline_stops_query(aws: AwsMock) -> None:
    aws.reply("StartQuery", {"queryId": "q1"})
    aws.reply("GetQueryResults", running()[0])
    aws.fixture("stopQuery.moto-500")
    with client(aws) as c, pytest.raises(sdk.TimeoutError, match="not complete"):
        c.query("fields @message", timeout_s=0)
    assert aws.ops()[-1] == "StopQuery"


def test_query_retries_limit_exceeded(aws: AwsMock) -> None:
    slept: list[float] = []
    aws.error("StartQuery", "LimitExceededException").error("StartQuery", "LimitExceededException")
    aws.reply("StartQuery", {"queryId": "q1"}).fixture("getQueryResults.complete")
    internals = sdk.ClientInternals(inject_host_prefix=False, sleep=slept.append)
    with client(aws, internals=internals) as c:
        assert c.query("fields @message").status == "Complete"
    assert slept == [1.0, 2.0]
    aws.error("StartQuery", "LimitExceededException")
    with client(aws, internals=internals) as c, pytest.raises(sdk.ValidationError):
        c.query("fields @message", retry_on_query_limit=1)
    aws.reply("StartQuery", {})
    with client(aws) as c, pytest.raises(sdk.UnavailableError):
        c.query("fields @message")


def test_query_cancel_stops_query(aws: AwsMock) -> None:
    aws.reply("StartQuery", {"queryId": "q1"})
    aws.reply("GetQueryResults", running()[0])
    aws.reply("StopQuery", {"success": True})
    token = sdk.CancelToken()
    internals = sdk.ClientInternals(inject_host_prefix=False, sleep=lambda s: token.cancel())
    with client(aws, internals=internals) as c, pytest.raises(sdk.CancelledError):
        c.query("fields @message", cancel=token)
    assert aws.ops() == ["StartQuery", "GetQueryResults", "StopQuery"]


def test_query_cancel_during_poll_request(aws: AwsMock) -> None:
    aws.reply("StartQuery", {"queryId": "q1"})
    aws.reply("GetQueryResults", running()[0], delay=1.0)
    aws.reply("StopQuery", {"success": True})
    token = sdk.CancelToken()
    threading.Timer(0.3, token.cancel).start()
    with client(aws) as c, pytest.raises(sdk.CancelledError):
        c.query("fields @message", cancel=token)
    time.sleep(0.9)
    assert "StopQuery" in aws.ops()


def test_query_real_sleep_with_cancel_token(aws: AwsMock) -> None:
    aws.reply("StartQuery", {"queryId": "q1"})
    aws.reply("GetQueryResults", running()[0])
    aws.fixture("getQueryResults.complete")
    with sdk.CloudWatchLogsClient(aws_config(aws.url)) as c:
        res = c.query("fields @message", poll_interval_s=0.01, cancel=sdk.CancelToken())
        assert res.status == "Complete"
    aws.reply("StartQuery", {"queryId": "q1"})
    aws.reply("GetQueryResults", running()[0])
    aws.fixture("getQueryResults.complete")
    with sdk.CloudWatchLogsClient(aws_config(aws.url)) as c:
        assert c.query("fields @message", poll_interval_s=0.01).status == "Complete"


def test_describe_queries(aws: AwsMock) -> None:
    aws.fixture("describeQueries.ok")
    with client(aws) as c:
        queries = c.describe_queries(group=G, status="Complete", max_results=10)
        assert queries[0].query_id == "9e4e6e71-0edf-1bf2-b267-9c7c4ffdc4e4"
        assert queries[0].create_time is not None and queries[0].log_group_name == G
        with pytest.raises(sdk.ValidationError):
            c.describe_queries(max_results=0)
    assert aws.bodies("DescribeQueries")[0] == {
        "logGroupName": G,
        "status": "Complete",
        "maxResults": 10,
    }
