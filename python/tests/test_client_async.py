"""AsyncCloudWatchLogsClient (aioboto3 / aiobotocore) against the local AWS JSON mock."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

import grow_aws_cloudwatch_logs as sdk
from _support import INTERNALS, TEST_SECRET, AwsMock, AwsReply, aws_config, spawn_capturing

G = "cwlv-0badc0de/app"
ARN = "arn:aws:logs:us-east-1:123456789012:log-group:" + G


def client(aws: AwsMock, **kw: Any) -> sdk.AsyncCloudWatchLogsClient:
    internals = kw.pop("internals", INTERNALS)
    url = kw.pop("url", aws.url)
    return sdk.AsyncCloudWatchLogsClient(aws_config(url, **kw), internals=internals)


def group(name: str = G, **extra: Any) -> dict[str, Any]:
    return {
        "logGroupName": name,
        "arn": f"arn:aws:logs:us-east-1:123456789012:log-group:{name}:*",
        "logGroupArn": f"arn:aws:logs:us-east-1:123456789012:log-group:{name}",
        "creationTime": 1795034976104,
        **extra,
    }


async def test_core_and_close(aws: AwsMock) -> None:
    aws.fixture("ping.ok").fixture("ping.ok")
    seen: list[Any] = []
    hooks = sdk.Hooks(on_request=seen.append, on_response=seen.append)
    async with client(aws, hooks=hooks, headers={"X-Extra": "2"}) as c:
        assert (await c.health()).healthy
        assert await c.ping() >= 0
        with pytest.raises(sdk.UnsupportedError):
            await c.server_version()
        assert await c.get_region() == "us-east-1"
        assert await c.get_sdk() is await c.get_sdk()
        assert "AsyncCloudWatchLogsClient(" in repr(c) and TEST_SECRET not in repr(c)
    assert aws.seen[-1].headers["X-Extra"] == "2"
    assert any(isinstance(s, sdk.ResponseInfo) for s in seen)
    await c.aclose()
    with pytest.raises(sdk.CancelledError, match="closed"):
        await c.ping()


async def test_health_variants(aws: AwsMock) -> None:
    aws.fixture("ping.auth-unknown-key").fixture(
        "deleteLogGroup.access-denied", op="DescribeLogGroups"
    )
    aws.error("DescribeLogGroups", "ThrottlingException")
    aws.fixture("stopQuery.moto-500", op="DescribeLogGroups")
    async with client(aws) as c:
        assert (await c.health()).details == {"auth": "failed", "code": "AUTHENTICATION"}
        assert (await c.health()).details["code"] == "PERMISSION_DENIED"
        assert (await c.health()).status == "degraded"
        assert (await c.health()).status == "unhealthy"
    async with client(aws, url="http://127.0.0.1:9") as c:
        assert (await c.health()).status == "unreachable"


async def test_ping_errors(aws: AwsMock) -> None:
    aws.fixture("ping.auth-bad-signature")
    aws.fixture("deleteLogGroup.access-denied", op="DescribeLogGroups")
    async with client(aws) as c:
        with pytest.raises(sdk.AuthenticationError) as info:
            await c.ping()
        assert info.value.request_id
        with pytest.raises(sdk.PermissionDeniedError):
            await c.ping()


async def test_total_timeout_and_task_cancel(aws: AwsMock) -> None:
    aws.reply("DescribeLogGroups", {"logGroups": []}, delay=1.0)
    async with client(aws, total_timeout=0.2) as c:
        with pytest.raises(sdk.TimeoutError):
            await c.ping()
    aws.reply("DescribeLogGroups", {"logGroups": []}, delay=1.0)
    sink: list[BaseException] = []
    async with client(aws) as c:
        task = spawn_capturing(c.ping(), sink)
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert isinstance(sink[0], sdk.CancelledError)


async def test_from_env_role_and_config_errors(
    aws: AwsMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = {
        "GROW_AWS_CLOUDWATCH_LOGS_URL": aws.url,
        "GROW_AWS_CLOUDWATCH_LOGS_REGION": "eu-west-1",
        "GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID": "AKIAENV",
        "GROW_AWS_CLOUDWATCH_LOGS_SECRET_ACCESS_KEY": "env-secret-value",
    }
    async with sdk.AsyncCloudWatchLogsClient.from_env(env) as c:
        assert await c.get_region() == "eu-west-1"
    cfg = aws_config(aws.url, role_arn="arn:aws:iam::123456789012:role/r", external_id="ext-1234")
    async with sdk.AsyncCloudWatchLogsClient(cfg, internals=INTERNALS) as c:
        raw = await c.get_sdk()
        assert (
            type(raw._request_signer._credentials).__name__ == "AioDeferredRefreshableCredentials"
        )
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    async with client(aws, region=None) as c:
        with pytest.raises(sdk.ConfigError):
            await c.ping()


async def test_caller_supplied_client(aws: AwsMock) -> None:
    from aiobotocore.session import AioSession
    from botocore.config import Config

    context = AioSession().create_client(
        "logs",
        endpoint_url=aws.url,
        region_name="us-east-1",
        aws_access_key_id="AKIA",
        aws_secret_access_key="x",
        config=Config(inject_host_prefix=False),
    )
    raw = await context.__aenter__()
    aws.fixture("ping.ok").fixture("ping.ok")
    c = sdk.AsyncCloudWatchLogsClient(sdk.CloudWatchLogsConfig(sdk_client=raw))
    assert await c.get_sdk() is raw
    assert (await c.health()).healthy
    await c.aclose()
    await raw.describe_log_groups()
    await context.__aexit__(None, None, None)


async def test_bootstrap_functions(aws: AwsMock) -> None:
    aws.fixture("ensureLogGroup.create").reply("DescribeLogGroups", {"logGroups": [group()]})
    aws.fixture("putRetentionPolicy.ok")
    aws.fixture("ensureLogGroup.exists").reply("DescribeLogGroups", {"logGroups": [group()]})
    aws.fixture("ensureLogGroup.tag-reconcile")
    aws.fixture("ensureLogStream.create").fixture("ensureLogStream.exists")
    aws.fixture("putRetentionPolicy.delete")
    aws.fixture("deleteLogStream.ok").fixture("deleteLogStream.not-found")
    aws.fixture("deleteLogGroup.ok").fixture("deleteLogGroup.not-found")
    aws.reply("DescribeLogGroups", {"logGroups": [group()]})
    async with client(aws) as c:
        res = await c.ensure_log_group(G, retention_days=7)
        assert res == sdk.EnsureLogGroupResult(True, ARN)
        assert (await c.ensure_log_group(G, tags={"a": "b"})).created is False
        assert (await c.ensure_log_stream(G, "s1")).created
        assert not (await c.ensure_log_stream(G, "s1")).created
        await c.put_retention_policy(G, None)
        assert await c.delete_log_stream(G, "s1")
        assert not await c.delete_log_stream(G, "s1", ignore_missing=True)
        assert await c.delete_log_group(G)
        assert not await c.delete_log_group(G, ignore_missing=True)
        info = await c.get_log_group(G)
        assert info is not None and info.arn == ARN


async def test_iterators(aws: AwsMock) -> None:
    aws.fixture("describeLogGroups.page1").fixture("describeLogGroups.page2")
    aws.fixture("describeLogGroups.limit-too-big")
    aws.fixture("describeLogStreams.page1")
    aws.fixture("filterLogEvents.page1").fixture("filterLogEvents.page2")
    aws.fixture("getLogEvents.head").fixture("getLogEvents.forward")
    aws.fixture("getLogEvents.end-of-stream")
    async with client(aws) as c:
        names = [g.name async for g in c.describe_log_groups(prefix="cwlv-0badc0de/", page_size=1)]
        assert names == [G, "cwlv-0badc0de/other"]
        with pytest.raises(sdk.ValidationError):
            [g async for g in c.describe_log_groups()]
        async for s in c.describe_log_streams(G, page_size=2, order_by="LogStreamName"):
            assert s.name == "big"
            break
        events = [e async for e in c.filter_log_events(group=G, streams=["s1"], page_size=2)]
        assert [e.event_id for e in events] == ["3", "4", "5"]
        head = [e async for e in c.get_log_events(G, "s1", start_from_head=True, page_size=2)]
        assert len(head) == 3


async def test_tags_arn_ingest_emf(aws: AwsMock) -> None:
    aws.reply("DescribeLogGroups", {"logGroups": [group()]}).reply("TagResource", {})
    aws.reply("UntagResource", {}).fixture("ensureLogGroup.list-tags")
    aws.fixture("putLogEvents.stream-not-found").fixture("ensureLogStream.create")
    aws.fixture("putLogEvents.ok").fixture("emf.put")
    aws.fixture("filterLogEvents.page2")
    async with client(aws, account_id="123456789012") as c:
        assert await c.log_group_arn(G) == ARN
        await c.tag_log_group(G, {"a": "1"})
        await c.untag_log_group(ARN, ["a"])
        assert await c.list_log_group_tags(ARN) == {"team": "sdk", "env": "test"}
        res = await c.put_log_events(G, "s1", [sdk.InputLogEvent("x", 1)])
        assert res.accepted == 1
        doc = await c.emf("latency", [1.0, 2.0], {"svc": "a"}, timestamp=1795034915000)
        assert doc["latency"] == [1.0, 2.0]
        page = await c.filter_log_events_page(group=G, next_token="t")
        assert page.next_token is None and page.events[0].message == "plain info line"


async def test_query_functions(aws: AwsMock) -> None:
    aws.fixture("startQuery.ok").fixture("getQueryResults.complete")
    aws.reply("StopQuery", {"success": True}).fixture("describeQueries.ok")
    aws.reply("StartQuery", {"queryId": "q1"}).reply("GetQueryResults", {"status": "Running"})
    aws.fixture("getQueryResults.complete")
    async with client(aws) as c:
        qid = await c.start_query("fields @message", groups=[G], start=0, end=1000)
        assert (await c.get_query_results(qid)).status == "Complete"
        assert await c.stop_query(qid)
        assert (await c.describe_queries(group=G))[0].query_id == qid
        res = await c.query("fields @message", poll_interval_s=0.01)
        assert res.status == "Complete"


async def test_query_real_sleep_and_cancel(aws: AwsMock) -> None:
    aws.reply("StartQuery", {"queryId": "q1"}).reply("GetQueryResults", {"status": "Running"})
    aws.fixture("getQueryResults.complete")
    async with sdk.AsyncCloudWatchLogsClient(aws_config(aws.url)) as c:
        assert (await c.query("fields @message", poll_interval_s=0.01)).status == "Complete"
    aws.reply("StartQuery", {"queryId": "q2"}).reply("GetQueryResults", {"status": "Running"})
    aws.reply("StopQuery", {"success": True})
    sink: list[BaseException] = []
    async with sdk.AsyncCloudWatchLogsClient(aws_config(aws.url)) as c:
        task = spawn_capturing(c.query("fields @message", poll_interval_s=5), sink)
        for _ in range(100):
            await asyncio.sleep(0.02)
            if "GetQueryResults" in aws.ops():
                break
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert isinstance(sink[0], sdk.CancelledError)
    assert aws.ops()[-1] == "StopQuery"


async def test_retry_on_throttling_and_error_hook(aws: AwsMock) -> None:
    errors: list[Any] = []
    aws.error("DescribeLogGroups", "ThrottlingException").fixture("ping.ok")
    async with client(aws, retry=sdk.RetryPolicy(max_retries=1)) as c:
        assert await c.ping() >= 0
    async with client(aws, url="http://127.0.0.1:9", hooks=sdk.Hooks(on_error=errors.append)) as c:
        with pytest.raises(sdk.UnavailableError):
            await c.ping()
    assert errors and isinstance(errors[0].error, sdk.UnavailableError)


async def test_drop_connection_maps_to_unavailable(aws: AwsMock) -> None:
    aws.replies["DescribeLogGroups"].append(AwsReply(drop=True))
    async with client(aws) as c:
        with pytest.raises((sdk.UnavailableError, sdk.TimeoutError)):
            await c.ping()


def test_sync_drop_connection(aws: AwsMock) -> None:
    aws.replies["DescribeLogGroups"].append(AwsReply(drop=True))
    with sdk.CloudWatchLogsClient(aws_config(aws.url), internals=INTERNALS) as c:
        with pytest.raises(sdk.UnavailableError):
            c.ping()


async def test_internal_sleep_hook(aws: AwsMock) -> None:
    slept: list[float] = []
    aws.reply("StartQuery", {"queryId": "q1"}).reply("GetQueryResults", {"status": "Running"})
    aws.fixture("getQueryResults.complete")
    internals = sdk.ClientInternals(inject_host_prefix=False, sleep=slept.append)
    async with client(aws, internals=internals) as c:
        await c.query("fields @message", end=datetime.now(timezone.utc))
    assert slept == [1.0]
