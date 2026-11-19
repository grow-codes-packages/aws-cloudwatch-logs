"""Logging category: exporter, log client, query client (search asc/desc, tail, streams)."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import grow_aws_cloudwatch_logs as sdk
from _support import INTERNALS, AwsMock, aws_config, event, session_start, session_update
from grow_aws_cloudwatch_logs import _ops

G = "app/main"
END = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
END_MS = int(END.timestamp() * 1000)


def sync_client(aws: AwsMock, **kw: Any) -> sdk.CloudWatchLogsClient:
    return sdk.CloudWatchLogsClient(aws_config(aws.url, **kw), internals=INTERNALS)


def async_client(aws: AwsMock, **kw: Any) -> sdk.AsyncCloudWatchLogsClient:
    return sdk.AsyncCloudWatchLogsClient(aws_config(aws.url, **kw), internals=INTERNALS)


def ev(i: int, ts: int, message: str | None = None) -> dict[str, Any]:
    return {
        "eventId": f"{i:03d}",
        "timestamp": ts,
        "message": message or f"m{i}",
        "logStreamName": "s1",
    }


def cursor(data: dict[str, Any]) -> str:
    return (
        base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode())
        .decode()
        .rstrip("=")
    )


# --------------------------------------------------------------------------- export


def test_log_client_sends_normalized_json(aws: AwsMock) -> None:
    aws.fixture("putLogEvents.stream-not-found").fixture("ensureLogStream.create")
    aws.fixture("putLogEvents.ok")
    c = sync_client(aws)
    logs = c.log_client(
        resource={"service.name": "svc"}, options=sdk.BatchOptions(flush_interval_s=60)
    )
    logs.logger("it").info("hello", {"password": "hunter2", "n": 1})
    logs.flush(5)
    assert logs.stats().sent == 1
    body = aws.bodies("PutLogEvents")[-1]
    assert (body["logGroupName"], body["logStreamName"]) == (G, "s1")
    record = json.loads(body["logEvents"][0]["message"])
    assert record["body"] == "hello" and record["resource"] == {"service.name": "svc"}
    assert record["attributes"]["password"] == "[REDACTED]"
    c.close()  # flushes and closes the log client too
    c.close()


def test_exporter_rejections_and_oversize(aws: AwsMock) -> None:
    aws.reply("PutLogEvents", {"rejectedLogEventsInfo": {"tooOldLogEventEndIndex": 0}})
    with sync_client(aws, max_event_bytes=200, oversize_policy="drop") as c:
        exp = c.exporter(group="g/x", stream="st", create_group=True)
        records = [
            sdk.LogRecord(body="ok", timestamp=END),
            sdk.LogRecord(body="x" * 500, timestamp=END),
        ]
        result = exp.export(records, gzip=True, timeout_s=5)
        assert (result.accepted, result.rejected) == (0, 2)
        assert isinstance(result.error, sdk.ValidationError)
        assert exp.export([], gzip=False, timeout_s=1) == sdk.ExportResult(accepted=0)
        exp.shutdown()
        with pytest.raises(sdk.ConfigError):
            sdk.CloudWatchLogsClient(aws_config(aws.url, log_group=None)).exporter()


async def test_async_log_client_and_exporter(aws: AwsMock) -> None:
    aws.fixture("putLogEvents.ok").fixture("putLogEvents.ok")
    c = async_client(aws)
    logs = c.log_client(options=sdk.BatchOptions(flush_interval_s=60))
    logs.send(sdk.LogInput("async hello", severity=sdk.Severity.ERROR))
    await logs.flush(5)
    assert logs.stats().sent == 1
    record = json.loads(aws.bodies("PutLogEvents")[0]["logEvents"][0]["message"])
    assert record["severityText"] == "ERROR"
    exp = c.exporter(stream="other")
    result = await exp.export([sdk.LogRecord(body="b", timestamp=END)], gzip=True, timeout_s=5)
    assert result == sdk.ExportResult(accepted=1)
    big = sdk.LogRecord(body="x" * 300_000, timestamp=END)
    async with async_client(aws, oversize_policy="error") as c2:
        assert (await c2.exporter().export([big], gzip=False, timeout_s=1)).rejected == 1
    await exp.shutdown()
    await c.aclose()


# --------------------------------------------------------------------------- search asc


def test_search_asc_pages_and_cursor(aws: AwsMock) -> None:
    aws.reply("FilterLogEvents", {"events": [ev(1, 1)], "nextToken": "t1"})
    aws.reply("FilterLogEvents", {"events": [], "nextToken": "t2"})
    aws.reply("FilterLogEvents", {"events": [ev(2, 2), ev(3, 3)], "nextToken": "t3"})
    aws.reply("FilterLogEvents", {"events": [ev(4, 4)]})
    with sync_client(aws) as c:
        q = c.log_query_client()
        page = q.search("needle", order="asc", limit=2, start=END - timedelta(hours=1), end=END)
        assert [r.body for r in page.records] == ["m1", "m2"]
        assert page.next_cursor is not None
        rest = q.search("needle", order="asc", limit=2, cursor=page.next_cursor)
        assert [r.body for r in rest.records] == ["m4"] and rest.next_cursor is None
        assert rest.records[0].attributes["aws.log.stream"] == "s1"
    bodies = aws.bodies("FilterLogEvents")
    assert bodies[0]["filterPattern"] == '"needle"' and bodies[0]["limit"] == 2
    assert bodies[1]["nextToken"] == "t1" and bodies[3]["nextToken"] == "t3"


def test_search_validation(aws: AwsMock) -> None:
    with sync_client(aws) as c:
        q = c.log_query_client(group="other/group")
        assert q.group == "other/group"
        for kwargs in (
            {"limit": 0},
            {"limit": True},
            {"order": "sideways"},
            {"cursor": "!!!"},
            {"cursor": cursor({"o": "d", "e": 1, "w": 0, "k": 0})},
            {"cursor": cursor({"o": "a", "t": "x"}), "order": "desc"},
        ):
            with pytest.raises(sdk.ValidationError):
                q.search("x", **kwargs)
        with pytest.raises(sdk.UnsupportedError):
            q.list_labels()


# --------------------------------------------------------------------------- search desc


def test_search_desc_slices_newest_first(aws: AwsMock) -> None:
    slice1 = {"events": [ev(1, END_MS - 60_000), ev(2, END_MS - 30_000)]}
    slice2 = {"events": [ev(3, END_MS - 400_000), ev(4, END_MS - 350_000)]}
    aws.reply("FilterLogEvents", slice1).reply("FilterLogEvents", slice2)
    aws.reply("FilterLogEvents", slice2)
    start = END - timedelta(minutes=10)
    with sync_client(aws) as c:
        q = c.log_query_client()
        page = q.search("", start=start, end=END, limit=3)
        assert [r.body for r in page.records] == ["m2", "m1", "m4"]
        assert page.next_cursor is not None
        rest = q.search("", start=start, end=END, limit=3, cursor=page.next_cursor)
        assert [r.body for r in rest.records] == ["m3"] and rest.next_cursor is None
    bodies = aws.bodies("FilterLogEvents")
    assert "filterPattern" not in bodies[0]
    assert (bodies[0]["startTime"], bodies[0]["endTime"]) == (END_MS + 1 - 300_000, END_MS)
    assert bodies[1]["startTime"] == int(start.timestamp() * 1000)
    assert bodies[1]["endTime"] == END_MS + 1 - 300_000 - 1


def test_search_desc_default_start_and_window_cursor(aws: AwsMock) -> None:
    created = END_MS - 2 * 3_600_000
    aws.reply("DescribeLogGroups", {"logGroups": [{"logGroupName": G, "creationTime": created}]})
    for _ in range(4):
        aws.reply("FilterLogEvents", {"events": []})
    with sync_client(aws) as c:
        q = c.log_query_client()
        page = q.search("x", end=END, limit=1)
        assert page.records == []
        assert page.next_cursor is None
    bodies = aws.bodies("FilterLogEvents")
    assert bodies[-1]["startTime"] == created - 3_600_000
    # the creation time is cached
    aws.reply("FilterLogEvents", {"events": [ev(1, END_MS - 1000)]})
    with sync_client(aws) as c:
        q = c.log_query_client()
        aws.reply("DescribeLogGroups", {"logGroups": []})
        page = q.search("x", end=END, limit=1)
        assert len(page.records) == 1 and page.next_cursor is not None


def test_search_desc_narrows_large_slices(aws: AwsMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_ops, "SEARCH_SCAN_CAP", 2)
    many = {"events": [ev(i, END_MS - 1000 * i) for i in range(1, 5)]}
    aws.reply("FilterLogEvents", many)
    aws.reply("FilterLogEvents", {"events": [ev(1, END_MS - 1000)]})
    with sync_client(aws) as c:
        page = c.log_query_client().search("x", start=END - timedelta(minutes=10), end=END, limit=1)
    assert [r.body for r in page.records] == ["m1"]
    bodies = aws.bodies("FilterLogEvents")
    assert bodies[1]["startTime"] > bodies[0]["startTime"]


def test_search_scan_follows_tokens(aws: AwsMock) -> None:
    aws.reply("FilterLogEvents", {"events": [ev(1, END_MS - 1000)], "nextToken": "n"})
    aws.reply("FilterLogEvents", {"events": [ev(2, END_MS - 500)]})
    with sync_client(aws) as c:
        page = c.log_query_client().search("x", start=END - timedelta(minutes=1), end=END, limit=5)
    assert [r.body for r in page.records] == ["m2", "m1"]


def test_unknown_group_uses_lookback(aws: AwsMock) -> None:
    aws.reply("DescribeLogGroups", {"logGroups": []})
    aws.reply("FilterLogEvents", {"events": [ev(1, END_MS - 1000), ev(2, END_MS - 900)]})
    with sync_client(aws) as c:
        page = c.log_query_client().search("x", end=END, limit=1)
    assert page.next_cursor is not None
    assert json.loads(base64.urlsafe_b64decode(page.next_cursor + "==="))["k"] == 1


def test_tail_streams_and_wait_for_log(aws: AwsMock) -> None:
    result = {"logStreamName": "s1", "message": "tail one", "timestamp": END_MS}
    frames = event("initial-response", None) + session_start() + session_update(result)
    aws.reply(
        "DescribeLogGroups",
        {
            "logGroups": [
                {"logGroupName": G, "logGroupArn": "arn:aws:logs:us-east-1:1:log-group:" + G}
            ]
        },
    )
    aws.reply("StartLiveTail", frames, stream=True)
    aws.fixture("describeLogStreams.prefix")
    aws.reply("DescribeLogGroups", {"logGroups": []})
    aws.reply("FilterLogEvents", {"events": [ev(1, END_MS, "found it")]})
    for _ in range(12):  # the remaining slices of the 30-day window
        aws.reply("FilterLogEvents", {"events": []})
    got: list[sdk.LogRecord] = []
    with sync_client(aws) as c:
        q = c.log_query_client()
        handle = q.tail("tail", got.append)
        assert handle.wait(5)
        handle.stop()
        assert [r.body for r in got] == ["tail one"]
        assert q.list_streams()
        rec = q.wait_for_log("found", timeout_s=2, interval_s=0.01)
        assert rec.body == "found it"
    assert aws.bodies("StartLiveTail")[0]["logEventFilterPattern"] == '"tail"'


async def test_async_query_client(aws: AwsMock) -> None:
    result = {"logStreamName": "s1", "message": "tail one", "timestamp": END_MS}
    frames = event("initial-response", None) + session_start() + session_update(result)
    aws.reply("FilterLogEvents", {"events": [ev(1, 1)]})
    aws.reply("StartLiveTail", frames, stream=True)
    aws.fixture("describeLogStreams.prefix")
    got: list[sdk.LogRecord] = []
    async with async_client(aws, account_id="123456789012") as c:
        q = c.log_query_client()
        page = await q.search("x", order="asc")
        assert [r.body for r in page.records] == ["m1"]
        handle = await q.tail("tail", got.append)
        assert await handle.wait(5)
        await handle.stop()
        assert got and got[0].body == "tail one"
        assert await q.list_streams()
        with pytest.raises(sdk.UnsupportedError):
            await q.list_labels()
        with pytest.raises(sdk.ConfigError):
            async_client(aws, log_group=None).log_query_client()
