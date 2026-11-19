"""Live Tail (StartLiveTail event streams) for the sync and async clients."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

import grow_aws_cloudwatch_logs as sdk
from _support import (
    INTERNALS,
    AwsMock,
    aws_config,
    event,
    exception_frame,
    session_start,
    session_update,
)

G = "grow-bootstrap/seed"
ARN = "arn:aws:logs:us-east-1:123456789012:log-group:" + G


INIT = event("initial-response", None)
PAD = session_update() * 2  # keep-alive updates


def result(message: str, stream: str = "seed-stream") -> dict[str, Any]:
    return {
        "logStreamName": stream,
        "logGroupIdentifier": ARN,
        "message": message,
        "timestamp": 1795034969000,
        "ingestionTime": 1795034972331,
    }


def sync_client(aws: AwsMock, **kw: Any) -> sdk.CloudWatchLogsClient:
    kw.setdefault("account_id", "123456789012")
    return sdk.CloudWatchLogsClient(aws_config(aws.url, **kw), internals=INTERNALS)


def async_client(aws: AwsMock, **kw: Any) -> sdk.AsyncCloudWatchLogsClient:
    kw.setdefault("account_id", "123456789012")
    return sdk.AsyncCloudWatchLogsClient(aws_config(aws.url, **kw), internals=INTERNALS)


def test_seed_fixture_replay(aws: AwsMock) -> None:
    aws.fixture("startLiveTail.seed")
    events: list[sdk.LiveTailEvent] = []
    updates: list[sdk.LiveTailUpdate] = []
    with sync_client(aws) as c:
        session = c.start_live_tail([G], events.append, on_update=updates.append, pattern="grow")
        assert session.wait(5)
        assert session.session_id == "dab2e97c-0f97-49b7-bc9d-d7a93d6341e3"
        assert session.closed and session.error is None
    assert events[0].message.startswith('{"level":"info"')
    assert events[0].log_stream_name == "seed-stream" and events[0].ingestion_time
    assert updates == [sdk.LiveTailUpdate(sampled=False, count=3)]
    body = aws.bodies("StartLiveTail")[0]
    assert body == {"logGroupIdentifiers": [ARN], "logEventFilterPattern": "grow"}
    assert "LiveTailSession(" in repr(session)


def test_streaming_session_close_and_errors(aws: AwsMock) -> None:
    hold = threading.Event()
    frames = event("initial-response", None) + session_update(result("early")) + session_start()
    frames += session_update(result("one"), result("two"), sampled=True) + PAD
    aws.reply("StartLiveTail", frames, stream=True, hold=hold)
    events: list[str] = []

    def boom(_: Any) -> None:
        raise RuntimeError("callbacks never break the session")

    with sync_client(aws) as c:
        session = c.start_live_tail(
            [G], lambda e: events.append(e.message), streams=["s1"], on_update=boom
        )
        deadline = time.monotonic() + 5
        while len(events) < 3 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert events == ["early", "one", "two"]
        assert not session.closed
        session.close()
        session.close()
        assert session.closed and session.error is None
    hold.set()
    assert aws.bodies("StartLiveTail")[0]["logStreamNames"] == ["s1"]


def test_stream_exception_frame_goes_to_on_error(aws: AwsMock) -> None:
    aws.reply(
        "StartLiveTail",
        INIT + session_start() + exception_frame("SessionStreamingException"),
        stream=True,
    )
    errors: list[sdk.CloudWatchLogsError] = []
    with sync_client(aws) as c:
        session = c.start_live_tail([G], lambda e: None, on_error=errors.append)
        assert session.wait(5)
    assert isinstance(session.error, sdk.UnavailableError) and session.error.retryable
    assert errors == [session.error]


def test_exception_before_start_raises(aws: AwsMock) -> None:
    aws.reply("StartLiveTail", INIT + exception_frame("SessionTimeoutException"), stream=True)
    with sync_client(aws) as c, pytest.raises(sdk.TimeoutError):
        c.start_live_tail([G], lambda e: None)
    aws.reply("StartLiveTail", event("initial-response", None), stream=True)
    with sync_client(aws) as c:
        session = c.start_live_tail([G], lambda e: None)
        assert session.closed and session.session_id == ""
        session.close()


def test_validation_and_service_errors(aws: AwsMock) -> None:
    aws.error("StartLiveTail", "ResourceNotFoundException")
    with sync_client(aws) as c:
        for kwargs in (
            {"groups": []},
            {"groups": [G] * 11},
            {"groups": G},
            {"groups": [G], "streams": ["a"], "stream_prefixes": ["b"]},
            {"groups": [G, G], "streams": ["a"]},
            {"groups": ["bad name!"]},
        ):
            with pytest.raises(sdk.ValidationError):
                c.start_live_tail(on_event=lambda e: None, **kwargs)  # type: ignore[arg-type]
        with pytest.raises(sdk.NotFoundError):
            c.start_live_tail([ARN + ":*"], lambda e: None, stream_prefixes=["p"])
    assert aws.bodies("StartLiveTail")[0] == {
        "logGroupIdentifiers": [ARN],
        "logStreamNamePrefixes": ["p"],
    }


def test_group_arn_lookup_without_account(aws: AwsMock) -> None:
    aws.reply(
        "DescribeLogGroups",
        {"logGroups": [{"logGroupName": G, "logGroupArn": ARN, "arn": ARN + ":*"}]},
    )
    aws.fixture("startLiveTail.seed")
    with sdk.CloudWatchLogsClient(aws_config(aws.url), internals=INTERNALS) as c:
        session = c.start_live_tail([G], lambda e: None)
        session.wait(5)
    assert aws.bodies("StartLiveTail")[0]["logGroupIdentifiers"] == [ARN]


def test_client_close_stops_sessions(aws: AwsMock) -> None:
    hold = threading.Event()
    aws.reply("StartLiveTail", INIT + session_start() + PAD, stream=True, hold=hold)
    c = sync_client(aws)
    session = c.start_live_tail([G], lambda e: None)
    c.close()
    assert session.closed
    hold.set()


def test_no_stream_in_response(aws: AwsMock, monkeypatch: pytest.MonkeyPatch) -> None:
    with sync_client(aws) as c:
        monkeypatch.setattr(c, "_invoke", lambda call, cancel: {})
        with pytest.raises(sdk.UnavailableError):
            c.start_live_tail([ARN], lambda e: None)


# --------------------------------------------------------------------------- async


async def test_async_seed_and_streaming(aws: AwsMock) -> None:
    aws.fixture("startLiveTail.seed")
    events: list[sdk.LiveTailEvent] = []
    async with async_client(aws) as c:
        session = await c.start_live_tail([G], events.append)
        assert await session.wait(5)
        assert session.session_id and session.error is None
    assert len(events) == 3
    hold = threading.Event()
    aws.reply(
        "StartLiveTail",
        INIT
        + session_update(result("early"))
        + session_start()
        + session_update(result("one"))
        + PAD,
        stream=True,
        hold=hold,
    )
    got: list[str] = []
    async with async_client(aws) as c:
        session = await c.start_live_tail([G], lambda e: got.append(e.message))
        for _ in range(250):
            if len(got) >= 2:
                break
            await asyncio.sleep(0.02)
        assert got == ["early", "one"]
        assert not await session.wait(0.05)
        await session.close()
        await session.close()
        assert session.closed
        assert "AsyncLiveTailSession(" in repr(session)
    hold.set()


async def test_async_errors(aws: AwsMock) -> None:
    aws.reply(
        "StartLiveTail",
        INIT + session_start() + exception_frame("SessionStreamingException"),
        stream=True,
    )
    aws.reply("StartLiveTail", INIT + exception_frame("SessionTimeoutException"), stream=True)
    aws.reply("StartLiveTail", event("initial-response", None), stream=True)
    errors: list[Any] = []
    async with async_client(aws) as c:
        session = await c.start_live_tail([G], lambda e: None, on_error=errors.append)
        assert await session.wait(5)
        assert isinstance(session.error, sdk.UnavailableError) and errors
        with pytest.raises(sdk.TimeoutError):
            await c.start_live_tail([G], lambda e: None)
        ended = await c.start_live_tail([G], lambda e: None)
        assert ended.closed
        await ended.close()
        with pytest.raises(sdk.ValidationError):
            await c.start_live_tail([], lambda e: None)


async def test_async_close_stops_sessions(aws: AwsMock, monkeypatch: pytest.MonkeyPatch) -> None:
    hold = threading.Event()
    aws.reply("StartLiveTail", INIT + session_start() + PAD, stream=True, hold=hold)
    c = async_client(aws)
    session = await c.start_live_tail([G], lambda e: None)
    await c.aclose()
    assert session.closed
    hold.set()
    c2 = async_client(aws)

    async def no_stream(call: Any) -> dict[str, Any]:
        return {}

    monkeypatch.setattr(c2, "_invoke", no_stream)
    with pytest.raises(sdk.UnavailableError):
        await c2.start_live_tail([ARN], lambda e: None)
    await c2.aclose()
