"""Every public function against the real service (Moto in Docker), with round-trip checks.

Moto quirks respected here (API.md §6): every ``logGroupIdentifier`` resolves to the most
recently created group and live tail replays existing events once, so each live-tail test
creates its group last and puts the events before tailing; Insights runs only fields / sort /
limit; ``StopQuery`` is not implemented (HTTP 500 → ``UnavailableError``).
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from factories import async_core_client, core_client

import grow_aws_cloudwatch_logs as sdk


@pytest.fixture
def groups(client: sdk.CloudWatchLogsClient, prefix: str) -> Iterator[list[str]]:
    created: list[str] = []
    yield created
    for name in created:
        client.delete_log_group(name, ignore_missing=True)


def test_bootstrap_functions(
    client: sdk.CloudWatchLogsClient, prefix: str, groups: list[str]
) -> None:
    group, other, kms = f"{prefix}/app", f"{prefix}/other", f"{prefix}/kms"
    groups.extend([group, other, kms])
    res = client.ensure_log_group(group, retention_days=7, tags={"team": "sdk"})
    assert res.created and res.arn.endswith(":log-group:" + group)
    again = client.ensure_log_group(group, retention_days=14, tags={"env": "it"})
    assert again == sdk.EnsureLogGroupResult(created=False, arn=res.arn)
    assert client.list_log_group_tags(group) == {"team": "sdk", "env": "it"}
    client.tag_log_group(group, {"x": "1"})
    client.untag_log_group(res.arn, ["team"])
    assert client.list_log_group_tags(group) == {"env": "it", "x": "1"}

    listed = list(client.describe_log_groups(prefix=prefix + "/", page_size=1))
    assert [g.name for g in listed] == [group] and listed[0].retention_days == 14
    info = client.get_log_group(group)
    assert info is not None and info.arn == res.arn and info.creation_time is not None
    assert client.get_log_group(prefix + "/missing") is None
    assert client.log_group_arn(group) == res.arn

    client.put_retention_policy(group, 30)
    assert next(client.describe_log_groups(prefix=group)).retention_days == 30
    client.put_retention_policy(group, None)
    assert next(client.describe_log_groups(prefix=group)).retention_days is None

    for stream in ("s1", "s2", "s3"):
        assert client.ensure_log_stream(group, stream).created
    assert client.ensure_log_stream(group, "s1").created is False
    streams = list(client.describe_log_streams(group, page_size=2))
    assert [s.name for s in streams] == ["s1", "s2", "s3"] and streams[0].arn
    assert len(list(client.describe_log_streams(group, limit=2))) == 2
    assert next(client.describe_log_streams(group, descending=True)).name == "s3"
    assert [s.name for s in client.describe_log_streams(group, prefix="s2")] == ["s2"]
    assert client.delete_log_stream(group, "s3") is True
    assert client.delete_log_stream(group, "s3", ignore_missing=True) is False
    with pytest.raises(sdk.NotFoundError):
        client.delete_log_stream(group, "s3")

    assert client.ensure_log_stream(other, "x", create_group=True).created
    assert client.delete_log_group(other) is True
    assert list(client.describe_log_groups(prefix=other)) == []
    assert client.delete_log_group(other, ignore_missing=True) is False
    with pytest.raises(sdk.NotFoundError):
        client.delete_log_group(other)

    client.ensure_log_group(kms, kms_key_id="arn:aws:kms:us-east-1:123456789012:key/k1")
    with pytest.raises(sdk.ConflictError):
        client.ensure_log_group(kms, kms_key_id="k2")
    with pytest.raises(sdk.ValidationError):
        client.ensure_log_group("bad name!")
    with pytest.raises(sdk.ValidationError):
        client.put_retention_policy(group, 2)


def test_ingest_and_query_functions(
    client: sdk.CloudWatchLogsClient, prefix: str, groups: list[str]
) -> None:
    group = f"{prefix}/app"
    groups.append(group)
    client.ensure_log_group(group)
    marker = "marker-" + prefix
    base = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(microsecond=0)

    # ingest: unsorted input, unicode, auto-created stream
    res = client.put_log_events(
        group,
        "s1",
        [
            sdk.InputLogEvent(f"third {marker} ERROR", base + timedelta(seconds=2)),
            sdk.InputLogEvent(f"first {marker}", base),
            {"message": f"second {marker} ünïcode ✓", "timestamp": base + timedelta(seconds=1)},
        ],
    )
    assert (res.accepted, res.rejected, res.batches) == (3, 0, 1)
    assert (
        client.put_log_events(group, "auto", [sdk.InputLogEvent(f"auto {marker}", base)]).accepted
        == 1
    )
    # oversize truncation round trip
    big = "x" * 300_000 + marker
    res = client.put_log_events(group, "big", [sdk.InputLogEvent(big, base)])
    assert (res.truncated, res.accepted) == (1, 1)
    stored = list(client.get_log_events(group, "big", start_from_head=True))
    assert len(stored) == 1 and "truncated" in stored[0].message
    assert len(stored[0].message.encode()) <= sdk.DEFAULT_MAX_EVENT_BYTES
    # > 10,000 events are split client side
    bulk = [sdk.InputLogEvent("bulk", base) for _ in range(10_001)]
    res = client.put_log_events(group, "bulk", bulk)
    assert (res.batches, res.accepted) == (2, 10_001)
    # EMF
    doc = sdk.build_emf(
        "latency", 42, {"service": "api"}, namespace="GrowSdkIT", unit="Milliseconds"
    )
    assert doc["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "GrowSdkIT"
    sent = client.emf(
        "latency", 42, {"service": "api"}, group=group, stream="emf", properties={"marker": marker}
    )
    emf = list(client.get_log_events(group, "emf", start_from_head=True))
    assert len(emf) == 1 and json.loads(emf[0].message) == sent

    # get_log_events: forward one per page, newest page, limit
    head = list(client.get_log_events(group, "s1", start_from_head=True, page_size=1))
    assert [e.message.split()[0] for e in head] == ["first", "second", "third"]
    assert head[0].timestamp == base and "ünïcode ✓" in head[1].message
    assert len(list(client.get_log_events(group, "s1", limit=1))) == 1
    assert (
        len(list(client.get_log_events(group, "s1", start=base, end=base + timedelta(milliseconds=500))))
        == 1
    )

    # filter_log_events: term, phrase over pages, limit, page API
    filtered = list(client.filter_log_events(group=group, streams=["s1"], pattern="ERROR"))
    assert len(filtered) == 1 and filtered[0].message.startswith("third")
    assert filtered[0].log_stream_name == "s1" and filtered[0].event_id
    paged = list(
        client.filter_log_events(
            group=group,
            pattern=f'"{marker}"',
            page_size=1,
            start=base - timedelta(seconds=1),
            end=datetime.now(timezone.utc) + timedelta(minutes=1),
        )
    )
    assert len(paged) >= 5
    assert len(list(client.filter_log_events(group=group, pattern=marker, limit=2))) == 2
    page = client.filter_log_events_page(group=group, streams=["s1"], page_size=2)
    assert len(page.events) == 2 and page.next_token
    page = client.filter_log_events_page(
        group=group, streams=["s1"], page_size=2, next_token=page.next_token
    )
    assert [e.message.split()[0] for e in page.events] == ["third"]

    # Logs Insights
    window = {
        "groups": [group],
        "start": base - timedelta(minutes=1),
        "end": datetime.now(timezone.utc) + timedelta(minutes=1),
    }
    qid = client.start_query(
        "fields @timestamp, @message | sort @timestamp asc | limit 3", **window
    )
    assert qid
    first = client.get_query_results(qid)
    assert first.status in sdk.QUERY_STATUSES
    res2 = client.query("fields @timestamp, @message | sort @timestamp desc | limit 2", **window)
    assert res2.status == "Complete" and len(res2.rows) == 2 and res2.rows[0]["@message"]
    assert sdk.parse_insights_timestamp(res2.rows[0]["@timestamp"]) is not None
    assert any(q.query_id == qid for q in client.describe_queries(group=group))
    if os.environ.get("GROW_TEST_EMULATOR", "moto") == "moto":
        with pytest.raises(sdk.UnavailableError):
            client.stop_query(qid)  # not implemented by Moto 5.2.4
    else:  # pragma: no cover - real AWS
        client.stop_query(qid)


def test_logging_category(env: dict[str, str], prefix: str, groups: list[str]) -> None:
    group = f"{prefix}/logs"
    groups.append(group)
    marker = "marker-" + prefix
    with core_client(env, log_group=group, log_stream="logclient") as c:
        c.ensure_log_group(group)
        logs = c.log_client(resource={"service.name": "it-python"})
        logs.logger("it", {"run": prefix}).info(
            f"logger hello {marker}", {"password": "hunter2-" + prefix}
        )
        logs.send(
            sdk.LogInput(
                f"send hello {marker}",
                severity=sdk.Severity.WARN,
                trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
                span_id="00f067aa0ba902b7",
            )
        )
        logs.flush(30)
        stats = logs.stats()
        assert (stats.sent, stats.failed, stats.dropped) == (2, 0, 0)
        q = c.log_query_client()
        assert q.group == group
        rec = q.wait_for_log(f"send hello {marker}", timeout_s=20, interval_s=0.5)
        assert (
            rec.severity == sdk.Severity.WARN and rec.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        )
        assert rec.resource["service.name"] == "it-python"
        assert rec.attributes["aws.log.stream"] == "logclient"
        logged = q.wait_for_log(f"logger hello {marker}", timeout_s=20, interval_s=0.5)
        assert "hunter2-" + prefix not in logged.to_json_str()
        # exporter directly
        exported = c.exporter(stream="exporter").export(
            [sdk.LogRecord(body=f"exported {marker}", severity=sdk.Severity.ERROR)],
            gzip=True,
            timeout_s=10,
        )
        assert (exported.accepted, exported.rejected) == (1, 0)
        # search: desc newest first with cursors, asc oldest first
        desc = q.search(marker, limit=2)
        assert len(desc.records) == 2 and desc.next_cursor
        assert desc.records[0].timestamp >= desc.records[1].timestamp
        all_desc = collect(q, marker, "desc")
        all_asc = collect(q, marker, "asc")
        assert len(all_desc) == len(all_asc) == 3
        assert sorted(r.body for r in all_desc) == sorted(r.body for r in all_asc)
        assert all(a.timestamp >= b.timestamp for a, b in zip(all_desc, all_desc[1:]))
        streams = q.list_streams()
        assert {"logclient", "exporter"} <= set(streams)
        with pytest.raises(sdk.UnsupportedError):
            q.list_labels()


def collect(q: sdk.CloudWatchLogsQueryClient, query: str, order: str) -> list[sdk.LogRecord]:
    out: list[sdk.LogRecord] = []
    cursor = None
    while True:
        page = q.search(query, limit=1, order=order, cursor=cursor)
        out.extend(page.records)
        cursor = page.next_cursor
        if not cursor:
            return out


def test_permission_denied_with_read_only_user(env: dict[str, str], prefix: str) -> None:
    key, secret = (
        env.get("GROW_TEST_READONLY_ACCESS_KEY_ID"),
        env.get("GROW_TEST_READONLY_SECRET_ACCESS_KEY"),
    )
    if not key or not secret:
        pytest.skip("no read-only user in /bootstrap/env")
    with core_client(env, access_key_id=key, secret_access_key=secret) as ro:
        assert ro.ping() > 0
        with pytest.raises(sdk.PermissionDeniedError):
            ro.ensure_log_group(prefix + "/ro")
        with pytest.raises(sdk.PermissionDeniedError):
            ro.put_log_events(env["CLOUDWATCH_LOG_GROUP"], "s1", [sdk.InputLogEvent("x")])
        seed = list(
            ro.filter_log_events(
                group=env["CLOUDWATCH_LOG_GROUP"],
                pattern=env.get("GROW_TEST_SEED_MARKER", "grow-seed-event"),
            )
        )
        assert len(seed) >= 1


def test_live_tail(client: sdk.CloudWatchLogsClient, prefix: str, groups: list[str]) -> None:
    tail = f"{prefix}/tail"
    groups.append(tail)
    client.ensure_log_group(tail)  # created last: Moto resolves identifiers to the newest group
    marker = "marker-" + prefix
    client.put_log_events(
        tail,
        "t1",
        [sdk.InputLogEvent(f"tail one {marker}"), sdk.InputLogEvent(f"tail two {marker}")],
    )
    events: list[sdk.LiveTailEvent] = []
    updates: list[sdk.LiveTailUpdate] = []
    lock = threading.Lock()

    def on_event(e: sdk.LiveTailEvent) -> None:
        with lock:
            events.append(e)

    session = client.start_live_tail([tail], on_event, pattern="tail", on_update=updates.append)
    assert session.session_id
    wait_until(lambda: len(events) >= 2 or session.closed)
    session.close()
    assert session.error is None
    assert [e.message.split(" ")[1] for e in events[:2]] == ["one", "two"]
    assert events[0].log_stream_name == "t1" and updates
    # LogQueryClient.tail on the same group
    records: list[sdk.LogRecord] = []
    handle = client.log_query_client(group=tail).tail("tail two", records.append)
    wait_until(lambda: len(records) >= 1 or handle.closed)
    handle.stop()
    assert len(records) == 1 and "tail two" in records[0].body


def wait_until(condition: Any, timeout: float = 15.0) -> None:
    import time

    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.05)
    time.sleep(0.1)


def test_async_client_functions(env: dict[str, str], prefix: str, groups: list[str]) -> None:
    group = f"{prefix}/async"
    groups.append(group)
    marker = "marker-" + prefix

    async def run() -> None:
        async with async_core_client(env, log_group=group, log_stream="alogs") as c:
            res = await c.ensure_log_group(group, retention_days=7, tags={"a": "1"})
            assert res.created and await c.log_group_arn(group) == res.arn
            assert (await c.get_log_group(group)) is not None
            assert await c.list_log_group_tags(group) == {"a": "1"}
            await c.tag_log_group(group, {"b": "2"})
            await c.untag_log_group(group, ["a"])
            assert await c.list_log_group_tags(group) == {"b": "2"}
            await c.put_retention_policy(group, 14)
            assert [g.retention_days async for g in c.describe_log_groups(prefix=group)] == [14]
            assert (await c.ensure_log_stream(group, "s1")).created
            put = await c.put_log_events(
                group, "s1", [sdk.InputLogEvent(f"async {marker} {i}") for i in range(3)]
            )
            assert put.accepted == 3
            await c.emf("count", 1, group=group, stream="emf")
            assert [s.name async for s in c.describe_log_streams(group)] == ["emf", "s1"]
            got = [e.message async for e in c.get_log_events(group, "s1", start_from_head=True)]
            assert got == [f"async {marker} {i}" for i in range(3)]
            found = [e async for e in c.filter_log_events(group=group, pattern=f'"{marker}"')]
            assert len(found) == 3
            page = await c.filter_log_events_page(group=group, page_size=1)
            assert len(page.events) == 1
            qid = await c.start_query("fields @message | limit 1", groups=[group])
            assert (await c.get_query_results(qid)).status in sdk.QUERY_STATUSES
            assert (await c.query("fields @message | limit 2", groups=[group])).status == "Complete"
            assert await c.describe_queries(group=group)
            with pytest.raises(sdk.UnavailableError):
                await c.stop_query(qid)
            logs = c.log_client()
            logs.send(sdk.LogInput(f"async log {marker}"))
            await logs.flush(30)
            q = c.log_query_client()
            rec = await q.wait_for_log(f"async log {marker}", timeout_s=20, interval_s=0.5)
            assert rec.body == f"async log {marker}"
            assert len((await q.search(marker, limit=10, order="asc")).records) == 4
            assert "alogs" in await q.list_streams()
            # live tail: the async group is the newest one
            events: list[sdk.LiveTailEvent] = []
            session = await c.start_live_tail([group], events.append, streams=["s1"])
            for _ in range(300):
                if len(events) >= 3 or session.closed:
                    break
                await asyncio.sleep(0.05)
            await session.close()
            assert len(events) == 3
            assert await c.delete_log_stream(group, "emf")
            assert await c.delete_log_group(group)
            assert not await c.delete_log_group(group, ignore_missing=True)

    asyncio.run(run())
