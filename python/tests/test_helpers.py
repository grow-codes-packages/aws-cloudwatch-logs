"""Pure helpers: AWS error mapping, batching, truncation, EMF, Insights timestamps, patterns."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from botocore import exceptions as bce

import grow_aws_cloudwatch_logs as sdk


def client_error(
    code: str, status: int = 400, message: str = "m", request_id: str = "r"
) -> bce.ClientError:
    return bce.ClientError(
        {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status, "RequestId": request_id},
        },
        "Op",
    )


# --------------------------------------------------------------------------- errors


@pytest.mark.parametrize(
    ("code", "cls", "retryable"),
    [
        ("ResourceNotFoundException", sdk.NotFoundError, False),
        ("ResourceAlreadyExistsException", sdk.ConflictError, False),
        ("OperationAbortedException", sdk.ConflictError, True),
        ("InvalidParameterException", sdk.ValidationError, False),
        ("LimitExceededException", sdk.ValidationError, False),
        ("UnrecognizedClientException", sdk.AuthenticationError, False),
        ("AccessDeniedException", sdk.PermissionDeniedError, False),
        ("ThrottlingException", sdk.RateLimitError, True),
        ("ServiceUnavailableException", sdk.UnavailableError, True),
        ("SessionStreamingException", sdk.UnavailableError, True),
        ("SessionTimeoutException", sdk.TimeoutError, True),
        ("com.amazonaws#ResourceNotFoundException", sdk.NotFoundError, False),
    ],
)
def test_map_by_aws_code(code: str, cls: type[sdk.CloudWatchLogsError], retryable: bool) -> None:
    err = sdk.map_aws_error("op", client_error(code))
    assert type(err) is cls and err.retryable is retryable
    assert err.http_status == 400 and err.request_id == "r"
    assert err.message.startswith("op: ") and err.message.endswith("(HTTP 400)")
    assert sdk.aws_error_code(err) == code.rsplit("#", 1)[-1]


@pytest.mark.parametrize(
    ("status", "message", "cls"),
    [
        (403, "<Code>InvalidClientTokenId</Code>", sdk.AuthenticationError),
        (403, "The security token included in the request is invalid", sdk.AuthenticationError),
        (403, "<Code>AccessDenied</Code>", sdk.PermissionDeniedError),
        (401, "", sdk.AuthenticationError),
        (404, "", sdk.NotFoundError),
        (408, "", sdk.TimeoutError),
        (504, "", sdk.TimeoutError),
        (429, "", sdk.RateLimitError),
        (500, "", sdk.UnavailableError),
        (418, "", sdk.ValidationError),
    ],
)
def test_map_by_status(status: int, message: str, cls: type[sdk.CloudWatchLogsError]) -> None:
    err = sdk.map_aws_error("op", client_error(str(status), status, message))
    assert type(err) is cls
    assert message == "" or message not in err.message


def test_map_other_shapes() -> None:
    no_status = bce.ClientError({"Error": {"Code": "Weird"}}, "Op")
    assert isinstance(sdk.map_aws_error("op", no_status), sdk.UnavailableError)
    assert isinstance(sdk.map_aws_error("op", bce.NoCredentialsError()), sdk.AuthenticationError)
    cfg = sdk.map_aws_error("op", bce.NoRegionError())
    assert isinstance(cfg, sdk.ConfigError) and "region" in cfg.message
    assert isinstance(sdk.map_aws_error("op", bce.ProfileNotFound(profile="p")), sdk.ConfigError)
    assert isinstance(
        sdk.map_aws_error("op", bce.ParamValidationError(report="x")), sdk.ValidationError
    )
    assert isinstance(
        sdk.map_aws_error("op", bce.ReadTimeoutError(endpoint_url="http://x")), sdk.TimeoutError
    )
    assert isinstance(sdk.map_aws_error("op", asyncio.TimeoutError()), sdk.TimeoutError)
    assert isinstance(
        sdk.map_aws_error("op", bce.EndpointConnectionError(endpoint_url="http://x")),
        sdk.UnavailableError,
    )
    assert isinstance(sdk.map_aws_error("op", asyncio.CancelledError()), sdk.CancelledError)
    assert isinstance(sdk.map_aws_error("op", RuntimeError("x")), sdk.UnavailableError)
    original = sdk.NotFoundError("x")
    assert sdk.map_aws_error("op", original) is original
    assert sdk.aws_error_code(None) is None and sdk.aws_error_code(RuntimeError()) is None


def test_cause_is_redacted() -> None:
    err = sdk.map_aws_error(
        "op", RuntimeError("leak super-secret-value here"), ["super-secret-value"]
    )
    assert isinstance(err.cause, sdk.AwsSdkError)
    assert "super-secret-value" not in str(err.cause) and err.cause.type_name == "RuntimeError"
    assert str(sdk.AwsSdkError("T", "")) == "T"


# --------------------------------------------------------------------------- batching


def test_truncate_message_utf8_boundary_and_marker() -> None:
    assert sdk.truncate_message("short", 10) == "short"
    text = "ü" * 100  # 200 bytes
    out = sdk.truncate_message(text, 60)
    assert sdk.utf8_bytes(out) <= 60 and out.startswith("ü") and out.endswith("bytes]")
    removed = int(out.split("[truncated ")[1].split(" ")[0])
    assert sdk.utf8_bytes(out.split(" …")[0]) + removed == 200
    tiny = sdk.truncate_message("x" * 100, 5)
    assert sdk.utf8_bytes(tiny) <= 5


def test_plan_batches_limits() -> None:
    base = 1_700_000_000_000
    events = [{"message": "m", "timestamp": base + i} for i in range(10_001)]
    plan = sdk.plan_log_event_batches(events)
    assert [len(b) for b in plan.batches] == [10_000, 1]
    big = [{"message": "x" * 200_000, "timestamp": base} for _ in range(6)]
    plan = sdk.plan_log_event_batches(big)
    assert [len(b) for b in plan.batches] == [5, 1]
    for batch in plan.batches:
        assert (
            sum(len(e["message"]) + sdk.EVENT_OVERHEAD_BYTES for e in batch) <= sdk.MAX_BATCH_BYTES
        )
    day = sdk.MAX_BATCH_SPAN_MS
    spans = [
        sdk.InputLogEvent("a", base),
        sdk.InputLogEvent("b", base + day),
        sdk.InputLogEvent("c", base + day + 1),
    ]
    assert [len(b) for b in sdk.plan_log_event_batches(spans).batches] == [2, 1]
    unsorted = [
        sdk.InputLogEvent("2", base + 2),
        sdk.InputLogEvent("1", base + 1),
        sdk.InputLogEvent("1b", base + 1),
    ]
    assert [e["message"] for e in sdk.plan_log_event_batches(unsorted).batches[0]] == [
        "1",
        "1b",
        "2",
    ]
    when = datetime(2026, 1, 1)
    plan = sdk.plan_log_event_batches([sdk.InputLogEvent("x", when)])
    assert plan.batches[0][0]["timestamp"] == int(
        when.replace(tzinfo=timezone.utc).timestamp() * 1000
    )


def test_plan_batches_policies_and_validation() -> None:
    events = [sdk.InputLogEvent("x" * 300, 1)]
    assert sdk.plan_log_event_batches(events, max_event_bytes=100).truncated == 1
    plan = sdk.plan_log_event_batches(events, max_event_bytes=100, oversize_policy="drop")
    assert plan.dropped == 1 and plan.batches == []
    with pytest.raises(sdk.ValidationError):
        sdk.plan_log_event_batches(events, max_event_bytes=100, oversize_policy="error")
    for bad in (0, True, sdk.MAX_EVENT_BYTES_HARD_CAP + 1):
        with pytest.raises(sdk.ValidationError):
            sdk.plan_log_event_batches([], max_event_bytes=bad)
    with pytest.raises(sdk.ValidationError):
        sdk.plan_log_event_batches([], oversize_policy="shrink")
    for bad_event in ({"message": 1}, "plain", {"message": "x", "timestamp": "now"}):
        with pytest.raises(sdk.ValidationError):
            sdk.plan_log_event_batches([bad_event])  # type: ignore[list-item]


# --------------------------------------------------------------------------- EMF


def test_build_emf() -> None:
    when = datetime(2026, 11, 18, tzinfo=timezone.utc)
    doc = sdk.build_emf(
        "latency", 42, {"service": "api"}, unit="Milliseconds", timestamp=when, properties={"m": 1}
    )
    assert doc["_aws"] == {
        "Timestamp": int(when.timestamp() * 1000),
        "CloudWatchMetrics": [
            {
                "Namespace": "GrowSdk",
                "Dimensions": [["service"]],
                "Metrics": [{"Name": "latency", "Unit": "Milliseconds"}],
            }
        ],
    }
    assert (doc["latency"], doc["service"], doc["m"]) == (42, "api", 1)
    assert sdk.build_emf("v", [1, 2.5])["v"] == [1, 2.5]
    assert json.dumps(sdk.build_emf("v", 1))
    assert "None" in sdk.EMF_UNITS and sdk.DEFAULT_EMF_NAMESPACE == "GrowSdk"


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        (("", 1), {}),
        (("m", 1), {"namespace": ""}),
        (("m", 1), {"unit": "Parsecs"}),
        (("m", float("inf")), {}),
        (("m", True), {}),
        (("m", []), {}),
        (("m", [1] * 101), {}),
        (("m", "1"), {}),
        (("m", 1, {f"d{i}": "v" for i in range(31)}), {}),
        (("m", 1, {"": "v"}), {}),
        (("m", 1, {"m": "v"}), {}),
        (("m", 1, {"d": 1}), {}),
        (("m", 1), {"properties": {"_aws": 1}}),
        (("m", 1), {"timestamp": "now"}),
    ],
)
def test_build_emf_validation(args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    with pytest.raises(sdk.ValidationError):
        sdk.build_emf(*args, **kwargs)


# --------------------------------------------------------------------------- query helpers


def test_parse_insights_timestamp() -> None:
    aws = sdk.parse_insights_timestamp("2026-11-18 10:20:30.123")
    assert aws == datetime(2026, 11, 18, 10, 20, 30, 123000, tzinfo=timezone.utc)
    assert sdk.parse_insights_timestamp("2026-11-18T10:20:30Z") == datetime(
        2026, 11, 18, 10, 20, 30, tzinfo=timezone.utc
    )
    moto = sdk.parse_insights_timestamp("1795034915000")
    assert moto is not None and moto.year == 2026
    assert sdk.parse_insights_timestamp("yesterday") is None
    assert sdk.parse_insights_timestamp("2026-13-40 10:20:30") is None


def test_partition_and_filter_pattern() -> None:
    assert sdk.partition_for_region("cn-north-1") == "aws-cn"
    assert sdk.partition_for_region("us-gov-west-1") == "aws-us-gov"
    assert sdk.partition_for_region("eu-west-1") == "aws"
    assert sdk.to_filter_pattern("  ") == ""
    assert sdk.to_filter_pattern('say "hi" \\o/') == '"say \\"hi\\" \\\\o/"'
    for native in ("{ $.a = 1 }", "?x ?y", "[w1]", "-x", '"q"'):
        assert sdk.to_filter_pattern(native) == native


def test_event_to_log_record() -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rec = sdk.LogRecord(body="hello", severity=sdk.Severity.WARN, timestamp=ts, attributes={"a": 1})
    event = sdk.FilteredLogEvent(ts, rec.to_json_str(), ts + timedelta(seconds=1), "s1", "7")
    out = sdk.event_to_log_record(event)
    assert out.body == "hello" and out.severity == sdk.Severity.WARN
    assert out.attributes == {"a": 1, "aws.log.stream": "s1", "aws.event.id": "7"}
    plain = sdk.event_to_log_record(
        sdk.FilteredLogEvent(ts, '{"not": "a record"}', None, None, None)
    )
    assert plain.body == '{"not": "a record"}' and plain.severity == sdk.Severity.INFO
    tail = sdk.event_to_log_record(sdk.LiveTailEvent(ts, "[1]", ts, "s", "arn:g"))
    assert tail.attributes == {"aws.log.stream": "s", "aws.log.group": "arn:g"}
    assert tail.observed_timestamp == ts
    broken = sdk.event_to_log_record(sdk.FilteredLogEvent(ts, "{oops", None, None, None))
    assert broken.body == "{oops"


def test_record_to_message() -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rec = sdk.LogRecord(body="x" * 5000, timestamp=ts, attributes={"k": "v"})
    assert sdk.record_to_message(rec, 100_000, "truncate") == rec.to_json_str()
    out = sdk.record_to_message(rec, 1000, "truncate")
    assert out is not None and len(out.encode()) <= 1000
    assert json.loads(out)["attributes"] == {"k": "v"} and "truncated" in json.loads(out)["body"]
    assert sdk.record_to_message(rec, 1000, "drop") is None
    huge_attrs = sdk.LogRecord(body="b", timestamp=ts, attributes={"k": "v" * 3000})
    cut = sdk.record_to_message(huge_attrs, 500, "truncate")
    assert cut is not None and len(cut.encode()) <= 500


def test_resource_registry_stops_items_once() -> None:
    stopped: list[str] = []

    class Item:
        def __init__(self, name: str) -> None:
            self.name = name

        def stop(self) -> None:
            stopped.append(self.name)
            raise RuntimeError("ignored")

    registry = sdk.ResourceRegistry()
    a, b = Item("a"), Item("b")
    registry.register(a)
    registry.register(b)
    registry.unregister(b)
    registry.unregister(b)
    assert not registry.closed
    registry.stop_all()
    registry.register(Item("late"))
    assert registry.closed and stopped == ["a", "late"]
