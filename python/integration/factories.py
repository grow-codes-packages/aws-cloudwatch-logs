"""Factories the integration suite and the conformance runner use (owned by this repository).

Every logging scenario writes to its own log group (created here, deleted afterwards), so runs
never see each other. ``version`` is left out of the core capabilities: CloudWatch Logs exposes no
server version (``server_version`` raises ``UnsupportedError``, see docs/compatibility.md).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import grow_aws_cloudwatch_logs as sdk
from grow_aws_cloudwatch_logs.testing import LoggingTarget, ScenarioContext
from grow_aws_cloudwatch_logs.testing.logging_conformance import batch_options_for

URL_VAR = "GROW_AWS_CLOUDWATCH_LOGS_URL"

#: Capabilities of the deployment under test for the core scenarios ("version" = serverVersion).
CORE_CAPABILITIES: set[str] = set()

#: Capabilities for the logging scenarios: query, tail, streams, labels.
LOGGING_CAPABILITIES: set[str] = {"query"}


def core_client(env: Mapping[str, str], **overrides: Any) -> sdk.CloudWatchLogsClient:
    """A client configured from the test environment (/bootstrap/env)."""
    return sdk.CloudWatchLogsClient.from_env(env, **overrides)


def async_core_client(env: Mapping[str, str], **overrides: Any) -> sdk.AsyncCloudWatchLogsClient:
    return sdk.AsyncCloudWatchLogsClient.from_env(env, **overrides)


def logging_target(
    env: Mapping[str, str], ctx: ScenarioContext, cleanups: list[Callable[[], None]]
) -> LoggingTarget:
    """A ``LoggingTarget(client, query)`` on a fresh log group for one scenario."""
    group = f"grow-conformance-py/{ctx.run}"
    client = core_client(env, log_group=group, log_stream=f"conformance-{ctx.run}")
    client.ensure_log_group(group)

    def cleanup() -> None:
        client.delete_log_group(group, ignore_missing=True)
        client.close()

    cleanups.append(cleanup)
    logs = client.log_client(options=batch_options_for(ctx.scenario))
    return LoggingTarget(client=logs, query=client.log_query_client())
