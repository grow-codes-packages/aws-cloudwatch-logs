"""Core round trips against the real service plus the core conformance scenarios."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from factories import CORE_CAPABILITIES, async_core_client, core_client

import grow_aws_cloudwatch_logs as sdk
from grow_aws_cloudwatch_logs.testing import ConformanceRunner, assert_scenario, pytest_params


def test_health(client: sdk.CloudWatchLogsClient) -> None:
    status = client.health()
    assert status.healthy and status.status == "ok", status


def test_ping(client: sdk.CloudWatchLogsClient) -> None:
    assert client.ping() > 0


def test_server_version_is_unsupported(client: sdk.CloudWatchLogsClient) -> None:
    with pytest.raises(sdk.UnsupportedError):
        client.server_version()


def test_close_is_idempotent(env: dict[str, str]) -> None:
    c = core_client(env)
    c.close()
    c.close()
    with pytest.raises(sdk.CancelledError):
        c.ping()


def test_async_client(env: dict[str, str]) -> None:
    async def run() -> None:
        async with async_core_client(env) as c:
            assert (await c.health()).healthy
            assert await c.ping() > 0
            with pytest.raises(sdk.UnsupportedError):
                await c.server_version()

    asyncio.run(run())


def test_bad_credentials_are_rejected(env: dict[str, str]) -> None:
    secret = "definitely-wrong-secret-key"
    overrides = {"access_key_id": "AKIANOTAREALKEY00000", "secret_access_key": secret}
    with core_client(env, **overrides) as c:
        with pytest.raises(sdk.AuthenticationError) as info:
            c.ping()
        assert secret not in str(info.value) and secret not in repr(info.value)
        health = c.health()
        assert health.healthy and health.details.get("auth") == "failed"


@pytest.mark.parametrize("scenario", pytest_params(categories=["core"]))
def test_core_conformance(env: dict[str, str], scenario: dict[str, Any]) -> None:
    runner = ConformanceRunner(
        {"core": lambda ctx: core_client(env)}, capabilities=CORE_CAPABILITIES
    )
    assert_scenario(runner, scenario)
