"""Logging conformance scenarios against the real service (one log group per scenario)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from factories import LOGGING_CAPABILITIES, logging_target

from grow_aws_cloudwatch_logs.testing import ConformanceRunner, assert_scenario, pytest_params


@pytest.mark.parametrize("scenario", pytest_params(categories=["logging"]))
def test_logging_conformance(env: dict[str, str], scenario: dict[str, Any]) -> None:
    cleanups: list[Callable[[], None]] = []
    runner = ConformanceRunner(
        {"logging": lambda ctx: logging_target(env, ctx, cleanups)},
        capabilities=LOGGING_CAPABILITIES,
        service_name="grow-conformance-python",
    )
    try:
        assert_scenario(runner, scenario)
    finally:
        for cleanup in cleanups:
            cleanup()
