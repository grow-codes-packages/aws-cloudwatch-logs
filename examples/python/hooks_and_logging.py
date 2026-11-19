"""Hooks, the SDK logger, retries, timeouts and a stdlib logging.Handler that ships to CloudWatch.

Hook data and log fields are always redacted (signatures, keys and tokens never show up).
"""

from __future__ import annotations

import logging

import grow_aws_cloudwatch_logs as sdk


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(sdk.LOGGER_NAME).setLevel(logging.DEBUG)  # silent unless enabled

    hooks = sdk.Hooks(
        on_request=lambda info: print(f"-> {info.method} {info.url} (attempt {info.attempt})"),
        on_response=lambda info: print(f"<- {info.status} in {info.duration_ms:.0f} ms"),
        on_error=lambda info: print(f"!! {info.error} retry={info.will_retry}"),
    )
    config = sdk.CloudWatchLogsConfig.from_env(
        hooks=hooks,
        request_timeout=5.0,
        total_timeout=30.0,
        retry=sdk.RetryPolicy(max_retries=2),  # AWS SDK standard retry mode, 3 attempts
        user_agent_suffix="example/1.0",
        log_group="grow-example/handler",
    )
    print(config)  # credentials print as [REDACTED]
    with sdk.CloudWatchLogsClient(config) as client:
        print(client.health())
        client.ensure_log_group("grow-example/handler", retention_days=1)
        app_log = logging.getLogger("example.app")
        handler = sdk.LogHandler(client.log_client(), exclude_loggers=("botocore", "urllib3"))
        app_log.addHandler(handler)
        app_log.warning("disk almost full", extra={"disk": "/var"})
    # close() flushed the log client


if __name__ == "__main__":
    main()
