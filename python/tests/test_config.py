"""CloudWatchLogsConfig: defaults, environment precedence, validation and redaction."""

from __future__ import annotations

import re
from typing import Any

import pytest

import grow_aws_cloudwatch_logs as sdk

P = "GROW_AWS_CLOUDWATCH_LOGS_"


def test_defaults() -> None:
    cfg = sdk.CloudWatchLogsConfig()
    assert cfg.url == "" and cfg.endpoint is None
    assert cfg.effective_url == "https://logs.us-east-1.amazonaws.com"
    assert sdk.CloudWatchLogsConfig(region="eu-west-1").effective_url.endswith(
        "eu-west-1.amazonaws.com"
    )
    assert cfg.max_event_bytes == sdk.DEFAULT_MAX_EVENT_BYTES == 262_144
    assert cfg.oversize_policy == "truncate" and cfg.auto_create is True
    assert cfg.role_session_name == sdk.DEFAULT_ROLE_SESSION_NAME == "grow-sdk"
    assert re.match(r"^.+-\d+-[0-9a-f]{6}$", cfg.log_stream)
    assert sdk.is_valid_log_stream_name(cfg.log_stream)
    assert cfg.validate() is cfg
    assert sdk.API_VERSION == "2014-03-28"


def test_blank_values_are_unset() -> None:
    cfg = sdk.CloudWatchLogsConfig(
        region=" ",
        profile="",
        log_group="",
        role_session_name=" ",
        secret_access_key="  ",
        url="  ",
        log_stream="",
    )
    assert cfg.region is None and cfg.profile is None and cfg.log_group is None
    assert cfg.secret_access_key is None and cfg.url == ""
    assert cfg.role_session_name == "grow-sdk" and cfg.log_stream


def test_env_precedence_and_vendor_variables() -> None:
    env = {
        "AWS_ENDPOINT_URL": "http://generic:1",
        "AWS_ENDPOINT_URL_CLOUDWATCH_LOGS": "http://logs:1",
        "AWS_DEFAULT_REGION": "eu-central-1",
        "AWS_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "AKIAAWS",
        "AWS_SECRET_ACCESS_KEY": "aws-secret-1234",
        "AWS_SESSION_TOKEN": "aws-session-1234",
        "AWS_PROFILE": "dev",
        "CLOUDWATCH_LOG_GROUP": "vendor/group",
        "CLOUDWATCH_LOG_STREAM": "vendor-stream",
    }
    cfg = sdk.CloudWatchLogsConfig.from_env(env)
    assert cfg.url == "http://logs:1" and cfg.region == "eu-west-1"
    assert cfg.access_key_id == "AKIAAWS" and cfg.secret_access_key is not None
    assert cfg.session_token is not None and cfg.session_token.reveal() == "aws-session-1234"
    assert (cfg.profile, cfg.log_group, cfg.log_stream) == ("dev", "vendor/group", "vendor-stream")
    del env["AWS_ENDPOINT_URL_CLOUDWATCH_LOGS"]
    assert sdk.CloudWatchLogsConfig.from_env(env).url == "http://generic:1"
    env.update(
        {
            P + "URL": "http://grow:1",
            P + "REGION": "us-west-2",
            P + "ACCESS_KEY_ID": "AKIAGROW",
            P + "SECRET_ACCESS_KEY": "grow-secret-1234",
            P + "PROFILE": "grow",
            P + "ROLE_ARN": "arn:aws:iam::123456789012:role/r",
            P + "ROLE_SESSION_NAME": "sess",
            P + "EXTERNAL_ID": "ext-1234",
            P + "LOG_GROUP": "grow/group",
            P + "LOG_STREAM": "grow-stream",
            P + "ACCOUNT_ID": "123456789012",
            P + "MAX_EVENT_BYTES": "1024",
            P + "OVERSIZE_POLICY": "DROP",
            P + "AUTO_CREATE": "false",
        }
    )
    cfg = sdk.CloudWatchLogsConfig.from_env(env)
    assert cfg.url == "http://grow:1" and cfg.region == "us-west-2"
    assert cfg.access_key_id == "AKIAGROW" and cfg.session_token is None  # one layer only
    assert (
        cfg.secret_access_key is not None and cfg.secret_access_key.reveal() == "grow-secret-1234"
    )
    assert cfg.role_arn and cfg.role_session_name == "sess"
    assert cfg.external_id is not None and cfg.external_id.reveal() == "ext-1234"
    assert (cfg.log_group, cfg.log_stream, cfg.account_id) == (
        "grow/group",
        "grow-stream",
        "123456789012",
    )
    assert (cfg.max_event_bytes, cfg.oversize_policy, cfg.auto_create) == (1024, "drop", False)
    explicit = sdk.CloudWatchLogsConfig.from_env(env, region="ap-south-1", log_group=None)
    assert explicit.region == "ap-south-1" and explicit.log_group == "grow/group"


def test_env_sdk_client_drops_environment_credentials() -> None:
    env = {
        "AWS_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "AKIA",
        "AWS_SECRET_ACCESS_KEY": "s3cret-1",
        P + "URL": "http://x:1",
        P + "LOG_GROUP": "g",
    }
    cfg = sdk.CloudWatchLogsConfig.from_env(env, sdk_client=object())
    assert cfg.region is None and cfg.access_key_id is None and cfg.url == ""
    assert cfg.log_group == "g"


@pytest.mark.parametrize(
    ("var", "value"),
    [("OVERSIZE_POLICY", "shrink"), ("MAX_EVENT_BYTES", "big"), ("AUTO_CREATE", "maybe")],
)
def test_env_invalid_values_name_the_variable(var: str, value: str) -> None:
    with pytest.raises(sdk.ConfigError) as info:
        sdk.CloudWatchLogsConfig.from_env({P + var: value})
    assert P + var in str(info.value) and value not in str(info.value)


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"region": "EU WEST"}, "region"),
        ({"access_key_id": "AKIA"}, "requires secret_access_key"),
        ({"secret_access_key": "s3cret"}, "requires access_key_id"),
        ({"session_token": "tok-1234"}, "session_token"),
        ({"sdk_client": object(), "region": "us-east-1", "url": "http://x"}, "sdk_client"),
        ({"role_arn": "role"}, "role_arn"),
        ({"log_group": "bad group!"}, "log_group"),
        ({"log_stream": "a:b"}, "log_stream"),
        ({"account_id": "12"}, "account_id"),
        ({"max_event_bytes": 10}, "max_event_bytes"),
        ({"max_event_bytes": True}, "max_event_bytes"),
        ({"oversize_policy": "shrink"}, "oversize_policy"),
        ({"url": "ftp://x"}, "url"),
        ({"connect_timeout": 0}, "connect_timeout"),
    ],
)
def test_validation(kwargs: dict[str, Any], fragment: str) -> None:
    with pytest.raises(sdk.ConfigError) as info:
        sdk.CloudWatchLogsConfig(**kwargs).validate()
    assert fragment in str(info.value)


def test_repr_redacts_credentials() -> None:
    cfg = sdk.CloudWatchLogsConfig(
        access_key_id="AKIAEXAMPLEKEY",
        secret_access_key="very-secret-key",
        session_token="very-secret-token",
        external_id="very-secret-ext",
    )
    text = repr(cfg)
    for secret in ("very-secret-key", "very-secret-token", "very-secret-ext", "AKIAEXAMPLEKEY"):
        assert secret not in text
    assert set(cfg.known_secrets()) == {"very-secret-key", "very-secret-token", "very-secret-ext"}


def test_name_validators() -> None:
    assert sdk.is_valid_log_group_name("a/b-c_d.e#f")
    assert not sdk.is_valid_log_group_name("a b") and not sdk.is_valid_log_group_name("")
    assert not sdk.is_valid_log_group_name("x" * 513) and not sdk.is_valid_log_group_name(3)
    assert sdk.is_valid_log_stream_name("any thing")
    assert not sdk.is_valid_log_stream_name("a*b") and not sdk.is_valid_log_stream_name("")
    assert sdk.default_log_stream_name() != sdk.default_log_stream_name()
