"""Configuration for the Amazon CloudWatch Logs client.

This file is owned by the service package. The client is built on the official AWS SDK for
Python (boto3 for :class:`~grow_aws_cloudwatch_logs.CloudWatchLogsClient`, aioboto3 /
aiobotocore for :class:`~grow_aws_cloudwatch_logs.AsyncCloudWatchLogsClient`), see SPEC §2.3
"Cloud SDK services": the Grow settings are mapped onto the SDK client.
"""

from __future__ import annotations

import dataclasses
import os
import re
import secrets
import socket
from collections.abc import Mapping
from typing import Any, ClassVar, Literal, TypeVar

from ._core import BaseConfig, ConfigError, EnvReader, Secret

__all__ = [
    "API_VERSION",
    "DEFAULT_MAX_EVENT_BYTES",
    "DEFAULT_ROLE_SESSION_NAME",
    "DEFAULT_URL",
    "MAX_EVENT_BYTES_HARD_CAP",
    "OVERSIZE_POLICIES",
    "CloudWatchLogsConfig",
    "OversizePolicy",
    "default_log_stream_name",
    "is_valid_log_group_name",
    "is_valid_log_stream_name",
]

#: Default endpoint override: empty, so the AWS SDK resolves ``https://logs.<region>.amazonaws.com``.
DEFAULT_URL = ""

#: CloudWatch Logs API version.
API_VERSION = "2014-03-28"

#: Default per-event message limit in UTF-8 bytes (256 KiB).
DEFAULT_MAX_EVENT_BYTES = 262_144

#: Largest message PutLogEvents accepts: 1 MiB minus the 26 bytes AWS counts per event.
MAX_EVENT_BYTES_HARD_CAP = 1_048_550

#: Default STS role session name used with ``role_arn``.
DEFAULT_ROLE_SESSION_NAME = "grow-sdk"

#: What happens to a log event whose message is larger than ``max_event_bytes``.
OversizePolicy = Literal["truncate", "drop", "error"]

#: Accepted ``oversize_policy`` values.
OVERSIZE_POLICIES: tuple[str, ...] = ("truncate", "drop", "error")

_GROUP_RE = re.compile(r"^[.\-_/#A-Za-z0-9]{1,512}$")
_REGION_RE = re.compile(r"^[a-z0-9-]+$")
_ACCOUNT_RE = re.compile(r"^\d{12}$")
_STREAM_BAD = re.compile(r"[:*]")
_HOST_BAD = re.compile(r"[:*\s]")

_C = TypeVar("_C", bound="CloudWatchLogsConfig")


def is_valid_log_group_name(name: object) -> bool:
    """``True`` when ``name`` is a valid log group name (1-512 of ``[.-_/#A-Za-z0-9]``)."""
    return isinstance(name, str) and _GROUP_RE.match(name) is not None


def is_valid_log_stream_name(name: object) -> bool:
    """``True`` when ``name`` is a valid log stream name (1-512 characters, no ``:`` or ``*``)."""
    return isinstance(name, str) and 1 <= len(name) <= 512 and _STREAM_BAD.search(name) is None


def default_log_stream_name() -> str:
    """Default stream name ``<hostname>-<pid>-<random6>`` (``:``, ``*`` and spaces become ``-``)."""
    host = _HOST_BAD.sub("-", socket.gethostname()) or "host"
    return f"{host}-{os.getpid()}-{secrets.token_hex(3)}"


def _blank_to_none(value: str | None) -> str | None:
    return None if value is None or not str(value).strip() else value


@dataclasses.dataclass(frozen=True, repr=False)
class CloudWatchLogsConfig(BaseConfig):
    """Immutable settings for the CloudWatch Logs clients.

    Common fields (``url``, timeouts, ``retry``, ``tls``, ``proxy``, ``headers``,
    ``user_agent_suffix``, ``hooks``, ``logger``) come from :class:`BaseConfig`; durations are
    float seconds (``datetime.timedelta`` accepted). How they reach the AWS SDK: ``url`` is the
    endpoint override (empty = the SDK resolves the regional endpoint), ``connect_timeout`` /
    ``request_timeout`` become the botocore connect / read timeouts, ``retry.max_retries`` the
    standard retry mode with ``max_retries + 1`` attempts, ``total_timeout`` bounds a whole call,
    ``tls`` / ``proxy`` the botocore ``verify`` / ``client_cert`` / ``proxies`` settings,
    ``headers`` are added to every request and ``user_agent_suffix`` follows the Grow
    User-Agent token. Credentials left unset use the AWS default chain (environment, shared
    config / SSO, web identity, container, instance metadata).

    Environment (:meth:`from_env`), prefix ``GROW_AWS_CLOUDWATCH_LOGS_``, then the standard
    variables: ``URL`` (> ``AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`` > ``AWS_ENDPOINT_URL``),
    ``REGION`` (> ``AWS_REGION`` > ``AWS_DEFAULT_REGION``), ``ACCESS_KEY_ID`` /
    ``SECRET_ACCESS_KEY`` / ``SESSION_TOKEN`` (> ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``
    / ``AWS_SESSION_TOKEN``, one layer only), ``PROFILE`` (> ``AWS_PROFILE``), ``ROLE_ARN``,
    ``ROLE_SESSION_NAME``, ``EXTERNAL_ID``, ``LOG_GROUP`` (> ``CLOUDWATCH_LOG_GROUP``),
    ``LOG_STREAM`` (> ``CLOUDWATCH_LOG_STREAM``), ``ACCOUNT_ID``, ``MAX_EVENT_BYTES``,
    ``OVERSIZE_POLICY``, ``AUTO_CREATE`` plus the common ``CONNECT_TIMEOUT``,
    ``REQUEST_TIMEOUT``, ``TOTAL_TIMEOUT``, ``MAX_RETRIES``, ``CA_CERT``, ``CLIENT_CERT``,
    ``CLIENT_KEY``, ``INSECURE_SKIP_VERIFY``, ``PROXY``.

    Attributes:
        url: endpoint override; empty (default) = ``https://logs.<region>.amazonaws.com``.
        region: AWS region (``None`` = the SDK chain: environment, profile).
        access_key_id: access key id (requires ``secret_access_key``).
        secret_access_key: secret access key (:class:`Secret`).
        session_token: session token for temporary credentials (:class:`Secret`).
        profile: shared config profile.
        role_arn: role assumed with STS on top of the base credentials.
        role_session_name: STS session name (default ``grow-sdk``).
        external_id: STS external id (:class:`Secret`).
        log_group: default log group of the log client, the query client and ``emf``.
        log_stream: log stream of the log client (default ``<hostname>-<pid>-<random6>``,
            created on first use).
        account_id: 12-digit account id used to build log group ARNs for live tail (else they
            are looked up with ``DescribeLogGroups``).
        max_event_bytes: per-event message limit in UTF-8 bytes (64 - 1,048,550).
        oversize_policy: ``truncate`` (default), ``drop`` or ``error`` for larger events.
        auto_create: ``put_log_events`` creates a missing log stream and retries once.
        sdk_client: an existing AWS SDK client to use instead of building one (a boto3 ``logs``
            client for the sync client, an entered aiobotocore ``logs`` client for the async
            client). Mutually exclusive with the credential, region, profile, role and endpoint
            fields; ``close()`` never closes it and the Grow hooks, TLS, proxy, timeout, header
            and User-Agent settings do not apply to it.
    """

    ENV_VENDOR: ClassVar[Mapping[str, tuple[str, ...]]] = {
        "URL": ("AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "AWS_ENDPOINT_URL"),
        "REGION": ("AWS_REGION", "AWS_DEFAULT_REGION"),
        "PROFILE": ("AWS_PROFILE",),
        "LOG_GROUP": ("CLOUDWATCH_LOG_GROUP",),
        "LOG_STREAM": ("CLOUDWATCH_LOG_STREAM",),
    }

    url: str = DEFAULT_URL
    region: str | None = None
    access_key_id: str | None = None
    secret_access_key: Secret | None = None
    session_token: Secret | None = None
    profile: str | None = None
    role_arn: str | None = None
    role_session_name: str = DEFAULT_ROLE_SESSION_NAME
    external_id: Secret | None = None
    log_group: str | None = None
    log_stream: str = ""
    account_id: str | None = None
    max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES
    oversize_policy: OversizePolicy = "truncate"
    auto_create: bool = True
    sdk_client: Any = dataclasses.field(default=None, compare=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        for name in (
            "region",
            "access_key_id",
            "profile",
            "role_arn",
            "log_group",
            "account_id",
        ):
            object.__setattr__(self, name, _blank_to_none(getattr(self, name)))
        object.__setattr__(self, "url", (self.url or "").strip())
        if not (self.role_session_name or "").strip():
            object.__setattr__(self, "role_session_name", DEFAULT_ROLE_SESSION_NAME)
        if not (self.log_stream or "").strip():
            object.__setattr__(self, "log_stream", default_log_stream_name())
        for name in ("secret_access_key", "session_token", "external_id"):
            value = getattr(self, name)
            if isinstance(value, Secret) and not value.reveal().strip():
                object.__setattr__(self, name, None)

    # ------------------------------------------------------------------ derived values

    @property
    def endpoint(self) -> str | None:
        """The explicit endpoint override passed to the AWS SDK (``None`` = SDK resolves it)."""
        return self.url or None

    @property
    def effective_url(self) -> str:
        """``url`` when set, else the public endpoint of ``region`` (``us-east-1`` when unset)."""
        return self.url or f"https://logs.{self.region or 'us-east-1'}.amazonaws.com"

    # ------------------------------------------------------------------ environment

    @classmethod
    def _env_values(cls, reader: EnvReader) -> dict[str, Any]:
        values = super()._env_values(reader)
        vendor = cls.ENV_VENDOR

        def put(key: str, value: Any) -> None:
            if value is not None:
                values[key] = value

        put("region", reader.string("REGION", vendor.get("REGION", ())))
        # Credentials come from one layer: a GROW_* key id never pairs with an AWS_* secret.
        grow_key = reader.string("ACCESS_KEY_ID")
        if grow_key is not None:
            put("access_key_id", grow_key)
            put("secret_access_key", reader.string("SECRET_ACCESS_KEY"))
            put("session_token", reader.string("SESSION_TOKEN"))
        else:
            plain = EnvReader(reader._env, prefix="")
            put("access_key_id", plain.string("AWS_ACCESS_KEY_ID"))
            put("secret_access_key", plain.string("AWS_SECRET_ACCESS_KEY"))
            put("session_token", plain.string("AWS_SESSION_TOKEN"))
        put("profile", reader.string("PROFILE", vendor.get("PROFILE", ())))
        put("role_arn", reader.string("ROLE_ARN"))
        put("role_session_name", reader.string("ROLE_SESSION_NAME"))
        put("external_id", reader.string("EXTERNAL_ID"))
        put("log_group", reader.string("LOG_GROUP", vendor.get("LOG_GROUP", ())))
        put("log_stream", reader.string("LOG_STREAM", vendor.get("LOG_STREAM", ())))
        put("account_id", reader.string("ACCOUNT_ID"))
        put("max_event_bytes", reader.int("MAX_EVENT_BYTES"))
        found = reader.lookup("OVERSIZE_POLICY")
        if found is not None:
            policy = found[1].strip().lower()
            if policy not in OVERSIZE_POLICIES:
                raise ConfigError(f"config: {found[0]} must be one of truncate, drop or error")
            values["oversize_policy"] = policy
        put("auto_create", reader.bool("AUTO_CREATE"))
        return values

    @classmethod
    def from_env(cls: type[_C], env: Mapping[str, str] | None = None, **overrides: Any) -> _C:
        """Build from the environment, then validate.

        Precedence: explicit keyword > ``GROW_AWS_CLOUDWATCH_LOGS_<NAME>`` > the standard AWS /
        CloudWatch variables > default. ``env`` defaults to ``os.environ``; pass a dict in
        tests. An explicit ``sdk_client`` replaces the credentials, region, profile, role and
        endpoint found in the environment.
        """
        values = cls._env_values(EnvReader(env))
        explicit = {k: v for k, v in overrides.items() if v is not None}
        if "sdk_client" in explicit:
            for key in (
                "access_key_id",
                "secret_access_key",
                "session_token",
                "region",
                "url",
                "profile",
                "role_arn",
            ):
                values.pop(key, None)
        values.update(explicit)
        return cls(**values).validate()

    # ------------------------------------------------------------------ validation

    def validate(self: _C) -> _C:
        """Common checks plus the CloudWatch Logs rules; raises :class:`ConfigError`."""
        if self.url:
            super().validate()
        else:
            BaseConfig.validate(dataclasses.replace(self, url=self.effective_url))
        problems: list[str] = []
        if self.region is not None and not _REGION_RE.match(self.region):
            problems.append("region must look like us-east-1")
        if self.access_key_id is not None and self.secret_access_key is None:
            problems.append("access_key_id requires secret_access_key")
        if self.access_key_id is None and self.secret_access_key is not None:
            problems.append("secret_access_key requires access_key_id")
        if self.session_token is not None and self.access_key_id is None:
            problems.append("session_token requires access_key_id and secret_access_key")
        if self.sdk_client is not None:
            clash = [
                name
                for name, value in (
                    ("access_key_id", self.access_key_id),
                    ("profile", self.profile),
                    ("role_arn", self.role_arn),
                    ("region", self.region),
                    ("url", self.endpoint),
                )
                if value is not None
            ]
            if clash:
                problems.append(f"sdk_client is mutually exclusive with {', '.join(clash)}")
        if self.role_arn is not None and not self.role_arn.startswith("arn:"):
            problems.append("role_arn must be an ARN")
        if self.log_group is not None and not is_valid_log_group_name(self.log_group):
            problems.append("log_group must be 1-512 characters of [.-_/#A-Za-z0-9]")
        if not is_valid_log_stream_name(self.log_stream):
            problems.append("log_stream must be 1-512 characters without ':' or '*'")
        if self.account_id is not None and not _ACCOUNT_RE.match(self.account_id):
            problems.append("account_id must be 12 digits")
        if (
            isinstance(self.max_event_bytes, bool)
            or not isinstance(self.max_event_bytes, int)
            or not 64 <= self.max_event_bytes <= MAX_EVENT_BYTES_HARD_CAP
        ):
            problems.append(
                f"max_event_bytes must be an integer from 64 to {MAX_EVENT_BYTES_HARD_CAP}"
            )
        if self.oversize_policy not in OVERSIZE_POLICIES:
            problems.append("oversize_policy must be truncate, drop or error")
        if problems:
            raise ConfigError(f"config: invalid configuration: {'; '.join(problems)}")
        return self
