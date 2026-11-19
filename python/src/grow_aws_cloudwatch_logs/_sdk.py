"""Builds the AWS SDK clients from a :class:`CloudWatchLogsConfig` and bridges the Grow hooks.

The AWS SDK signs, serialises, retries (standard mode) and parses, including the StartLiveTail
event stream. This module maps the Grow settings onto the botocore ``Config`` (timeouts,
retries, proxy, client certificate, User-Agent), the ``verify`` argument (CA bundle,
``insecure_skip_verify``) and the credentials (explicit keys, profile, STS role), and fires the
SPEC §2.5 hooks from botocore events:

* ``before-sign``: adds ``config.headers`` to every request;
* ``before-send``: ``on_request`` (method, redacted URL and headers, attempt from the
  ``amz-sdk-request`` header) and the cancellation / total-timeout checks of the sync client;
* ``response-received``: ``on_response`` for every HTTP response, ``on_error`` for transport
  errors (``will_retry`` = attempts left).
"""

from __future__ import annotations

import contextlib
import contextvars
import dataclasses
import os
import re
import tempfile
import time
from collections.abc import Callable
from typing import Any

from botocore.config import Config as BotocoreConfig

from ._aws_errors import map_aws_error
from ._core import CancelledError, CancelToken, TimeoutError, user_agent
from ._core.hooks import ErrorInfo, RequestInfo, ResponseInfo, SdkLogger, call_hook
from ._core.redaction import redact_headers, redact_url
from .config import CloudWatchLogsConfig

__all__ = ["CallState", "HookBridge", "TlsFiles", "botocore_config", "current_state"]

_ATTEMPT_RE = re.compile(rb"attempt=(\d+)")
_insecure_warned = False


@dataclasses.dataclass
class CallState:
    """Per-call state shared with the botocore event handlers (via a context variable)."""

    op: str
    total_timeout: float | None = None
    deadline: float | None = None
    cancel: CancelToken | None = None
    method: str = "POST"
    url: str = ""
    attempt: int = 1
    started: float = 0.0


_STATE: contextvars.ContextVar[CallState | None] = contextvars.ContextVar(
    "grow_aws_cloudwatch_logs_call", default=None
)


def current_state() -> contextvars.ContextVar[CallState | None]:
    """The context variable holding the current :class:`CallState`."""
    return _STATE


def botocore_config(
    config: CloudWatchLogsConfig, *, probe: bool = False, inject_host_prefix: bool = True
) -> BotocoreConfig:
    """The botocore ``Config`` for ``config``; ``probe`` = one attempt, short timeouts."""
    attempts = 1 if probe else config.retry.max_retries + 1
    connect = min(config.connect_timeout, 10.0) if probe else config.connect_timeout
    read = min(config.request_timeout, 10.0) if probe else config.request_timeout
    kwargs: dict[str, Any] = {
        "connect_timeout": connect,
        "read_timeout": read,
        "retries": {"mode": "standard", "total_max_attempts": attempts},
        "user_agent_extra": user_agent(config.user_agent_suffix),
        "inject_host_prefix": inject_host_prefix,
    }
    if config.proxy:
        kwargs["proxies"] = {"http": config.proxy, "https": config.proxy}
    if config.tls.client_cert_path and config.tls.client_key_path:
        kwargs["client_cert"] = (config.tls.client_cert_path, config.tls.client_key_path)
    return BotocoreConfig(**kwargs)


class TlsFiles:
    """Resolves the botocore ``verify`` value; writes ``ca_cert_pem`` to a private temp file."""

    def __init__(self, config: CloudWatchLogsConfig, log: SdkLogger) -> None:
        self._path: str | None = None
        tls = config.tls
        self.verify: bool | str | None = None
        if tls.insecure_skip_verify:
            global _insecure_warned
            if not _insecure_warned:
                _insecure_warned = True
                log.warn("TLS certificate verification is disabled (insecure_skip_verify)")
            self.verify = False
        elif tls.ca_cert_path:
            self.verify = tls.ca_cert_path
        elif tls.ca_cert_pem:
            fd, path = tempfile.mkstemp(prefix="grow-cwl-ca-", suffix=".pem")
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(tls.ca_cert_pem)
            self._path = path
            self.verify = path

    def close(self) -> None:
        """Remove the temporary CA file (idempotent)."""
        if self._path is not None:
            with contextlib.suppress(OSError):
                os.unlink(self._path)
            self._path = None


def _text(value: Any) -> str:
    return value.decode("latin-1") if isinstance(value, bytes) else str(value)


class HookBridge:
    """botocore event handlers that implement the Grow hooks, headers and call deadlines."""

    def __init__(self, config: CloudWatchLogsConfig, log: SdkLogger) -> None:
        self.config = config
        self.log = log
        self.max_attempts = config.retry.max_retries + 1

    def register(self, client: Any, *, probe: bool = False) -> None:
        """Register the handlers on one botocore client (its own event emitter)."""
        events = client.meta.events
        events.register("before-sign", self.before_sign)
        events.register("before-send", self.before_send)
        if probe:
            events.register("response-received", self.response_received_probe)
        else:
            events.register("response-received", self.response_received)

    def before_sign(self, request: Any = None, **_: Any) -> None:
        """Add the configured extra headers."""
        if request is not None:
            for key, value in self.config.headers.items():
                request.headers[key] = value

    def before_send(self, request: Any = None, **_: Any) -> None:
        """Check cancellation / deadline, then fire ``on_request``."""
        state = _STATE.get()
        if state is None:
            state = CallState(op="aws")
            _STATE.set(state)
        if state.cancel is not None and state.cancel.cancelled:
            raise CancelledError(f"{state.op}: cancelled")
        if state.deadline is not None and time.monotonic() >= state.deadline:
            raise TimeoutError(f"{state.op}: timed out after {state.total_timeout:g}s")
        headers = {k: _text(v) for k, v in dict(request.headers).items()}
        match = _ATTEMPT_RE.search(_text(headers.get("amz-sdk-request", "")).encode())
        state.attempt = int(match.group(1)) if match else 1
        state.method = str(request.method).upper()
        state.url = redact_url(str(request.url))
        state.started = time.monotonic()
        call_hook(
            self.config.hooks.on_request if self.config.hooks else None,
            RequestInfo(
                method=state.method,
                url=state.url,
                headers=redact_headers(headers),
                attempt=state.attempt,
            ),
        )
        self.log.debug(
            "aws-cloudwatch-logs request",
            {"method": state.method, "url": state.url, "attempt": state.attempt},
        )

    def _received(
        self, max_attempts: int, response_dict: Any = None, exception: Any = None
    ) -> None:
        state = _STATE.get()
        if state is None:
            return
        hooks = self.config.hooks
        duration = (time.monotonic() - state.started) * 1000.0
        if response_dict is not None:
            headers = response_dict.get("headers") or {}
            request_id = headers.get("x-amzn-requestid") or headers.get("x-amz-request-id")
            call_hook(
                hooks.on_response if hooks else None,
                ResponseInfo(
                    method=state.method,
                    url=state.url,
                    status=int(response_dict.get("status_code") or 0),
                    duration_ms=duration,
                    attempt=state.attempt,
                    request_id=request_id,
                ),
            )
        elif exception is not None:
            error = map_aws_error(state.op, exception, self.config.known_secrets())
            will_retry = state.attempt < max_attempts and error.retryable
            call_hook(
                hooks.on_error if hooks else None,
                ErrorInfo(
                    method=state.method,
                    url=state.url,
                    error=error,
                    attempt=state.attempt,
                    will_retry=will_retry,
                ),
            )
            self.log.debug(
                "aws-cloudwatch-logs transport error",
                {"error": error.message, "attempt": state.attempt, "willRetry": will_retry},
            )

    def response_received(self, response_dict: Any = None, exception: Any = None, **_: Any) -> None:
        """Fire ``on_response`` / ``on_error`` for one attempt."""
        self._received(self.max_attempts, response_dict, exception)

    def response_received_probe(
        self, response_dict: Any = None, exception: Any = None, **_: Any
    ) -> None:
        """Same as :meth:`response_received` for the one-attempt health probe client."""
        self._received(1, response_dict, exception)


def bind_state(state: CallState) -> Callable[[], None]:
    """Make ``state`` the current call state; returns a function that restores the old one."""
    token = _STATE.set(state)
    return lambda: _STATE.reset(token)
