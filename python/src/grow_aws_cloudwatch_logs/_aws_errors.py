"""AWS SDK error → SPEC §2.1 error mapping (by AWS error code first, API.md §1.1)."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Iterable, Mapping
from typing import Any

from botocore import exceptions as bce

from ._core import (
    AuthenticationError,
    CancelledError,
    CloudWatchLogsError,
    ConfigError,
    ErrorCode,
    Secret,
    error_for_code,
    redact_text,
    truncate,
)

__all__ = ["AWS_ERROR_CODES", "AwsSdkError", "aws_error_code", "map_aws_error"]

#: AWS error codes and the Grow code they map to.
AWS_ERROR_CODES: Mapping[str, ErrorCode] = {
    "ResourceNotFoundException": ErrorCode.NOT_FOUND,
    "ResourceAlreadyExistsException": ErrorCode.CONFLICT,
    "OperationAbortedException": ErrorCode.CONFLICT,
    "DataAlreadyAcceptedException": ErrorCode.CONFLICT,
    "InvalidSequenceTokenException": ErrorCode.CONFLICT,
    "ConflictException": ErrorCode.CONFLICT,
    "InvalidParameterException": ErrorCode.VALIDATION,
    "ValidationException": ErrorCode.VALIDATION,
    "MalformedQueryException": ErrorCode.VALIDATION,
    "InvalidOperationException": ErrorCode.VALIDATION,
    "LimitExceededException": ErrorCode.VALIDATION,
    "UnrecognizedClientException": ErrorCode.AUTHENTICATION,
    "InvalidClientTokenId": ErrorCode.AUTHENTICATION,
    "InvalidSignatureException": ErrorCode.AUTHENTICATION,
    "SignatureDoesNotMatch": ErrorCode.AUTHENTICATION,
    "IncompleteSignature": ErrorCode.AUTHENTICATION,
    "IncompleteSignatureException": ErrorCode.AUTHENTICATION,
    "MissingAuthenticationToken": ErrorCode.AUTHENTICATION,
    "MissingAuthenticationTokenException": ErrorCode.AUTHENTICATION,
    "ExpiredToken": ErrorCode.AUTHENTICATION,
    "ExpiredTokenException": ErrorCode.AUTHENTICATION,
    "AccessDeniedException": ErrorCode.PERMISSION_DENIED,
    "AccessDenied": ErrorCode.PERMISSION_DENIED,
    "ThrottlingException": ErrorCode.RATE_LIMITED,
    "Throttling": ErrorCode.RATE_LIMITED,
    "TooManyRequestsException": ErrorCode.RATE_LIMITED,
    "RequestLimitExceeded": ErrorCode.RATE_LIMITED,
    "ServiceUnavailableException": ErrorCode.UNAVAILABLE,
    "ServiceUnavailable": ErrorCode.UNAVAILABLE,
    "InternalFailure": ErrorCode.UNAVAILABLE,
    "InternalServerError": ErrorCode.UNAVAILABLE,
    "SessionStreamingException": ErrorCode.UNAVAILABLE,
    "SessionTimeoutException": ErrorCode.TIMEOUT,
    "RequestTimeout": ErrorCode.TIMEOUT,
    "RequestTimeoutException": ErrorCode.TIMEOUT,
}

#: AWS codes whose retryability differs from the default of their Grow code.
_RETRYABLE: Mapping[str, bool] = {
    "OperationAbortedException": True,
    "SessionStreamingException": True,
    "SessionTimeoutException": True,
}

_REASONS: Mapping[ErrorCode, str] = {
    ErrorCode.CONFIG: "invalid configuration",
    ErrorCode.AUTHENTICATION: "authentication failed",
    ErrorCode.PERMISSION_DENIED: "permission denied",
    ErrorCode.NOT_FOUND: "not found",
    ErrorCode.CONFLICT: "conflict",
    ErrorCode.RATE_LIMITED: "rate limited",
    ErrorCode.VALIDATION: "invalid request",
    ErrorCode.UNAVAILABLE: "service unavailable",
    ErrorCode.TIMEOUT: "timed out",
    ErrorCode.UNSUPPORTED: "not supported",
    ErrorCode.CANCELLED: "cancelled",
}

_AUTH_TEXT = re.compile(r"InvalidClientTokenId|SignatureDoesNotMatch|security token", re.I)

_CREDENTIAL_ERRORS: tuple[type[BaseException], ...] = (
    bce.NoCredentialsError,
    bce.PartialCredentialsError,
    bce.CredentialRetrievalError,
    bce.TokenRetrievalError,
    bce.UnauthorizedSSOTokenError,
    bce.SSOTokenLoadError,
)
_CONFIG_ERRORS: tuple[type[BaseException], ...] = (
    bce.NoRegionError,
    bce.ProfileNotFound,
    bce.InvalidRegionError,
    bce.InvalidConfigError,
)
_TIMEOUT_ERRORS: tuple[type[BaseException], ...] = (
    bce.ConnectTimeoutError,
    bce.ReadTimeoutError,
    asyncio.TimeoutError,
)


class AwsSdkError(Exception):
    """Redacted stand-in for the AWS SDK exception, kept as ``error.cause``.

    ``type_name`` is the class name of the original exception; the text has every known
    credential removed and is truncated to 300 characters.
    """

    def __init__(self, type_name: str, text: str) -> None:
        super().__init__(f"{type_name}: {text}" if text else type_name)
        self.type_name = type_name


def aws_error_code(exc: BaseException | None) -> str | None:
    """AWS error code of an SDK exception (without a ``namespace#`` prefix) or of a mapped
    SDK error (``error.aws_code``); ``None`` when there is none."""
    if exc is None:
        return None
    code = getattr(exc, "aws_code", None)
    if isinstance(code, str) and code:
        return code
    if isinstance(exc, bce.ClientError):
        raw = exc.response.get("Error", {}).get("Code")
        if isinstance(raw, str) and raw:
            return raw.rsplit("#", 1)[-1]
    return None


def _redacted_cause(exc: BaseException, secrets: list[str]) -> AwsSdkError:
    return AwsSdkError(type(exc).__name__, truncate(redact_text(str(exc), secrets)))


def _status_code(status: int) -> ErrorCode:
    if status == 401:
        return ErrorCode.AUTHENTICATION
    if status == 403:
        return ErrorCode.PERMISSION_DENIED
    if status == 404:
        return ErrorCode.NOT_FOUND
    if status in (408, 504):
        return ErrorCode.TIMEOUT
    if status == 429:
        return ErrorCode.RATE_LIMITED
    if status >= 500:
        return ErrorCode.UNAVAILABLE
    return ErrorCode.VALIDATION


def _build(
    code: ErrorCode,
    message: str,
    *,
    cause: BaseException,
    http_status: int | None = None,
    request_id: str | None = None,
    retryable: bool | None = None,
    aws_code: str | None = None,
) -> CloudWatchLogsError:
    err = error_for_code(code)(
        message,
        http_status=http_status,
        request_id=request_id,
        retryable=retryable,
        cause=cause,
    )
    err.aws_code = aws_code  # type: ignore[attr-defined]
    return err


def map_aws_error(
    operation: str,
    exc: BaseException,
    known_secrets: Iterable[str | Secret | None] = (),
) -> CloudWatchLogsError:
    """Map any exception raised by an AWS SDK call to a :class:`CloudWatchLogsError`.

    AWS error code first, then the HTTP status (a 403 without a recognised code — Moto answers
    IAM errors with XML on a JSON protocol — is ``AUTHENTICATION`` when the body names a bad key
    or signature, else ``PERMISSION_DENIED``), then credential / configuration / timeout /
    network shapes. The message is ``"<operation>: <reason>[: <AwsCode>] (HTTP <status>)"``;
    AWS messages are never copied (they may echo log data). ``error.aws_code`` holds the AWS
    code and ``error.cause`` a redacted :class:`AwsSdkError`.
    """
    if isinstance(exc, CloudWatchLogsError):
        return exc
    secrets = [s.reveal() if isinstance(s, Secret) else s for s in known_secrets if s]
    cause = _redacted_cause(exc, [s for s in secrets if isinstance(s, str)])
    if isinstance(exc, asyncio.CancelledError):
        return CancelledError(f"{operation}: cancelled", cause=cause)
    if isinstance(exc, bce.ClientError):
        response: Mapping[str, Any] = exc.response or {}
        meta = response.get("ResponseMetadata") or {}
        status = meta.get("HTTPStatusCode")
        status = status if isinstance(status, int) else None
        request_id = meta.get("RequestId") or (meta.get("HTTPHeaders") or {}).get(
            "x-amzn-requestid"
        )
        aws_code = aws_error_code(exc)
        code = AWS_ERROR_CODES.get(aws_code or "")
        named = code is not None
        if code is None and status == 403:
            text = f"{(response.get('Error') or {}).get('Message', '')} {aws_code or ''}"
            code = (
                ErrorCode.AUTHENTICATION if _AUTH_TEXT.search(text) else ErrorCode.PERMISSION_DENIED
            )
        if code is None:
            code = _status_code(status) if status is not None and status >= 400 else None
        if code is None:
            code = ErrorCode.UNAVAILABLE
        reason = _REASONS[code] + (f": {aws_code}" if named else "")
        suffix = "" if status is None else f" (HTTP {status})"
        return _build(
            code,
            f"{operation}: {reason}{suffix}",
            cause=cause,
            http_status=status,
            request_id=request_id if isinstance(request_id, str) else None,
            retryable=_RETRYABLE.get(aws_code or ""),
            aws_code=aws_code,
        )
    if isinstance(exc, _CREDENTIAL_ERRORS):
        return AuthenticationError(f"{operation}: no usable AWS credentials", cause=cause)
    if isinstance(exc, _CONFIG_ERRORS):
        what = (
            "AWS region is not configured"
            if isinstance(exc, bce.NoRegionError)
            else (f"invalid AWS configuration ({type(exc).__name__})")
        )
        return ConfigError(f"{operation}: {what}", cause=cause)
    if isinstance(exc, bce.ParamValidationError):
        return _build(ErrorCode.VALIDATION, f"{operation}: invalid request parameters", cause=cause)
    if isinstance(exc, _TIMEOUT_ERRORS) or type(exc).__name__ == "TimeoutError":
        return _build(ErrorCode.TIMEOUT, f"{operation}: timed out", cause=cause)
    name = type(exc).__name__
    return _build(ErrorCode.UNAVAILABLE, f"{operation}: service unavailable: {name}", cause=cause)
