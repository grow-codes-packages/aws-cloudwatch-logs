// Service-owned: AWS SDK error → SPEC §2.1 error mapping (by AWS error code first, API.md §1.1).

import {
  AuthenticationError,
  CancelledError,
  CloudWatchLogsError,
  ConfigError,
  ErrorCode,
  type ErrorDetails,
  type KnownSecret,
  errorForCode,
  reasonForCode,
  redactError,
} from "./core/index.js";

/** AWS error codes and the Grow code they map to (some codes override the default retryability). */
export const AWS_ERROR_CODES: Readonly<Record<string, ErrorCode>> = Object.freeze({
  ResourceNotFoundException: ErrorCode.NotFound,
  ResourceAlreadyExistsException: ErrorCode.Conflict,
  OperationAbortedException: ErrorCode.Conflict,
  DataAlreadyAcceptedException: ErrorCode.Conflict,
  InvalidSequenceTokenException: ErrorCode.Conflict,
  ConflictException: ErrorCode.Conflict,
  InvalidParameterException: ErrorCode.Validation,
  ValidationException: ErrorCode.Validation,
  MalformedQueryException: ErrorCode.Validation,
  InvalidOperationException: ErrorCode.Validation,
  LimitExceededException: ErrorCode.Validation,
  UnrecognizedClientException: ErrorCode.Authentication,
  InvalidClientTokenId: ErrorCode.Authentication,
  InvalidSignatureException: ErrorCode.Authentication,
  SignatureDoesNotMatch: ErrorCode.Authentication,
  IncompleteSignature: ErrorCode.Authentication,
  IncompleteSignatureException: ErrorCode.Authentication,
  MissingAuthenticationToken: ErrorCode.Authentication,
  MissingAuthenticationTokenException: ErrorCode.Authentication,
  ExpiredToken: ErrorCode.Authentication,
  ExpiredTokenException: ErrorCode.Authentication,
  CredentialsProviderError: ErrorCode.Authentication,
  AccessDeniedException: ErrorCode.PermissionDenied,
  AccessDenied: ErrorCode.PermissionDenied,
  ThrottlingException: ErrorCode.RateLimited,
  Throttling: ErrorCode.RateLimited,
  TooManyRequestsException: ErrorCode.RateLimited,
  RequestLimitExceeded: ErrorCode.RateLimited,
  ServiceUnavailableException: ErrorCode.Unavailable,
  ServiceUnavailable: ErrorCode.Unavailable,
  InternalFailure: ErrorCode.Unavailable,
  InternalServerError: ErrorCode.Unavailable,
  SessionStreamingException: ErrorCode.Unavailable,
  SessionTimeoutException: ErrorCode.Timeout,
  RequestTimeout: ErrorCode.Timeout,
  RequestTimeoutException: ErrorCode.Timeout,
});

/** AWS codes whose default retryability differs from their Grow code. */
const RETRYABLE_CODES: Readonly<Record<string, boolean>> = Object.freeze({
  OperationAbortedException: true,
  SessionStreamingException: true,
  SessionTimeoutException: true,
});

const NETWORK_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ENOTFOUND",
  "EAI_AGAIN",
  "EPIPE",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "ECONNABORTED",
  "UND_ERR_SOCKET",
  "UND_ERR_CLOSED",
  "UND_ERR_CONNECT_TIMEOUT",
]);

interface AwsErrorShape {
  name?: unknown;
  code?: unknown;
  message?: unknown;
  Code?: unknown;
  $metadata?: { httpStatusCode?: unknown; requestId?: unknown };
  $response?: { statusCode?: unknown; body?: unknown };
  $responseBodyText?: unknown;
  cause?: unknown;
}

function str(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

/** AWS error code of an SDK error (`name`, without a `namespace#` prefix). */
export function awsErrorCode(err: unknown): string | undefined {
  if (typeof err !== "object" || err === null) return undefined;
  const e = err as AwsErrorShape;
  const raw = str(e.Code) ?? str(e.name) ?? str(e.code);
  if (raw === undefined) return undefined;
  const hash = raw.lastIndexOf("#");
  return hash >= 0 ? raw.slice(hash + 1) : raw;
}

function errorText(err: AwsErrorShape): string {
  const body = err.$response?.body;
  const bodyText =
    body instanceof Uint8Array ? Buffer.from(body.subarray(0, 4096)).toString("utf8") : str(body);
  return [str(err.message), str(err.$responseBodyText), bodyText, str(err.code), str(err.name)]
    .filter((v) => v !== undefined)
    .join(" ");
}

/** Node / undici network error code found on the error or its causes. */
function networkCode(err: unknown): string | undefined {
  let current: unknown = err;
  for (let depth = 0; depth < 5 && typeof current === "object" && current !== null; depth++) {
    const code = (current as { code?: unknown }).code;
    if (typeof code === "string" && NETWORK_CODES.has(code)) return code;
    current = (current as { cause?: unknown }).cause;
  }
  return undefined;
}

/**
 * Maps any error thrown by an AWS SDK call to a {@link CloudWatchLogsError} (API.md §1.1):
 * AWS error code first, then the HTTP status, then network / timeout / cancellation shapes. The
 * message is `"<operation>: <reason>: <AwsCode> (HTTP <status>)"`; AWS messages are never copied
 * (they may echo log data), and the cause is redacted with `knownSecrets`.
 */
export function mapAwsError(
  operation: string,
  err: unknown,
  knownSecrets: Iterable<KnownSecret> = [],
): CloudWatchLogsError {
  if (err instanceof CloudWatchLogsError) return err;
  const e = (typeof err === "object" && err !== null ? err : {}) as AwsErrorShape;
  const secrets = [...knownSecrets];
  const cause = redactError(err, secrets);
  const awsCode = awsErrorCode(err);
  const rawStatus = e.$metadata?.httpStatusCode ?? e.$response?.statusCode;
  const status = typeof rawStatus === "number" ? rawStatus : undefined;
  const requestId = str(e.$metadata?.requestId);
  const details: ErrorDetails = { cause };
  if (status !== undefined) details.httpStatus = status;
  if (requestId !== undefined) details.requestId = requestId;
  const suffix = status === undefined ? "" : ` (HTTP ${status})`;

  if (awsCode === "AbortError" || awsCode === "CancelledError") {
    return new CancelledError(`${operation}: cancelled`, details);
  }
  if (awsCode === "CredentialsProviderError" || awsCode === "TokenProviderError") {
    return new AuthenticationError(`${operation}: no usable AWS credentials`, details);
  }
  if (/region is missing/i.test(str(e.message) ?? "")) {
    return new ConfigError(`${operation}: AWS region is not configured`, details);
  }
  let code = awsCode === undefined ? undefined : AWS_ERROR_CODES[awsCode];
  if (code === undefined && status === 403) {
    // Moto answers IAM errors with XML on a JSON protocol: the SDK cannot read the code.
    code = /InvalidClientTokenId|SignatureDoesNotMatch|security token/i.test(errorText(e))
      ? ErrorCode.Authentication
      : ErrorCode.PermissionDenied;
  }
  if (code === undefined && status !== undefined && status >= 400) {
    code =
      status === 401
        ? ErrorCode.Authentication
        : status === 404
          ? ErrorCode.NotFound
          : status === 408 || status === 504
            ? ErrorCode.Timeout
            : status === 429
              ? ErrorCode.RateLimited
              : status >= 500
                ? ErrorCode.Unavailable
                : ErrorCode.Validation;
  }
  if (code !== undefined) {
    if (awsCode !== undefined && RETRYABLE_CODES[awsCode] !== undefined) {
      details.retryable = RETRYABLE_CODES[awsCode];
    }
    const named = awsCode !== undefined && AWS_ERROR_CODES[awsCode] !== undefined;
    const reason = reasonForCode(code) + (named ? `: ${awsCode}` : "");
    return errorForCode(code, `${operation}: ${reason}${suffix}`, details);
  }
  if (awsCode === "TimeoutError" || (e as { code?: unknown }).code === "ETIMEDOUT") {
    return errorForCode(ErrorCode.Timeout, `${operation}: timed out`, details);
  }
  const net = networkCode(err);
  if (net !== undefined) {
    return errorForCode(
      ErrorCode.Unavailable,
      `${operation}: service unavailable: ${net}`,
      details,
    );
  }
  return errorForCode(ErrorCode.Unavailable, `${operation}: service unavailable`, details);
}
