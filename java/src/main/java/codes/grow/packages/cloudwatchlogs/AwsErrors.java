package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.CancelledException;
import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import codes.grow.packages.cloudwatchlogs.core.ErrorCode;
import codes.grow.packages.cloudwatchlogs.core.Redaction;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Maps AWS SDK errors to the SPEC section 2.1 exception classes (API.md section 1.1): AWS error
 * code first, then the HTTP status, then network, timeout and cancellation shapes. Messages read
 * {@code "<operation>: <reason>: <AwsCode> (HTTP <status>)"}; AWS messages are never copied into
 * the message (they may echo log data) and the cause is redacted with the known secrets.
 */
public final class AwsErrors {
  /** AWS error codes and the Grow code they map to. */
  public static final Map<String, ErrorCode> AWS_ERROR_CODES =
      Map.ofEntries(
          Map.entry("ResourceNotFoundException", ErrorCode.NOT_FOUND),
          Map.entry("ResourceAlreadyExistsException", ErrorCode.CONFLICT),
          Map.entry("OperationAbortedException", ErrorCode.CONFLICT),
          Map.entry("DataAlreadyAcceptedException", ErrorCode.CONFLICT),
          Map.entry("InvalidSequenceTokenException", ErrorCode.CONFLICT),
          Map.entry("ConflictException", ErrorCode.CONFLICT),
          Map.entry("InvalidParameterException", ErrorCode.VALIDATION),
          Map.entry("ValidationException", ErrorCode.VALIDATION),
          Map.entry("MalformedQueryException", ErrorCode.VALIDATION),
          Map.entry("InvalidOperationException", ErrorCode.VALIDATION),
          Map.entry("LimitExceededException", ErrorCode.VALIDATION),
          Map.entry("UnrecognizedClientException", ErrorCode.AUTHENTICATION),
          Map.entry("InvalidClientTokenId", ErrorCode.AUTHENTICATION),
          Map.entry("InvalidSignatureException", ErrorCode.AUTHENTICATION),
          Map.entry("SignatureDoesNotMatch", ErrorCode.AUTHENTICATION),
          Map.entry("IncompleteSignature", ErrorCode.AUTHENTICATION),
          Map.entry("IncompleteSignatureException", ErrorCode.AUTHENTICATION),
          Map.entry("MissingAuthenticationToken", ErrorCode.AUTHENTICATION),
          Map.entry("MissingAuthenticationTokenException", ErrorCode.AUTHENTICATION),
          Map.entry("ExpiredToken", ErrorCode.AUTHENTICATION),
          Map.entry("ExpiredTokenException", ErrorCode.AUTHENTICATION),
          Map.entry("AccessDeniedException", ErrorCode.PERMISSION_DENIED),
          Map.entry("AccessDenied", ErrorCode.PERMISSION_DENIED),
          Map.entry("ThrottlingException", ErrorCode.RATE_LIMITED),
          Map.entry("Throttling", ErrorCode.RATE_LIMITED),
          Map.entry("TooManyRequestsException", ErrorCode.RATE_LIMITED),
          Map.entry("RequestLimitExceeded", ErrorCode.RATE_LIMITED),
          Map.entry("ServiceUnavailableException", ErrorCode.UNAVAILABLE),
          Map.entry("ServiceUnavailable", ErrorCode.UNAVAILABLE),
          Map.entry("InternalFailure", ErrorCode.UNAVAILABLE),
          Map.entry("InternalServerError", ErrorCode.UNAVAILABLE),
          Map.entry("SessionStreamingException", ErrorCode.UNAVAILABLE),
          Map.entry("SessionTimeoutException", ErrorCode.TIMEOUT),
          Map.entry("RequestTimeout", ErrorCode.TIMEOUT),
          Map.entry("RequestTimeoutException", ErrorCode.TIMEOUT));

  private static final Pattern AUTH_TEXT =
      Pattern.compile("InvalidClientTokenId|SignatureDoesNotMatch|security token");
  private static final Pattern XML_CODE = Pattern.compile("<Code>([A-Za-z0-9.]+)</Code>");

  private AwsErrors() {}

  /**
   * Returns the AWS error code of an SDK error (without a {@code namespace#} prefix), also when it
   * is wrapped in a {@link CompletionException} or carried as the cause of a mapped SDK exception.
   *
   * @param error any throwable
   * @return the code, or empty
   */
  public static Optional<String> awsErrorCode(Throwable error) {
    Throwable t = unwrap(error);
    for (int depth = 0; t != null && depth < 5; depth++) {
      if (t instanceof AwsServiceException) {
        AwsErrorDetails d = ((AwsServiceException) t).awsErrorDetails();
        String code = d == null ? null : d.errorCode();
        if (code != null && !code.isEmpty()) {
          int hash = code.lastIndexOf('#');
          return Optional.of(hash >= 0 ? code.substring(hash + 1) : code);
        }
        return Optional.empty();
      }
      if (t instanceof AwsCause) {
        return Optional.ofNullable(((AwsCause) t).awsCode);
      }
      t = t.getCause();
    }
    return Optional.empty();
  }

  private static Throwable unwrap(Throwable t) {
    Throwable c = t;
    while ((c instanceof CompletionException || c instanceof ExecutionException)
        && c.getCause() != null) {
      c = c.getCause();
    }
    return c;
  }

  /**
   * Maps any error of an AWS SDK call to a {@link CloudWatchLogsException}. An SDK exception is
   * returned unchanged.
   *
   * @param operation operation name for the message
   * @param error the error
   * @param knownSecrets values to redact from the cause
   * @return the mapped exception
   */
  public static CloudWatchLogsException map(
      String operation, Throwable error, Collection<String> knownSecrets) {
    Throwable t = unwrap(error);
    if (t instanceof CloudWatchLogsException) {
      return (CloudWatchLogsException) t;
    }
    if (t instanceof CancellationException
        || t instanceof InterruptedException
        || t instanceof AbortedException) {
      return new CancelledException(operation + ": cancelled", t);
    }
    String awsCode = awsErrorCode(t).orElse(null);
    Integer status = null;
    String requestId = null;
    String bodyText = "";
    if (t instanceof SdkServiceException) {
      SdkServiceException s = (SdkServiceException) t;
      status = s.statusCode() > 0 ? s.statusCode() : null;
      requestId = s.requestId();
      if (t instanceof AwsServiceException) {
        AwsErrorDetails d = ((AwsServiceException) t).awsErrorDetails();
        if (d != null && d.rawResponse() != null) {
          byte[] raw = d.rawResponse().asByteArray();
          bodyText = new String(raw, 0, Math.min(raw.length, 4096), StandardCharsets.UTF_8);
        }
      }
    }
    if (awsCode != null && !AWS_ERROR_CODES.containsKey(awsCode) && !bodyText.isEmpty()) {
      // Moto answers IAM errors with XML on the JSON protocol: read the <Code> element.
      java.util.regex.Matcher m = XML_CODE.matcher(bodyText);
      if (m.find() && AWS_ERROR_CODES.containsKey(m.group(1))) {
        awsCode = m.group(1);
      }
    } else if (awsCode == null && !bodyText.isEmpty()) {
      java.util.regex.Matcher m = XML_CODE.matcher(bodyText);
      if (m.find()) {
        awsCode = m.group(1);
      }
    }
    String text = (t.getMessage() == null ? "" : t.getMessage()) + " " + bodyText;
    AwsCause cause = new AwsCause(t, awsCode, knownSecrets);
    String suffix = status == null ? "" : " (HTTP " + status + ")";
    if (t instanceof SdkClientException && !(t instanceof SdkServiceException)) {
      String msg = t.getMessage() == null ? "" : t.getMessage();
      if (t instanceof ApiCallTimeoutException || t instanceof ApiCallAttemptTimeoutException) {
        return CloudWatchLogsException.of(
            ErrorCode.TIMEOUT, operation + ": timed out", null, null, null, cause);
      }
      if (msg.contains("Unable to load region")) {
        return CloudWatchLogsException.of(
            ErrorCode.CONFIG,
            operation + ": AWS region is not configured",
            null,
            null,
            null,
            cause);
      }
      if (msg.toLowerCase(java.util.Locale.ROOT).contains("credential")) {
        return CloudWatchLogsException.of(
            ErrorCode.AUTHENTICATION,
            operation + ": no usable AWS credentials",
            null,
            null,
            null,
            cause);
      }
    }
    ErrorCode code = awsCode == null ? null : AWS_ERROR_CODES.get(awsCode);
    boolean named = code != null;
    if (code == null && status != null && status == 403) {
      code =
          AUTH_TEXT.matcher(text).find() ? ErrorCode.AUTHENTICATION : ErrorCode.PERMISSION_DENIED;
    }
    if (code == null && status != null && status >= 400) {
      code = forStatus(status);
    }
    if (code != null) {
      String reason = reason(code) + (named ? ": " + awsCode : "");
      return CloudWatchLogsException.of(
          code, operation + ": " + reason + suffix, status, requestId, null, cause);
    }
    Throwable io = networkCause(t);
    if (io instanceof HttpTimeoutException && !(io instanceof HttpConnectTimeoutException)) {
      return CloudWatchLogsException.of(
          ErrorCode.TIMEOUT, operation + ": timed out", null, null, null, cause);
    }
    if (io != null) {
      return CloudWatchLogsException.of(
          ErrorCode.UNAVAILABLE,
          operation + ": service unavailable: " + io.getClass().getSimpleName(),
          null,
          null,
          null,
          cause);
    }
    return CloudWatchLogsException.of(
        ErrorCode.UNAVAILABLE, operation + ": service unavailable", status, requestId, null, cause);
  }

  private static ErrorCode forStatus(int status) {
    switch (status) {
      case 401:
        return ErrorCode.AUTHENTICATION;
      case 404:
        return ErrorCode.NOT_FOUND;
      case 408:
      case 504:
        return ErrorCode.TIMEOUT;
      case 429:
        return ErrorCode.RATE_LIMITED;
      default:
        return status >= 500 ? ErrorCode.UNAVAILABLE : ErrorCode.VALIDATION;
    }
  }

  private static Throwable networkCause(Throwable t) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < 6; depth++) {
      if (c instanceof HttpTimeoutException
          || c instanceof ConnectException
          || c instanceof UnknownHostException
          || c instanceof IOException) {
        return c;
      }
      c = c.getCause();
    }
    return null;
  }

  /**
   * Human reason for a code ({@code not found}, {@code permission denied}, …).
   *
   * @param code error code
   * @return the reason
   */
  static String reason(ErrorCode code) {
    switch (code) {
      case AUTHENTICATION:
        return "authentication failed";
      case PERMISSION_DENIED:
        return "permission denied";
      case NOT_FOUND:
        return "not found";
      case CONFLICT:
        return "conflict";
      case RATE_LIMITED:
        return "rate limited";
      case VALIDATION:
        return "invalid request";
      case TIMEOUT:
        return "timed out";
      case UNSUPPORTED:
        return "not supported";
      case CONFIG:
        return "invalid configuration";
      case CANCELLED:
        return "cancelled";
      default:
        return "service unavailable";
    }
  }

  /**
   * Redacted copy of an AWS SDK exception kept as the cause of a mapped exception: class name and
   * AWS error code, the message with known secrets removed, and the original stack trace.
   */
  public static final class AwsCause extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** AWS error code, may be null. */
    private final String awsCode;

    AwsCause(Throwable original, String awsCode, Collection<String> knownSecrets) {
      super(
          original.getClass().getSimpleName()
              + (awsCode == null ? "" : " [" + awsCode + "]")
              + ": "
              + Redaction.truncate(
                  Redaction.redactText(
                      String.valueOf(original.getMessage()),
                      knownSecrets == null ? List.of() : knownSecrets),
                  Redaction.MAX_VENDOR_TEXT),
          null,
          false,
          true);
      this.awsCode = awsCode;
      setStackTrace(original.getStackTrace());
    }

    /**
     * Returns the AWS error code.
     *
     * @return the code, or empty
     */
    public Optional<String> awsCode() {
      return Optional.ofNullable(awsCode);
    }
  }
}
