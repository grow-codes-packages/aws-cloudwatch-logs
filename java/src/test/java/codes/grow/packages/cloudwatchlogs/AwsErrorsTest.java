package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;

import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import codes.grow.packages.cloudwatchlogs.core.ErrorCode;
import codes.grow.packages.cloudwatchlogs.core.NotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;

class AwsErrorsTest {
  static AwsServiceException aws(String code, int status, String body) {
    AwsErrorDetails.Builder d = AwsErrorDetails.builder().errorCode(code);
    if (body != null) {
      d.rawResponse(SdkBytes.fromUtf8String(body));
    }
    return AwsServiceException.builder()
        .awsErrorDetails(d.build())
        .statusCode(status)
        .requestId("rid")
        .message("message echoing log data and secret-value-1234")
        .build();
  }

  static CloudWatchLogsException map(Throwable t) {
    return AwsErrors.map("op", t, List.of("secret-value-1234"));
  }

  @Test
  void mapsByAwsCodeFirst() {
    CloudWatchLogsException e = map(aws("ResourceNotFoundException", 400, null));
    assertThat(e).isInstanceOf(NotFoundException.class);
    assertThat(e.getMessage()).isEqualTo("op: not found: ResourceNotFoundException (HTTP 400)");
    assertThat(e.requestId()).contains("rid");
    assertThat(e.httpStatus()).hasValue(400);
    assertThat(e.getCause().getMessage())
        .doesNotContain("secret-value-1234")
        .contains("[REDACTED]");
    assertThat(((AwsErrors.AwsCause) e.getCause()).awsCode()).contains("ResourceNotFoundException");
    assertThat(AwsErrors.awsErrorCode(e)).contains("ResourceNotFoundException");
    String[][] cases = {
      {"ResourceAlreadyExistsException", "CONFLICT"},
      {"OperationAbortedException", "CONFLICT"},
      {"InvalidParameterException", "VALIDATION"},
      {"LimitExceededException", "VALIDATION"},
      {"UnrecognizedClientException", "AUTHENTICATION"},
      {"ExpiredTokenException", "AUTHENTICATION"},
      {"AccessDeniedException", "PERMISSION_DENIED"},
      {"ThrottlingException", "RATE_LIMITED"},
      {"ServiceUnavailableException", "UNAVAILABLE"},
      {"SessionTimeoutException", "TIMEOUT"},
      {"com.amazonaws.logs#ThrottlingException", "RATE_LIMITED"},
    };
    for (String[] c : cases) {
      assertThat(map(aws(c[0], 400, null)).code().name()).as(c[0]).isEqualTo(c[1]);
    }
    for (ErrorCode code : ErrorCode.values()) {
      assertThat(AwsErrors.reason(code)).isNotEmpty();
    }
  }

  @Test
  void statusFallbacksAndMotoXml() {
    assertThat(map(aws(null, 403, "<Code>InvalidClientTokenId</Code>")).code())
        .isEqualTo(ErrorCode.AUTHENTICATION);
    assertThat(map(aws("403", 403, "<Error><Code>AccessDenied</Code></Error>")).code())
        .isEqualTo(ErrorCode.PERMISSION_DENIED);
    assertThat(map(aws("Weird", 403, "<Code>Unknown</Code> security token")).code())
        .isEqualTo(ErrorCode.AUTHENTICATION);
    assertThat(map(aws(null, 403, "nothing")).code()).isEqualTo(ErrorCode.PERMISSION_DENIED);
    assertThat(map(aws(null, 401, null)).code()).isEqualTo(ErrorCode.AUTHENTICATION);
    assertThat(map(aws(null, 404, null)).code()).isEqualTo(ErrorCode.NOT_FOUND);
    assertThat(map(aws(null, 408, null)).code()).isEqualTo(ErrorCode.TIMEOUT);
    assertThat(map(aws(null, 504, null)).code()).isEqualTo(ErrorCode.TIMEOUT);
    assertThat(map(aws(null, 429, null)).code()).isEqualTo(ErrorCode.RATE_LIMITED);
    assertThat(map(aws(null, 500, "<html>")).code()).isEqualTo(ErrorCode.UNAVAILABLE);
    assertThat(map(aws(null, 418, null)).code()).isEqualTo(ErrorCode.VALIDATION);
    assertThat(map(aws(null, 0, null)).code()).isEqualTo(ErrorCode.UNAVAILABLE);
    assertThat(map(aws("", 0, null)).code()).isEqualTo(ErrorCode.UNAVAILABLE);
    AwsServiceException noDetails = AwsServiceException.builder().statusCode(500).build();
    assertThat(map(noDetails).code()).isEqualTo(ErrorCode.UNAVAILABLE);
    assertThat(AwsErrors.awsErrorCode(noDetails)).isEmpty();
  }

  @Test
  void clientSideShapes() {
    assertThat(map(new CompletionException(new CancellationException())).code())
        .isEqualTo(ErrorCode.CANCELLED);
    assertThat(map(new InterruptedException()).code()).isEqualTo(ErrorCode.CANCELLED);
    assertThat(map(AbortedException.builder().message("x").build()).code())
        .isEqualTo(ErrorCode.CANCELLED);
    assertThat(map(ApiCallTimeoutException.builder().message("t").build()).code())
        .isEqualTo(ErrorCode.TIMEOUT);
    assertThat(map(ApiCallAttemptTimeoutException.builder().message("t").build()).code())
        .isEqualTo(ErrorCode.TIMEOUT);
    assertThat(
            map(SdkClientException.create("Unable to load credentials from any of the providers"))
                .code())
        .isEqualTo(ErrorCode.AUTHENTICATION);
    assertThat(map(SdkClientException.create("Unable to load region")).code())
        .isEqualTo(ErrorCode.CONFIG);
    assertThat(
            map(SdkClientException.create(
                    "Unable to execute HTTP request", new ConnectException("x")))
                .getMessage())
        .contains("ConnectException");
    assertThat(
            map(SdkClientException.create("x", new HttpTimeoutException("request timed out")))
                .code())
        .isEqualTo(ErrorCode.TIMEOUT);
    assertThat(
            map(SdkClientException.create("x", new HttpConnectTimeoutException("connect"))).code())
        .isEqualTo(ErrorCode.UNAVAILABLE);
    assertThat(map(new ExecutionException(new IOException("io"))).code())
        .isEqualTo(ErrorCode.UNAVAILABLE);
    assertThat(map(SdkClientException.create(null)).code()).isEqualTo(ErrorCode.UNAVAILABLE);
    assertThat(map(new IllegalStateException()).getMessage()).isEqualTo("op: service unavailable");
    CloudWatchLogsException already = new NotFoundException("x");
    assertThat(map(new CompletionException(already))).isSameAs(already);
    assertThat(AwsErrors.map("op", aws("X", 400, null), null).getCause()).isNotNull();
    assertThat(AwsErrors.awsErrorCode(new RuntimeException(new RuntimeException()))).isEmpty();
  }
}
