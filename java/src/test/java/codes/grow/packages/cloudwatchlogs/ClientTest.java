package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.cloudwatchlogs.core.AuthenticationException;
import codes.grow.packages.cloudwatchlogs.core.CancelledException;
import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import codes.grow.packages.cloudwatchlogs.core.ConfigException;
import codes.grow.packages.cloudwatchlogs.core.ConflictException;
import codes.grow.packages.cloudwatchlogs.core.HealthStatus;
import codes.grow.packages.cloudwatchlogs.core.NotFoundException;
import codes.grow.packages.cloudwatchlogs.core.PermissionDeniedException;
import codes.grow.packages.cloudwatchlogs.core.TimeoutException;
import codes.grow.packages.cloudwatchlogs.core.UnavailableException;
import codes.grow.packages.cloudwatchlogs.core.UnsupportedException;
import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;

class ClientTest {
  static final String G = "cwlv-0badc0de/app";
  static final String ARN = "arn:aws:logs:us-east-1:123456789012:log-group:" + G;

  AwsMock mock;
  CloudWatchLogsClient client;

  @BeforeEach
  void start() throws Exception {
    mock = new AwsMock();
    client = mock.client();
  }

  @AfterEach
  void stop() {
    client.close();
    mock.close();
  }

  static String group(String name, String extra) {
    return "{\"logGroups\":[{\"logGroupName\":\""
        + name
        + "\",\"arn\":\"arn:aws:logs:us-east-1:123456789012:log-group:"
        + name
        + ":*\",\"creationTime\":1795034976104"
        + extra
        + "}]}";
  }

  static <T> List<T> list(Iterable<T> it) {
    List<T> out = new ArrayList<>();
    it.forEach(out::add);
    return out;
  }

  // ---------------------------------------------------------------- core

  @Test
  void healthPingVersion() {
    mock.fixture("ping.ok").fixture("ping.ok");
    HealthStatus h = client.health();
    assertThat(h.healthy()).isTrue();
    assertThat(mock.lastBody("DescribeLogGroups").get("logGroupNamePrefix").asText())
        .isEqualTo("g");
    assertThat(client.ping()).isPositive();
    assertThatThrownBy(client::serverVersion).isInstanceOf(UnsupportedException.class);
    assertThat(CloudWatchLogsClient.API_VERSION).isEqualTo("2014-03-28");
    assertThat(client.region()).isEqualTo("us-east-1");
    assertThat(client.sdk()).isNotNull();
    assertThat(client.config().logGroup()).contains("g");
    assertThat(client.toString()).contains("[REDACTED]").doesNotContain("secret-value-1234");
    String ua = mock.requests("DescribeLogGroups").get(0).header("User-Agent");
    assertThat(ua).contains("grow-codes-aws-cloudwatch-logs/");
  }

  @Test
  void healthClassifiesFailures() throws Exception {
    mock.fixture("ping.auth-unknown-key");
    HealthStatus auth = client.health();
    assertThat(auth.healthy()).isTrue();
    assertThat(auth.details())
        .containsEntry("auth", "failed")
        .containsEntry("code", "AUTHENTICATION");
    mock.on("DescribeLogGroups", AwsMock.responder(AwsMock.load("ensureLogGroup.access-denied")));
    assertThat(client.health().details()).containsEntry("code", "PERMISSION_DENIED");
    mock.error("DescribeLogGroups", "ThrottlingException");
    assertThat(client.health().status()).isEqualTo("degraded");
    mock.on("DescribeLogGroups", 503, "{}");
    HealthStatus down = client.health();
    assertThat(down.healthy()).isFalse();
    assertThat(down.status()).isEqualTo("unhealthy");
    assertThat(down.details()).containsEntry("httpStatus", "503");
    try (CloudWatchLogsClient dead =
        mock.client(mock.config().url("http://127.0.0.1:1"), i -> {})) {
      HealthStatus h = dead.healthAsync().get(10, TimeUnit.SECONDS);
      assertThat(h.status()).isEqualTo("unreachable");
    }
    client.close();
    assertThatThrownBy(client::health).isInstanceOf(CancelledException.class);
  }

  @Test
  void pingAuthErrorsAndClose() {
    mock.fixture("ping.auth-bad-signature");
    assertThatThrownBy(client::ping).isInstanceOf(AuthenticationException.class);
    mock.fixture("ping.auth-unknown-key");
    assertThatThrownBy(client::ping)
        .isInstanceOf(AuthenticationException.class)
        .satisfies(
            e ->
                assertThat(((CloudWatchLogsException) e).requestId())
                    .contains("Ve62wSwPLClnpUTObALUf1FJpyirIVprlZOZDn4F2o2huCbITtKH"));
    client.close();
    client.close();
    assertThat(client.isClosed()).isTrue();
    assertThatThrownBy(client::ping).isInstanceOf(CancelledException.class);
  }

  @Test
  void totalTimeoutAndCancel() throws Exception {
    mock.always(
        "DescribeLogGroups",
        ex -> {
          Thread.sleep(2000);
          AwsMock.send(ex, 200, "{}", "application/json");
        });
    try (CloudWatchLogsClient slow =
        mock.client(mock.config().totalTimeout(Duration.ofMillis(200)), i -> {})) {
      assertThatThrownBy(slow::ping).isInstanceOf(TimeoutException.class);
      CompletableFuture<Duration> f = slow.pingAsync();
      f.cancel(true);
      assertThat(f).isCancelled();
    }
    try (CloudWatchLogsClient attempt =
        mock.client(mock.config().requestTimeout(Duration.ofMillis(200)), i -> {})) {
      assertThatThrownBy(attempt::ping).isInstanceOf(TimeoutException.class);
    }
  }

  @Test
  void retriesThrottlingWithTheSdkStandardMode() {
    mock.error("DescribeLogGroups", "ThrottlingException").fixture("ping.ok");
    try (CloudWatchLogsClient retrying =
        mock.client(
            mock.config()
                .retry(
                    codes.grow.packages.cloudwatchlogs.core.RetryPolicy.builder()
                        .maxRetries(2)
                        .initialBackoff(Duration.ofMillis(1))
                        .maxBackoff(Duration.ofMillis(2))
                        .build()),
            i -> {})) {
      assertThat(retrying.ping()).isPositive();
    }
    assertThat(mock.requests("DescribeLogGroups")).hasSize(2);
  }

  @Test
  void suppliedSdkClient() {
    CloudWatchLogsAsyncClient sdk =
        CloudWatchLogsAsyncClient.builder()
            .region(Region.US_WEST_2)
            .endpointOverride(java.net.URI.create(mock.server.url()))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("a", "b")))
            .httpClient(
                new GrowHttpClient(CloudWatchLogsConfig.builder().build().toTransportConfig()))
            .build();
    CloudWatchLogsClient c =
        CloudWatchLogsClient.create(CloudWatchLogsConfig.builder().sdkClient(sdk).build());
    assertThat(c.region()).isEqualTo("us-west-2");
    assertThat(c.sdk()).isSameAs(sdk);
    mock.fixture("ping.ok");
    assertThat(c.ping()).isPositive();
    c.close();
    mock.fixture("ping.ok");
    assertThat(sdk.describeLogGroups(b -> b.limit(1)).join()).isNotNull();
    sdk.close();
  }

  @Test
  void credentialVariants() {
    mock.fixture("ping.ok").fixture("ping.ok").fixture("ping.ok");
    try (CloudWatchLogsClient session =
        mock.client(mock.config().sessionToken("session-token-1234"), i -> {})) {
      session.ping();
    }
    assertThat(mock.requests("DescribeLogGroups").get(0).header("X-Amz-Security-Token"))
        .isEqualTo("session-token-1234");
    try (CloudWatchLogsClient provider =
        mock.client(
            CloudWatchLogsConfig.builder()
                .url(mock.server.url())
                .region("us-east-1")
                .retry(codes.grow.packages.cloudwatchlogs.core.RetryPolicy.none())
                .credentialsProvider(AnonymousCredentialsProvider.create()),
            i -> {})) {
      provider.ping();
    }
    try (CloudWatchLogsClient chain =
        mock.client(
            CloudWatchLogsConfig.builder()
                .url(mock.server.url())
                .region("us-east-1")
                .profile("grow-sdk-test-profile-that-does-not-exist")
                .retry(codes.grow.packages.cloudwatchlogs.core.RetryPolicy.none()),
            i -> {})) {
      assertThatThrownBy(chain::ping).isInstanceOf(AuthenticationException.class);
    }
    try (CloudWatchLogsClient def =
        mock.client(
            CloudWatchLogsConfig.builder()
                .url(mock.server.url())
                .region("us-east-1")
                .retry(codes.grow.packages.cloudwatchlogs.core.RetryPolicy.none()),
            i -> {})) {
      assertThat(def.region()).isEqualTo("us-east-1");
    }
  }

  @Test
  void assumeRoleIsWired() {
    try (CloudWatchLogsClient role =
        mock.client(mock.config().roleArn("arn:aws:iam::1:role/r").externalId("ext"), i -> {})) {
      // the STS provider is lazy: nothing is called until credentials are needed
      assertThat(role.config().roleArn()).contains("arn:aws:iam::1:role/r");
    }
  }

  @Test
  void regionMustResolve() {
    String old = System.getProperty("aws.region");
    System.setProperty("aws.region", "");
    try {
      CloudWatchLogsConfig cfg =
          CloudWatchLogsConfig.builder().profile("grow-sdk-missing-profile").build();
      // the default chain may still find AWS_REGION in the environment of the test JVM
      if (System.getenv("AWS_REGION") == null && System.getenv("AWS_DEFAULT_REGION") == null) {
        assertThatThrownBy(() -> CloudWatchLogsClient.create(cfg))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("region");
      }
    } finally {
      if (old == null) {
        System.clearProperty("aws.region");
      } else {
        System.setProperty("aws.region", old);
      }
    }
    System.setProperty("aws.region", "eu-west-1");
    try (CloudWatchLogsClient c =
        CloudWatchLogsClient.create(CloudWatchLogsConfig.builder().build())) {
      assertThat(c.region()).isEqualTo("eu-west-1");
    } finally {
      if (old == null) {
        System.clearProperty("aws.region");
      } else {
        System.setProperty("aws.region", old);
      }
    }
  }

  @Test
  void builderAndFromEnv() {
    try (CloudWatchLogsClient c =
        CloudWatchLogsClient.builder()
            .url(mock.server.url())
            .region("us-east-1")
            .credentials("AKID", "secret-1234")
            .logGroup("grp")
            .logStream("str")
            .config(b -> b.maxEventBytes(1000))
            .connectTimeout(Duration.ofSeconds(1))
            .requestTimeout(Duration.ofSeconds(2))
            .totalTimeout(Duration.ofSeconds(3))
            .retry(codes.grow.packages.cloudwatchlogs.core.RetryPolicy.none())
            .tls(codes.grow.packages.cloudwatchlogs.core.TlsOptions.defaults())
            .proxy("http://proxy.local:3128")
            .userAgentSuffix("app/1")
            .hooks(codes.grow.packages.cloudwatchlogs.core.Hooks.NONE)
            .logger(codes.grow.packages.cloudwatchlogs.core.Logger.NOOP)
            .header("X-A", "1")
            .fromEnv(codes.grow.packages.cloudwatchlogs.core.EnvReader.of(Map.of()))
            .build()) {
      assertThat(c.config().logGroup()).contains("grp");
      assertThat(c.config().logStream()).isEqualTo("str");
      assertThat(c.config().maxEventBytes()).isEqualTo(1000);
    }
    if (System.getenv("AWS_REGION") != null || System.getProperty("aws.region") != null) {
      CloudWatchLogsClient.fromEnv().close();
    }
  }

  // ---------------------------------------------------------------- bootstrap

  @Test
  void ensureLogGroupCreates() {
    mock.fixture("ensureLogGroup.create")
        .on("DescribeLogGroups", 200, group(G, ""))
        .fixture("putRetentionPolicy.ok");
    EnsureLogGroupResult r =
        client.ensureLogGroup(
            G,
            EnsureLogGroupOptions.builder()
                .retentionDays(7)
                .tags(Map.of("team", "sdk"))
                .kmsKeyId("k1")
                .build());
    assertThat(r.created()).isTrue();
    assertThat(r.arn()).isEqualTo(ARN);
    JsonNode create = mock.lastBody("CreateLogGroup");
    assertThat(create.get("tags").get("team").asText()).isEqualTo("sdk");
    assertThat(create.get("kmsKeyId").asText()).isEqualTo("k1");
    assertThat(mock.lastBody("PutRetentionPolicy").get("retentionInDays").asInt()).isEqualTo(7);
  }

  @Test
  void ensureLogGroupExisting() {
    String kms = ",\"kmsKeyId\":\"arn:aws:kms:us-east-1:1:key/k1\"";
    mock.fixture("ensureLogGroup.exists")
        .on("DescribeLogGroups", 200, group(G, kms))
        .on("DescribeLogGroups", 200, group(G, kms))
        .fixture("ensureLogGroup.tag-reconcile")
        .fixture("putRetentionPolicy.delete");
    EnsureLogGroupResult r =
        client.ensureLogGroup(
            G,
            EnsureLogGroupOptions.builder()
                .tags(Map.of("env", "it"))
                .kmsKeyId("k1")
                .retentionDays(0)
                .build());
    assertThat(r.created()).isFalse();
    assertThat(mock.lastBody("TagResource").get("resourceArn").asText()).isEqualTo(ARN);
    assertThat(mock.requests("DeleteRetentionPolicy")).hasSize(1);
    // a different key is a conflict
    mock.fixture("ensureLogGroup.exists").on("DescribeLogGroups", 200, group(G, kms));
    assertThatThrownBy(
            () -> client.ensureLogGroup(G, EnsureLogGroupOptions.builder().kmsKeyId("k2").build()))
        .isInstanceOf(ConflictException.class);
    // the full ARN matches too, and no tags means no TagResource
    mock.fixture("ensureLogGroup.exists").on("DescribeLogGroups", 200, group(G, kms));
    assertThat(
            client
                .ensureLogGroup(
                    G,
                    EnsureLogGroupOptions.builder()
                        .kmsKeyId("arn:aws:kms:us-east-1:1:key/k1")
                        .build())
                .created())
        .isFalse();
    mock.fixture("ensureLogGroup.exists").on("DescribeLogGroups", 200, group(G, ""));
    assertThat(client.ensureLogGroup(G).created()).isFalse();
  }

  @Test
  void ensureLogGroupErrors() {
    assertThatThrownBy(() -> client.ensureLogGroup("bad name!"))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.ensureLogGroup(G, EnsureLogGroupOptions.builder().retentionDays(8).build()))
        .isInstanceOf(ValidationException.class);
    mock.fixture("ensureLogGroup.access-denied");
    assertThatThrownBy(() -> client.ensureLogGroup(G))
        .isInstanceOf(PermissionDeniedException.class)
        .hasMessageContaining("AccessDenied");
    mock.fixture("ensureLogGroup.create").on("DescribeLogGroups", 200, "{\"logGroups\":[]}");
    assertThatThrownBy(() -> client.ensureLogGroupAsync(G, null).join())
        .hasCauseInstanceOf(NotFoundException.class);
  }

  @Test
  void getLogGroupPagesUntilExactMatch() {
    mock.on(
            "DescribeLogGroups",
            200,
            "{\"logGroups\":[{\"logGroupName\":\"" + G + "x\"}],\"nextToken\":\"t1\"}")
        .on(
            "DescribeLogGroups",
            200,
            "{\"logGroups\":[{\"logGroupName\":\""
                + G
                + "\",\"logGroupArn\":\""
                + ARN
                + "\",\"retentionInDays\":7,\"storedBytes\":5,\"metricFilterCount\":1,"
                + "\"logGroupClass\":\"STANDARD\",\"kmsKeyId\":\"k\"}]}");
    LogGroupInfo g = client.getLogGroup(G).orElseThrow();
    assertThat(g.arn()).isEqualTo(ARN);
    assertThat(g.retentionDays()).hasValue(7);
    assertThat(g.storedBytes()).hasValue(5);
    assertThat(g.metricFilterCount()).hasValue(1);
    assertThat(g.logGroupClass()).contains("STANDARD");
    assertThat(g.kmsKeyId()).contains("k");
    assertThat(g.creationTime()).isEmpty();
    assertThat(mock.lastBody("DescribeLogGroups").get("nextToken").asText()).isEqualTo("t1");
    mock.on("DescribeLogGroups", 200, "{\"logGroups\":[{}],\"nextToken\":\"\"}");
    assertThat(client.getLogGroup(G)).isEmpty();
    assertThatThrownBy(() -> client.getLogGroup("")).isInstanceOf(ValidationException.class);
  }

  @Test
  void ensureLogStream() {
    mock.fixture("ensureLogStream.create").fixture("ensureLogStream.exists");
    assertThat(client.ensureLogStream(G, "s1").created()).isTrue();
    assertThat(client.ensureLogStream(G, "s1").created()).isFalse();
    mock.fixture("ensureLogStream.group-not-found");
    assertThatThrownBy(() -> client.ensureLogStream(G, "s1")).isInstanceOf(NotFoundException.class);
    mock.fixture("ensureLogGroup.create")
        .on("DescribeLogGroups", 200, group(G, ""))
        .fixture("ensureLogStream.create");
    assertThat(client.ensureLogStream(G, "s1", true).created()).isTrue();
    assertThatThrownBy(() -> client.ensureLogStream(G, "a:b"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void retentionAndDeletes() {
    mock.fixture("putRetentionPolicy.ok").fixture("putRetentionPolicy.delete");
    client.putRetentionPolicy(G, 30);
    client.putRetentionPolicy(G, 0);
    assertThatThrownBy(() -> client.putRetentionPolicy(G, 2))
        .isInstanceOf(ValidationException.class);
    mock.fixture("putRetentionPolicy.not-found");
    assertThatThrownBy(() -> client.putRetentionPolicy(G, 7)).isInstanceOf(NotFoundException.class);
    mock.fixture("deleteLogGroup.ok")
        .fixture("deleteLogGroup.not-found")
        .fixture("deleteLogGroup.not-found");
    assertThat(client.deleteLogGroup(G)).isTrue();
    assertThat(client.deleteLogGroup(G, true)).isFalse();
    assertThatThrownBy(() -> client.deleteLogGroup(G)).isInstanceOf(NotFoundException.class);
    mock.fixture("deleteLogGroup.access-denied");
    assertThatThrownBy(() -> client.deleteLogGroup(G, true))
        .isInstanceOf(PermissionDeniedException.class);
    mock.fixture("deleteLogStream.ok")
        .fixture("deleteLogStream.not-found")
        .fixture("deleteLogStream.not-found");
    assertThat(client.deleteLogStream(G, "s1")).isTrue();
    assertThat(client.deleteLogStreamAsync(G, "s1", true).join()).isFalse();
    assertThatThrownBy(() -> client.deleteLogStream(G, "s1")).isInstanceOf(NotFoundException.class);
  }

  @Test
  void describeLogGroupsPaginates() {
    mock.fixture("describeLogGroups.page1").fixture("describeLogGroups.page2");
    List<LogGroupInfo> all =
        list(
            client.describeLogGroups(
                DescribeLogGroupsOptions.builder().prefix("cwlv-0badc0de/").pageSize(1).build()));
    assertThat(all).extracting(LogGroupInfo::name).containsExactly(G, "cwlv-0badc0de/other");
    assertThat(all.get(0).arn()).isEqualTo(ARN);
    assertThat(all.get(1).kmsKeyId()).isPresent();
    assertThat(mock.lastBody("DescribeLogGroups").get("nextToken")).isNotNull();
    mock.fixture("describeLogGroups.page1");
    assertThat(
            client
                .describeLogGroupsAsync(DescribeLogGroupsOptions.builder().limit(1).build())
                .join())
        .hasSize(1);
    mock.fixture("describeLogGroups.page1").fixture("describeLogGroups.page2");
    assertThat(client.describeLogGroupsAsync(null).join()).hasSize(2);
    mock.fixture("describeLogGroups.empty");
    Iterator<LogGroupInfo> it = client.describeLogGroups(null).iterator();
    assertThat(it.hasNext()).isFalse();
    assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    assertThatThrownBy(
            () -> client.describeLogGroups(DescribeLogGroupsOptions.builder().pageSize(51).build()))
        .isInstanceOf(ValidationException.class);
    mock.fixture("describeLogGroups.limit-too-big");
    assertThatThrownBy(() -> list(client.describeLogGroups(null)))
        .isInstanceOf(ValidationException.class);
    mock.fixture("describeLogGroups.page1");
    assertThat(
            CloudWatchLogsClient.stream(
                    client.describeLogGroups(DescribeLogGroupsOptions.builder().limit(1).build()))
                .count())
        .isEqualTo(1);
  }

  @Test
  void describeLogStreamsPaginates() {
    mock.fixture("describeLogStreams.page1")
        .fixture("describeLogStreams.page2")
        .on("DescribeLogStreams", 200, "{\"logStreams\":[]}");
    List<LogStreamInfo> all =
        list(client.describeLogStreams(G, DescribeLogStreamsOptions.builder().pageSize(2).build()));
    assertThat(all).hasSizeGreaterThanOrEqualTo(3);
    assertThat(all.get(0).name()).isEqualTo("big");
    assertThat(all.get(0).arn()).isPresent();
    assertThat(all.get(0).firstEventTime()).isPresent();
    assertThat(all.get(0).lastEventTime()).isPresent();
    assertThat(all.get(0).lastIngestionTime()).isPresent();
    assertThat(all.get(0).creationTime()).isPresent();
    mock.fixture("describeLogStreams.last-event-desc");
    List<LogStreamInfo> desc =
        client
            .describeLogStreamsAsync(
                G,
                DescribeLogStreamsOptions.builder()
                    .orderBy(LogStreamOrder.LAST_EVENT_TIME)
                    .descending(true)
                    .limit(1)
                    .build())
            .join();
    assertThat(desc).hasSize(1);
    JsonNode body = mock.lastBody("DescribeLogStreams");
    assertThat(body.get("orderBy").asText()).isEqualTo("LastEventTime");
    assertThat(body.get("descending").asBoolean()).isTrue();
    mock.fixture("describeLogStreams.prefix");
    assertThat(
            list(
                client.describeLogStreams(
                    G, DescribeLogStreamsOptions.builder().prefix("s").build())))
        .isNotEmpty();
    assertThat(mock.lastBody("DescribeLogStreams").get("logStreamNamePrefix").asText())
        .isEqualTo("s");
    assertThatThrownBy(
            () ->
                client.describeLogStreams(
                    G,
                    DescribeLogStreamsOptions.builder()
                        .prefix("s")
                        .orderBy(LogStreamOrder.LAST_EVENT_TIME)
                        .build()))
        .isInstanceOf(ValidationException.class);
    mock.fixture("describeLogStreams.not-found");
    assertThatThrownBy(() -> list(client.describeLogStreams(G)))
        .isInstanceOf(NotFoundException.class);
    mock.on("DescribeLogStreams", 200, "{\"logStreams\":[{}]}");
    assertThat(list(client.describeLogStreams(G)).get(0).name()).isEmpty();
  }

  @Test
  void tags() {
    mock.on("DescribeLogGroups", 200, group(G, ""))
        .fixture("ensureLogGroup.tag-reconcile")
        .fixture("ensureLogGroup.list-tags");
    client.tagLogGroup(G, Map.of("a", "1"));
    assertThat(mock.lastBody("TagResource").get("tags").get("a").asText()).isEqualTo("1");
    assertThat(client.listLogGroupTags(ARN + ":*")).containsEntry("team", "sdk");
    assertThat(mock.lastBody("ListTagsForResource").get("resourceArn").asText()).isEqualTo(ARN);
    mock.on("UntagResource", 200, "{}");
    client.untagLogGroup(ARN, List.of("team"));
    assertThat(mock.lastBody("UntagResource").get("tagKeys").get(0).asText()).isEqualTo("team");
    mock.on("DescribeLogGroups", 200, "{\"logGroups\":[]}");
    assertThatThrownBy(() -> client.listLogGroupTags(G)).isInstanceOf(NotFoundException.class);
    assertThatThrownBy(() -> client.tagLogGroup("bad name", Map.of()))
        .isInstanceOf(ValidationException.class);
  }

  // ---------------------------------------------------------------- ingest

  @Test
  void putLogEventsSortsAndCounts() {
    mock.fixture("putLogEvents.ok");
    Instant t = Instant.ofEpochMilli(1795034916000L);
    PutLogEventsResult r =
        client.putLogEvents(
            G,
            "s1",
            List.of(
                LogEvent.of(t.plusSeconds(2), "c"),
                LogEvent.of(t, "a"),
                LogEvent.of(t.plusSeconds(1), "b")));
    assertThat(r.accepted()).isEqualTo(3);
    assertThat(r.batches()).isEqualTo(1);
    assertThat(r.rejectedInfo()).isEmpty();
    JsonNode events = mock.lastBody("PutLogEvents").get("logEvents");
    assertThat(events.get(0).get("message").asText()).isEqualTo("a");
    assertThat(events.get(2).get("message").asText()).isEqualTo("c");
    assertThat(mock.lastBody("PutLogEvents").has("sequenceToken")).isFalse();
  }

  @Test
  void putLogEventsReportsRejected() {
    // real AWS: [old, ok, new] -> tooOldLogEventEndIndex 1 (exclusive), tooNewLogEventStartIndex 2
    mock.fixture("putLogEvents.rejected-aws");
    Instant t = Instant.now();
    PutLogEventsResult r =
        client.putLogEvents(
            G,
            "s3",
            List.of(
                LogEvent.of(t.minusSeconds(1000), "old"),
                LogEvent.of(t, "ok"),
                LogEvent.of(t.plusSeconds(1000), "new")));
    assertThat(r.accepted()).isEqualTo(1);
    assertThat(r.rejected()).isEqualTo(2);
    assertThat(r.rejectedInfo().get(0).tooOldLogEventEndIndex()).hasValue(1);
    assertThat(r.rejectedInfo().get(0).tooNewLogEventStartIndex()).hasValue(2);
    assertThat(r.rejectedInfo().get(0).expiredLogEventEndIndex()).isEmpty();
    mock.on("PutLogEvents", 200, "{\"rejectedLogEventsInfo\":{\"expiredLogEventEndIndex\":1}}");
    assertThat(
            client
                .putLogEvents(
                    G, "s3", List.of(LogEvent.of(t, "a"), LogEvent.of(t, "b"), LogEvent.of(t, "c")))
                .rejected())
        .isEqualTo(1);
    // Moto 5.2.4 returns the end index one lower: read with the AWS rule
    mock.fixture("putLogEvents.rejected-too-old-too-new");
    PutLogEventsResult moto =
        client.putLogEvents(
            G,
            "s3",
            List.of(
                LogEvent.of(t.minusSeconds(1000), "old"),
                LogEvent.of(t, "ok"),
                LogEvent.of(t.plusSeconds(1000), "new")));
    assertThat(moto.accepted()).isEqualTo(2);
    assertThat(moto.rejected()).isEqualTo(1);
  }

  @Test
  void putLogEventsAutoCreatesTheStream() {
    mock.fixture("putLogEvents.stream-not-found")
        .fixture("ensureLogStream.create")
        .fixture("putLogEvents.ok");
    assertThat(client.putLogEvents(G, "missing", List.of(LogEvent.of("x"))).accepted())
        .isEqualTo(1);
    // missing group without createGroup: the original NOT_FOUND is raised
    mock.fixture("putLogEvents.group-not-found").fixture("ensureLogStream.group-not-found");
    assertThatThrownBy(() -> client.putLogEvents(G, "s", List.of(LogEvent.of("x"))))
        .isInstanceOf(NotFoundException.class);
    // with createGroup the group is created too
    mock.fixture("putLogEvents.group-not-found")
        .fixture("ensureLogGroup.create")
        .on("DescribeLogGroups", 200, group(G, ""))
        .fixture("ensureLogStream.create")
        .fixture("putLogEvents.ok");
    assertThat(
            client
                .putLogEvents(
                    G,
                    "s",
                    List.of(LogEvent.of("x")),
                    PutLogEventsOptions.builder().createGroup(true).build())
                .accepted())
        .isEqualTo(1);
    // auto-create off
    mock.fixture("putLogEvents.stream-not-found");
    assertThatThrownBy(
            () ->
                client.putLogEvents(
                    G,
                    "s",
                    List.of(LogEvent.of("x")),
                    PutLogEventsOptions.builder().autoCreate(false).build()))
        .isInstanceOf(NotFoundException.class);
    // stream creation denied: that error wins
    mock.fixture("putLogEvents.stream-not-found").fixture("ensureLogGroup.access-denied");
    mock.on("CreateLogStream", AwsMock.responder(AwsMock.load("ensureLogGroup.access-denied")));
    assertThatThrownBy(() -> client.putLogEvents(G, "s", List.of(LogEvent.of("x"))))
        .isInstanceOf(PermissionDeniedException.class);
    mock.fixture("putLogEvents.access-denied");
    assertThatThrownBy(
            () -> client.putLogEventsAsync(G, "s", List.of(LogEvent.of("x")), null).join())
        .hasCauseInstanceOf(PermissionDeniedException.class);
  }

  @Test
  void putLogEventsSplitsAndAppliesThePolicy() {
    mock.always("PutLogEvents", "{}");
    Instant t = Instant.now();
    List<LogEvent> many = new ArrayList<>();
    for (int i = 0; i < 10_001; i++) {
      many.add(LogEvent.of(t, "m"));
    }
    PutLogEventsResult r = client.putLogEvents(G, "s", many);
    assertThat(r.batches()).isEqualTo(2);
    assertThat(r.accepted()).isEqualTo(10_001);
    String big = "x".repeat(300_000);
    r = client.putLogEvents(G, "s", List.of(LogEvent.of(t, big)));
    assertThat(r.truncated()).isEqualTo(1);
    r =
        client.putLogEvents(
            G,
            "s",
            List.of(LogEvent.of(t, big), LogEvent.of(t, "ok")),
            PutLogEventsOptions.builder()
                .oversizePolicy(OversizePolicy.DROP)
                .maxEventBytes(1000)
                .build());
    assertThat(r.dropped()).isEqualTo(1);
    assertThat(r.accepted()).isEqualTo(1);
    assertThatThrownBy(
            () ->
                client.putLogEvents(
                    G,
                    "s",
                    List.of(LogEvent.of(t, big)),
                    PutLogEventsOptions.builder().oversizePolicy(OversizePolicy.ERROR).build()))
        .isInstanceOf(ValidationException.class);
    r = client.putLogEvents(G, "s", List.of());
    assertThat(r.batches()).isZero();
    assertThatThrownBy(() -> client.putLogEvents("bad name", "s", List.of()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> client.putLogEvents(G, "s*", List.of()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void emfSendsTheDocument() throws Exception {
    mock.fixture("emf.put");
    Map<String, Object> doc =
        client.emf(
            "latency",
            42,
            Map.of("service", "api"),
            Emf.Options.defaults().withUnit("Milliseconds").withTarget(null, "emf"));
    assertThat(doc).containsEntry("latency", 42.0).containsEntry("service", "api");
    JsonNode sent = mock.lastBody("PutLogEvents");
    assertThat(sent.get("logGroupName").asText()).isEqualTo("g");
    assertThat(sent.get("logStreamName").asText()).isEqualTo("emf");
    JsonNode message = AwsMock.JSON.readTree(sent.get("logEvents").get(0).get("message").asText());
    assertThat(message.get("_aws").get("CloudWatchMetrics").get(0).get("Namespace").asText())
        .isEqualTo("GrowSdk");
    mock.on("PutLogEvents", 200, "{\"rejectedLogEventsInfo\":{\"tooOldLogEventEndIndex\":1}}");
    assertThatThrownBy(() -> client.emf("m", 1, null, null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("rejected");
    try (CloudWatchLogsClient noGroup = mock.client(mock.config().logGroup(null), i -> {})) {
      assertThatThrownBy(() -> noGroup.emf("m", 1, Map.of(), null))
          .isInstanceOf(ConfigException.class);
    }
  }

  // ---------------------------------------------------------------- query

  @Test
  void filterLogEventsPages() {
    mock.fixture("filterLogEvents.page1").fixture("filterLogEvents.page2");
    FilterLogEventsOptions o =
        FilterLogEventsOptions.builder().group(G).streams(List.of("s1")).pageSize(2).build();
    List<FilteredLogEvent> all = list(client.filterLogEvents(o));
    assertThat(all).hasSize(3);
    assertThat(all.get(0).eventId()).contains("3");
    assertThat(all.get(0).logStreamName()).contains("s1");
    assertThat(all.get(0).ingestionTime()).isPresent();
    mock.fixture("filterLogEvents.page1");
    FilterLogEventsPage p = client.filterLogEventsPage(o, null);
    assertThat(p.nextToken()).contains("cwlv-0badc0de/app@s1@4");
    mock.fixture("filterLogEvents.page1");
    assertThat(
            client
                .filterLogEventsAsync(
                    FilterLogEventsOptions.builder()
                        .group(ARN + ":*")
                        .pattern("ERROR")
                        .start(Instant.ofEpochMilli(1))
                        .end(Instant.ofEpochMilli(2))
                        .limit(1)
                        .build())
                .join())
        .hasSize(1);
    JsonNode body = mock.lastBody("FilterLogEvents");
    assertThat(body.get("logGroupIdentifier").asText()).isEqualTo(ARN);
    assertThat(body.get("filterPattern").asText()).isEqualTo("ERROR");
    assertThat(body.get("limit").asInt()).isEqualTo(1);
    assertThat(body.get("startTime").asLong()).isEqualTo(1);
    assertThat(body.get("endTime").asLong()).isEqualTo(2);
    mock.on(
        "FilterLogEvents",
        200,
        "{\"events\":[{\"eventId\":\"2\",\"timestamp\":5},{\"eventId\":\"1\",\"timestamp\":5},{}]}");
    List<FilteredLogEvent> sorted =
        client
            .filterLogEventsPageAsync(
                FilterLogEventsOptions.builder().streamPrefix("s").pattern("").build(), null)
            .join()
            .events();
    assertThat(sorted).extracting(e -> e.eventId().orElse("-")).containsExactly("-", "1", "2");
    assertThat(mock.lastBody("FilterLogEvents").get("logGroupName").asText()).isEqualTo("g");
    assertThat(mock.lastBody("FilterLogEvents").get("logStreamNamePrefix").asText()).isEqualTo("s");
    assertThatThrownBy(
            () ->
                client.filterLogEventsPage(
                    FilterLogEventsOptions.builder()
                        .streams(List.of("a"))
                        .streamPrefix("b")
                        .build(),
                    null))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.filterLogEventsPage(
                    FilterLogEventsOptions.builder().streams(List.of()).build(), null))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.filterLogEventsPage(
                    FilterLogEventsOptions.builder().pageSize(10_001).build(), null))
        .isInstanceOf(ValidationException.class);
    mock.fixture("filterLogEvents.not-found");
    assertThatThrownBy(() -> list(client.filterLogEvents(null)))
        .isInstanceOf(NotFoundException.class);
  }

  @Test
  void getLogEventsForwardAndBackward() {
    mock.fixture("getLogEvents.head")
        .fixture("getLogEvents.forward")
        .fixture("getLogEvents.end-of-stream");
    List<StreamLogEvent> head =
        list(
            client.getLogEvents(
                G,
                "s1",
                GetLogEventsOptions.builder()
                    .startFromHead(true)
                    .pageSize(2)
                    .start(Instant.ofEpochMilli(1))
                    .end(Instant.ofEpochMilli(9_999_999_999_999L))
                    .build()));
    assertThat(head).hasSize(3);
    assertThat(head.get(2).message()).isEqualTo("plain info line");
    assertThat(head.get(0).ingestionTime()).isPresent();
    JsonNode body = mock.lastBody("GetLogEvents");
    assertThat(body.get("startFromHead").asBoolean()).isTrue();
    assertThat(body.get("startTime").asLong()).isEqualTo(1);
    mock.fixture("getLogEvents.tail")
        .on(
            "GetLogEvents",
            200,
            "{\"events\":[],\"nextBackwardToken\":\"b/x\",\"nextForwardToken\":\"f/x\"}");
    List<StreamLogEvent> tail =
        client.getLogEventsAsync(G, "s1", GetLogEventsOptions.builder().pageSize(1).build()).join();
    assertThat(tail).hasSize(1);
    mock.fixture("getLogEvents.tail");
    assertThat(list(client.getLogEvents(G, "s1", GetLogEventsOptions.builder().limit(1).build())))
        .hasSize(1);
    mock.on("GetLogEvents", 200, "{\"events\":[{}]}");
    assertThat(list(client.getLogEvents(G, "s1", null)).get(0).message()).isEmpty();
    mock.fixture("getLogEvents.stream-not-found");
    assertThatThrownBy(() -> list(client.getLogEvents(G, "s1", null)))
        .isInstanceOf(NotFoundException.class);
    assertThatThrownBy(
            () -> client.getLogEvents(G, "s1", GetLogEventsOptions.builder().pageSize(0).build()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void insightsQueries() {
    mock.fixture("startQuery.ok").fixture("getQueryResults.complete");
    QueryOptions range =
        QueryOptions.builder()
            .groups(List.of(G))
            .start(Instant.ofEpochMilli(1_795_034_914_500L))
            .end(Instant.ofEpochMilli(1_795_034_924_100L))
            .limit(3)
            .build();
    String id = client.startQuery("fields @message", range);
    assertThat(id).isEqualTo("9e4e6e71-0edf-1bf2-b267-9c7c4ffdc4e4");
    JsonNode body = mock.lastBody("StartQuery");
    assertThat(body.get("startTime").asLong()).isEqualTo(1_795_034_914L);
    assertThat(body.get("endTime").asLong()).isEqualTo(1_795_034_925L);
    assertThat(body.get("logGroupNames").get(0).asText()).isEqualTo(G);
    assertThat(body.get("limit").asInt()).isEqualTo(3);
    QueryResults res = client.getQueryResults(id);
    assertThat(res.status()).isEqualTo(QueryStatus.COMPLETE);
    assertThat(res.rows().get(0)).containsKey("@message");
    assertThat(res.statistics()).isEmpty();
    mock.on(
        "GetQueryResults",
        200,
        "{\"status\":\"Running\",\"results\":[[{\"field\":\"@m\"},{\"value\":\"x\"}]],"
            + "\"statistics\":{\"recordsMatched\":1,\"recordsScanned\":2}}");
    QueryResults running = client.getQueryResultsAsync(id).join();
    assertThat(running.status()).isEqualTo(QueryStatus.RUNNING);
    assertThat(running.rows().get(0)).containsEntry("@m", "");
    assertThat(running.statistics().orElseThrow().recordsScanned()).isEqualTo(2);
    assertThat(running.statistics().orElseThrow().bytesScanned()).isZero();
    mock.fixture("startQuery.ok");
    client
        .startQueryAsync(
            "fields @message", QueryOptions.builder().groups(List.of(ARN + ":*")).build())
        .join();
    assertThat(mock.lastBody("StartQuery").get("logGroupIdentifiers").get(0).asText())
        .isEqualTo(ARN);
    mock.fixture("startQuery.ok");
    client.startQuery("fields @message", null);
    assertThat(mock.lastBody("StartQuery").get("logGroupNames").get(0).asText()).isEqualTo("g");
    assertThatThrownBy(() -> client.startQuery(" ", null)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () -> client.startQuery("f", QueryOptions.builder().groups(List.of()).build()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.startQuery(
                    "f",
                    QueryOptions.builder()
                        .start(Instant.ofEpochSecond(10))
                        .end(Instant.ofEpochSecond(5))
                        .build()))
        .isInstanceOf(ValidationException.class);
    mock.on("StartQuery", 200, "{}");
    assertThatThrownBy(() -> client.startQuery("f", null)).isInstanceOf(UnavailableException.class);
    assertThatThrownBy(() -> client.getQueryResults("")).isInstanceOf(ValidationException.class);
    mock.fixture("getQueryResults.unknown-id-moto-500");
    assertThatThrownBy(() -> client.getQueryResults("x")).isInstanceOf(UnavailableException.class);
  }

  @Test
  void stopAndDescribeQueries() {
    mock.on("StopQuery", 200, "{\"success\":true}").on("StopQuery", 200, "{\"success\":false}");
    assertThat(client.stopQuery("q")).isTrue();
    assertThat(client.stopQueryAsync("q").join()).isFalse();
    mock.fixture("stopQuery.moto-500");
    assertThatThrownBy(() -> client.stopQuery("q")).isInstanceOf(UnavailableException.class);
    assertThatThrownBy(() -> client.stopQuery(null)).isInstanceOf(ValidationException.class);
    mock.fixture("describeQueries.ok");
    List<QueryInfo> qs =
        client.describeQueries(
            DescribeQueriesOptions.builder()
                .group(G)
                .status(QueryStatus.COMPLETE)
                .maxResults(10)
                .build());
    assertThat(qs).isNotEmpty();
    assertThat(qs.get(0).createTime()).isPresent();
    assertThat(qs.get(0).logGroupName()).contains(G);
    assertThat(qs.get(0).status()).isEqualTo(QueryStatus.COMPLETE);
    JsonNode body = mock.lastBody("DescribeQueries");
    assertThat(body.get("status").asText()).isEqualTo("Complete");
    mock.on("DescribeQueries", 200, "{\"queries\":[{}]}");
    QueryInfo empty = client.describeQueriesAsync(null).join().get(0);
    assertThat(empty.queryId()).isEmpty();
    assertThat(empty.createTime()).isEmpty();
    assertThatThrownBy(
            () -> client.describeQueries(DescribeQueriesOptions.builder().maxResults(0).build()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void queryPollsToCompletion() {
    mock.fixture("startQuery.ok")
        .on("GetQueryResults", 200, "{\"status\":\"Scheduled\"}")
        .on("GetQueryResults", 200, "{\"status\":\"Running\"}")
        .fixture("getQueryResults.complete");
    QueryResults r =
        client.query(
            "fields @message",
            QueryOptions.builder().groups(List.of(G)).pollInterval(Duration.ofSeconds(4)).build());
    assertThat(r.status()).isEqualTo(QueryStatus.COMPLETE);
    assertThat(mock.requests("GetQueryResults")).hasSize(3);
  }

  @Test
  void queryTerminalFailures() {
    for (String[] c :
        new String[][] {
          {"Failed", "UNAVAILABLE"},
          {"Unknown", "UNAVAILABLE"},
          {"Timeout", "TIMEOUT"},
          {"Cancelled", "CANCELLED"}
        }) {
      mock.fixture("startQuery.ok").on("GetQueryResults", 200, "{\"status\":\"" + c[0] + "\"}");
      assertThatThrownBy(() -> client.query("f", null))
          .isInstanceOf(CloudWatchLogsException.class)
          .hasMessageContaining("9e4e6e71")
          .satisfies(e -> assertThat(((CloudWatchLogsException) e).code().name()).isEqualTo(c[1]));
    }
  }

  @Test
  void queryDeadlineStopsTheQuery() {
    mock.fixture("startQuery.ok")
        .always("GetQueryResults", "{\"status\":\"Running\"}")
        .always("StopQuery", "{\"success\":true}");
    assertThatThrownBy(
            () ->
                client.query(
                    "f",
                    QueryOptions.builder()
                        .timeout(Duration.ofMillis(50))
                        .pollInterval(Duration.ofMillis(10))
                        .build()))
        .isInstanceOf(TimeoutException.class);
    // with a real sleeper the remaining time caps the wait
    try (CloudWatchLogsClient real =
        mock.client(mock.config(), i -> i.sleeper = CloudWatchLogsClient::delay)) {
      mock.fixture("startQuery.ok");
      assertThatThrownBy(
              () ->
                  real.query(
                      "f",
                      QueryOptions.builder()
                          .timeout(Duration.ofMillis(300))
                          .pollInterval(Duration.ofMillis(100))
                          .build()))
          .isInstanceOf(TimeoutException.class);
      mock.fixture("startQuery.ok");
      CompletableFuture<QueryResults> f =
          real.queryAsync("f", QueryOptions.builder().pollInterval(Duration.ofSeconds(1)).build());
      sleep(300);
      f.cancel(true);
      sleep(1500);
    }
    assertThat(mock.requests("StopQuery")).isNotEmpty();
  }

  @Test
  void queryRetriesTheConcurrencyLimit() {
    mock.error("StartQuery", "LimitExceededException")
        .fixture("startQuery.ok")
        .fixture("getQueryResults.complete");
    assertThat(client.query("f", null).status()).isEqualTo(QueryStatus.COMPLETE);
    mock.error("StartQuery", "LimitExceededException")
        .error("StartQuery", "LimitExceededException");
    assertThatThrownBy(() -> client.query("f", QueryOptions.builder().retryOnQueryLimit(2).build()))
        .isInstanceOf(ValidationException.class);
    mock.error("StartQuery", "MalformedQueryException");
    assertThatThrownBy(() -> client.query("f", null)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> client.queryAsync("", null).join())
        .hasCauseInstanceOf(ValidationException.class);
  }

  @Test
  void helpers() {
    assertThat(CloudWatchLogsClient.parseInsightsTimestamp("1795034915000"))
        .contains(Instant.ofEpochMilli(1795034915000L));
    assertThat(CloudWatchLogsClient.parseInsightsTimestamp("2026-11-18 10:00:00.123"))
        .contains(Instant.parse("2026-11-18T10:00:00.123Z"));
    assertThat(CloudWatchLogsClient.parseInsightsTimestamp("2026-11-18T10:00:00Z"))
        .contains(Instant.parse("2026-11-18T10:00:00Z"));
    assertThat(CloudWatchLogsClient.parseInsightsTimestamp("2026-13-18 10:00:00")).isEmpty();
    assertThat(CloudWatchLogsClient.parseInsightsTimestamp("nope")).isEmpty();
    assertThat(CloudWatchLogsClient.parseInsightsTimestamp(null)).isEmpty();
    assertThat(CloudWatchLogsClient.partitionForRegion("cn-north-1")).isEqualTo("aws-cn");
    assertThat(CloudWatchLogsClient.partitionForRegion("us-gov-west-1")).isEqualTo("aws-us-gov");
    assertThat(CloudWatchLogsClient.partitionForRegion("us-iso-east-1")).isEqualTo("aws-iso");
    assertThat(CloudWatchLogsClient.partitionForRegion("us-isob-east-1")).isEqualTo("aws-iso-b");
    assertThat(CloudWatchLogsClient.partitionForRegion("eusc-de-east-1")).isEqualTo("aws-eusc");
    assertThat(CloudWatchLogsClient.partitionForRegion("eu-west-1")).isEqualTo("aws");
    assertThat(CloudWatchLogsClient.stripStar("a:*")).isEqualTo("a");
    assertThat(CloudWatchLogsClient.RETENTION_DAYS).contains(1, 3653);
  }

  @Test
  void logGroupArn() {
    assertThat(client.logGroupArn(ARN + ":*")).isEqualTo(ARN);
    mock.on("DescribeLogGroups", 200, group(G, ""));
    assertThat(client.logGroupArn(G)).isEqualTo(ARN);
    try (CloudWatchLogsClient acct =
        mock.client(mock.config().accountId("123456789012").region("cn-north-1"), i -> {})) {
      assertThat(acct.logGroupArnAsync("x").join())
          .isEqualTo("arn:aws-cn:logs:cn-north-1:123456789012:log-group:x");
    }
    assertThatThrownBy(() -> client.logGroupArn("bad name"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void queryClientAndExporterFactories() {
    assertThat(client.logQueryClient().group()).isEqualTo("g");
    assertThat(client.logQueryClient("other").group()).isEqualTo("other");
    assertThat(client.exporter(null, null, false).stream()).isEqualTo("s");
    assertThat(client.exporter("x", "y", true).group()).isEqualTo("x");
    assertThatThrownBy(() -> client.logQueryClient("bad name"))
        .isInstanceOf(ValidationException.class);
    try (CloudWatchLogsClient noGroup = mock.client(mock.config().logGroup(null), i -> {})) {
      assertThatThrownBy(noGroup::logQueryClient).isInstanceOf(ConfigException.class);
      assertThatThrownBy(noGroup::logClient).isInstanceOf(ConfigException.class);
    }
  }

  static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
