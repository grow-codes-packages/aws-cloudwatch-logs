package codes.grow.packages.cloudwatchlogs.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import codes.grow.packages.cloudwatchlogs.CloudWatchLogClientOptions;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsClient;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsConfig;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsExporter;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsQueryClient;
import codes.grow.packages.cloudwatchlogs.DescribeLogGroupsOptions;
import codes.grow.packages.cloudwatchlogs.DescribeLogStreamsOptions;
import codes.grow.packages.cloudwatchlogs.DescribeQueriesOptions;
import codes.grow.packages.cloudwatchlogs.Emf;
import codes.grow.packages.cloudwatchlogs.EnsureLogGroupOptions;
import codes.grow.packages.cloudwatchlogs.EnsureLogGroupResult;
import codes.grow.packages.cloudwatchlogs.FilterLogEventsOptions;
import codes.grow.packages.cloudwatchlogs.FilterLogEventsPage;
import codes.grow.packages.cloudwatchlogs.FilteredLogEvent;
import codes.grow.packages.cloudwatchlogs.GetLogEventsOptions;
import codes.grow.packages.cloudwatchlogs.LiveTailEvent;
import codes.grow.packages.cloudwatchlogs.LiveTailHandle;
import codes.grow.packages.cloudwatchlogs.LiveTailOptions;
import codes.grow.packages.cloudwatchlogs.LiveTailRequest;
import codes.grow.packages.cloudwatchlogs.LiveTailUpdate;
import codes.grow.packages.cloudwatchlogs.LogEvent;
import codes.grow.packages.cloudwatchlogs.LogGroupInfo;
import codes.grow.packages.cloudwatchlogs.LogStreamInfo;
import codes.grow.packages.cloudwatchlogs.PutLogEventsResult;
import codes.grow.packages.cloudwatchlogs.QueryInfo;
import codes.grow.packages.cloudwatchlogs.QueryOptions;
import codes.grow.packages.cloudwatchlogs.QueryResults;
import codes.grow.packages.cloudwatchlogs.QueryStatus;
import codes.grow.packages.cloudwatchlogs.StreamLogEvent;
import codes.grow.packages.cloudwatchlogs.core.AuthenticationException;
import codes.grow.packages.cloudwatchlogs.core.CancelledException;
import codes.grow.packages.cloudwatchlogs.core.ConflictException;
import codes.grow.packages.cloudwatchlogs.core.HealthStatus;
import codes.grow.packages.cloudwatchlogs.core.NotFoundException;
import codes.grow.packages.cloudwatchlogs.core.PermissionDeniedException;
import codes.grow.packages.cloudwatchlogs.core.UnavailableException;
import codes.grow.packages.cloudwatchlogs.core.UnsupportedException;
import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import codes.grow.packages.cloudwatchlogs.logging.BatchingLogClient;
import codes.grow.packages.cloudwatchlogs.logging.ExportOptions;
import codes.grow.packages.cloudwatchlogs.logging.ExportResult;
import codes.grow.packages.cloudwatchlogs.logging.LogInput;
import codes.grow.packages.cloudwatchlogs.logging.LogRecord;
import codes.grow.packages.cloudwatchlogs.logging.LogSearchResult;
import codes.grow.packages.cloudwatchlogs.logging.SearchOptions;
import codes.grow.packages.cloudwatchlogs.logging.Severity;
import codes.grow.packages.cloudwatchlogs.logging.SortOrder;
import codes.grow.packages.cloudwatchlogs.logging.TailHandle;
import codes.grow.packages.cloudwatchlogs.logging.TailOptions;
import codes.grow.packages.cloudwatchlogs.logging.WaitForLogOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Calls every public function of the package against the real service (Moto in {@code
 * docker/compose.yml}) with a round-trip assertion. StopQuery is not implemented by Moto 5.2.4
 * (HTTP 500 → UNAVAILABLE); unit tests cover its success path.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntegrationTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static Map<String, String> env;
  private static CloudWatchLogsClient c;
  private static String prefix;
  private static String group;
  private static String other;
  private static String marker;
  private static Instant base;

  @BeforeAll
  static void setup() throws Exception {
    env = ConformanceTest.environment();
    assumeTrue(env.containsKey("GROW_AWS_CLOUDWATCH_LOGS_URL"), "run through docker/compose.yml");
    c = CloudWatchLogsClient.create(CloudWatchLogsConfig.fromEnv(env));
    prefix = "it-java-" + UUID.randomUUID().toString().substring(0, 8);
    group = prefix + "/app";
    other = prefix + "/other";
    marker = "marker-" + prefix;
    base = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
  }

  @AfterAll
  static void cleanup() {
    if (c == null) {
      return;
    }
    for (String g : List.of(group, other, prefix + "/tail", prefix + "/kms")) {
      try {
        c.deleteLogGroup(g, true);
      } catch (RuntimeException ignored) {
        // best effort
      }
    }
    c.close();
  }

  private static <T> List<T> list(Iterable<T> it) {
    List<T> out = new ArrayList<>();
    it.forEach(out::add);
    return out;
  }

  @Test
  @Order(1)
  void core() {
    HealthStatus hs = c.health();
    assertTrue(hs.healthy());
    assertEquals("ok", hs.status());
    assertTrue(c.ping().toNanos() > 0);
    assertTrue(c.pingAsync().join().toNanos() > 0);
    assertThrows(UnsupportedException.class, c::serverVersion);
    assertFalse(c.region().isEmpty());
    assertNotNull(c.sdk());
    assertTrue(c.config().logGroup().isPresent());
    try (CloudWatchLogsClient bad =
        CloudWatchLogsClient.create(
            CloudWatchLogsConfig.builder()
                .url(c.config().url())
                .region(c.region())
                .accessKeyId("AKIANOTAREALKEY00000")
                .secretAccessKey("wrong-secret")
                .build())) {
      assertThrows(AuthenticationException.class, bad::ping);
      HealthStatus h = bad.health();
      assertTrue(h.healthy());
      assertEquals("failed", h.details().get("auth"));
    }
  }

  @Test
  @Order(2)
  void bootstrapFunctions() {
    EnsureLogGroupResult res =
        c.ensureLogGroup(
            group,
            EnsureLogGroupOptions.builder().retentionDays(7).tags(Map.of("team", "sdk")).build());
    assertTrue(res.created());
    assertTrue(res.arn().endsWith(":log-group:" + group), res.arn());
    res =
        c.ensureLogGroup(
            group,
            EnsureLogGroupOptions.builder().retentionDays(14).tags(Map.of("env", "it")).build());
    assertFalse(res.created());
    Map<String, String> tags = c.listLogGroupTags(group);
    assertEquals("sdk", tags.get("team"));
    assertEquals("it", tags.get("env"));
    c.tagLogGroup(group, Map.of("x", "1"));
    c.untagLogGroup(res.arn(), List.of("team"));
    tags = c.listLogGroupTagsAsync(group).join();
    assertEquals("1", tags.get("x"));
    assertFalse(tags.containsKey("team"));
    List<LogGroupInfo> groups =
        list(
            c.describeLogGroups(
                DescribeLogGroupsOptions.builder().prefix(prefix + "/").pageSize(1).build()));
    assertEquals(1, groups.size());
    assertEquals(group, groups.get(0).name());
    assertEquals(14, groups.get(0).retentionDays().getAsInt());
    LogGroupInfo g = c.getLogGroup(group).orElseThrow();
    assertEquals(res.arn(), g.arn());
    assertTrue(c.getLogGroup(prefix + "/missing").isEmpty());
    assertEquals(res.arn(), c.logGroupArn(group));
    c.putRetentionPolicy(group, 30);
    assertEquals(
        30,
        c.describeLogGroupsAsync(DescribeLogGroupsOptions.builder().prefix(group).build())
            .join()
            .get(0)
            .retentionDays()
            .getAsInt());
    c.putRetentionPolicy(group, 0);
    assertTrue(c.getLogGroup(group).orElseThrow().retentionDays().isEmpty());
    for (String s : List.of("s1", "s2", "s3")) {
      assertTrue(c.ensureLogStream(group, s).created());
    }
    assertFalse(c.ensureLogStream(group, "s1").created());
    List<LogStreamInfo> streams =
        list(c.describeLogStreams(group, DescribeLogStreamsOptions.builder().pageSize(2).build()));
    assertEquals(3, streams.size());
    assertEquals("s1", streams.get(0).name());
    assertEquals(
        2,
        c.describeLogStreamsAsync(group, DescribeLogStreamsOptions.builder().limit(2).build())
            .join()
            .size());
    List<LogStreamInfo> desc =
        list(
            c.describeLogStreams(group, DescribeLogStreamsOptions.builder().descending(true).build()));
    assertEquals("s3", desc.get(0).name());
    assertTrue(c.deleteLogStream(group, "s3"));
    assertFalse(c.deleteLogStream(group, "s3", true));
    assertThrows(NotFoundException.class, () -> c.deleteLogStream(group, "s3"));
    assertTrue(c.ensureLogStream(other, "x", true).created());
    assertTrue(c.deleteLogGroup(other));
    assertTrue(list(c.describeLogGroups(DescribeLogGroupsOptions.builder().prefix(other).build())).isEmpty());
    assertThrows(NotFoundException.class, () -> c.deleteLogGroup(other));
    String kms = prefix + "/kms";
    c.ensureLogGroup(
        kms,
        EnsureLogGroupOptions.builder()
            .kmsKeyId("arn:aws:kms:us-east-1:123456789012:key/k1")
            .build());
    assertThrows(
        ConflictException.class,
        () -> c.ensureLogGroup(kms, EnsureLogGroupOptions.builder().kmsKeyId("k2").build()));
    assertTrue(c.deleteLogGroupAsync(kms, false).join());
    assertThrows(ValidationException.class, () -> c.ensureLogGroup("bad name!"));
  }

  @Test
  @Order(3)
  void ingestFunctions() throws Exception {
    List<LogEvent> events =
        List.of(
            LogEvent.of(base.plusSeconds(2), "third " + marker + " ERROR"),
            LogEvent.of(base, "first " + marker),
            LogEvent.of(base.plusSeconds(1), "second " + marker + " ünïcode ✓"));
    PutLogEventsResult res = c.putLogEvents(group, "s1", events);
    assertEquals(3, res.accepted());
    assertEquals(1, res.batches());
    res = c.putLogEvents(group, "auto", List.of(LogEvent.of(base, "auto " + marker)));
    assertEquals(1, res.accepted());
    String big = "x".repeat(300_000) + marker;
    res = c.putLogEvents(group, "s2", List.of(LogEvent.of(base, big)));
    assertEquals(1, res.truncated());
    assertEquals(1, res.accepted());
    List<StreamLogEvent> got =
        list(c.getLogEvents(group, "s2", GetLogEventsOptions.builder().startFromHead(true).build()));
    assertEquals(1, got.size());
    assertTrue(got.get(0).message().length() <= CloudWatchLogsConfig.DEFAULT_MAX_EVENT_BYTES);
    assertTrue(got.get(0).message().contains("truncated"));
    List<LogEvent> many = new ArrayList<>(Collections.nCopies(10_001, LogEvent.of(base, "bulk")));
    res = c.putLogEventsAsync(group, "bulk", many, null).join();
    assertEquals(2, res.batches());
    assertEquals(10_001, res.accepted());
    Map<String, Object> doc =
        Emf.build(
            "latency",
            42,
            Map.of("service", "api"),
            Emf.Options.defaults().withNamespace("GrowSdkIT").withUnit("Milliseconds"));
    assertTrue(Emf.toJson(doc).contains("\"CloudWatchMetrics\""));
    Map<String, Object> sent =
        c.emf(
            "latency",
            42,
            Map.of("service", "api"),
            Emf.Options.defaults().withTarget(group, "emf").withProperties(Map.of("marker", marker)));
    assertEquals(42.0, sent.get("latency"));
    List<StreamLogEvent> emf =
        list(c.getLogEvents(group, "emf", GetLogEventsOptions.builder().startFromHead(true).build()));
    assertEquals(1, emf.size());
    JsonNode parsed = JSON.readTree(emf.get(0).message());
    assertEquals(42.0, parsed.get("latency").asDouble());
    assertEquals(marker, parsed.get("marker").asText());
  }

  @Test
  @Order(4)
  void queryFunctions() {
    List<StreamLogEvent> head =
        list(
            c.getLogEvents(
                group, "s1", GetLogEventsOptions.builder().startFromHead(true).pageSize(1).build()));
    assertEquals(3, head.size());
    assertTrue(head.get(0).message().startsWith("first"));
    assertTrue(head.get(2).message().startsWith("third"));
    assertEquals(base, head.get(0).timestamp());
    assertEquals(
        1,
        c.getLogEventsAsync(group, "s1", GetLogEventsOptions.builder().limit(1).build())
            .join()
            .size());
    List<FilteredLogEvent> filtered =
        list(
            c.filterLogEvents(
                FilterLogEventsOptions.builder()
                    .group(group)
                    .streams(List.of("s1"))
                    .pattern("ERROR")
                    .build()));
    assertEquals(1, filtered.size());
    assertTrue(filtered.get(0).message().startsWith("third"));
    assertEquals("s1", filtered.get(0).logStreamName().orElse(""));
    List<FilteredLogEvent> paged =
        list(
            c.filterLogEvents(
                FilterLogEventsOptions.builder()
                    .group(group)
                    .pattern("\"" + marker + "\"")
                    .pageSize(1)
                    .start(base.minusSeconds(1))
                    .end(Instant.now().plusSeconds(60))
                    .build()));
    assertTrue(paged.size() >= 5, "pages " + paged.size());
    assertEquals(
        2,
        c.filterLogEventsAsync(
                FilterLogEventsOptions.builder().group(group).pattern(marker).limit(2).build())
            .join()
            .size());
    FilterLogEventsOptions s1 =
        FilterLogEventsOptions.builder().group(group).streams(List.of("s1")).pageSize(2).build();
    FilterLogEventsPage page = c.filterLogEventsPage(s1, null);
    assertEquals(2, page.events().size());
    assertTrue(page.nextToken().isPresent());
    page = c.filterLogEventsPageAsync(s1, page.nextToken().get()).join();
    assertEquals(1, page.events().size());
    assertTrue(page.events().get(0).message().startsWith("third"));

    QueryOptions range =
        QueryOptions.builder()
            .groups(List.of(group))
            .start(base.minusSeconds(60))
            .end(Instant.now().plusSeconds(60))
            .build();
    String id = c.startQuery("fields @timestamp, @message | sort @timestamp asc | limit 3", range);
    assertFalse(id.isEmpty());
    QueryResults qr = c.getQueryResults(id);
    assertNotNull(qr.status());
    QueryResults res = c.query("fields @timestamp, @message | sort @timestamp desc | limit 2", range);
    assertEquals(QueryStatus.COMPLETE, res.status());
    assertEquals(2, res.rows().size());
    assertFalse(res.rows().get(0).get("@message").isEmpty());
    assertTrue(CloudWatchLogsClient.parseInsightsTimestamp(res.rows().get(0).get("@timestamp")).isPresent());
    List<QueryInfo> qs = c.describeQueries(DescribeQueriesOptions.builder().group(group).build());
    assertTrue(qs.stream().anyMatch(q -> q.queryId().equals(id)), qs.toString());
    if ("moto".equals(env.get("GROW_TEST_EMULATOR"))) {
      assertThrows(UnavailableException.class, () -> c.stopQuery(id));
    } else {
      c.stopQuery(id);
    }
  }

  @Test
  @Order(5)
  void logClientExporterAndSearch() throws Exception {
    try (CloudWatchLogsClient lc =
        CloudWatchLogsClient.create(
            CloudWatchLogsConfig.builder().logGroup(group).logStream("logclient").fromEnv(codes.grow.packages.cloudwatchlogs.core.EnvReader.of(env)).build())) {
      BatchingLogClient logs =
          lc.logClient(CloudWatchLogClientOptions.defaults().withResource(Map.of("service.name", "it-java")));
      CloudWatchLogsQueryClient q = lc.logQueryClient();
      assertEquals(group, q.group());
      logs.logger("it", Map.of("run", prefix)).info("logger hello " + marker, Map.of("password", "hunter2-" + prefix));
      logs.send(
          LogInput.builder("send hello " + marker)
              .severity(Severity.WARN)
              .traceId("4bf92f3577b34da6a3ce929d0e0e4736")
              .spanId("00f067aa0ba902b7")
              .build());
      logs.flush();
      assertEquals(2L, logs.stats().sent());
      assertEquals(0L, logs.stats().failed());
      LogRecord rec =
          q.waitForLog("send hello " + marker, WaitForLogOptions.of(Duration.ofSeconds(20), Duration.ofMillis(500)));
      assertEquals(Severity.WARN, rec.severity());
      assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", rec.traceId().orElse(""));
      assertEquals("it-java", rec.resource().get("service.name"));
      assertEquals("logclient", rec.attributes().get(CloudWatchLogsQueryClient.ATTR_LOG_STREAM));
      LogRecord lrec =
          q.waitForLog("logger hello " + marker, WaitForLogOptions.of(Duration.ofSeconds(20), Duration.ofMillis(500)));
      assertFalse(lrec.toString().contains("hunter2-" + prefix));
      assertFalse(JSON.writeValueAsString(lrec.attributes()).contains("hunter2-" + prefix));
      CloudWatchLogsExporter exp = lc.exporter(null, "exporter", false);
      ExportResult er =
          exp.export(
                  List.of(
                      LogRecord.builder()
                          .timestamp(Instant.now())
                          .severity(Severity.ERROR)
                          .body("exported " + marker)
                          .build()),
                  new ExportOptions(true, Duration.ofSeconds(10)))
              .join();
      assertEquals(1, er.accepted());
      assertEquals(0, er.rejected());
      LogSearchResult desc = q.search(marker, SearchOptions.builder().limit(3).build());
      assertEquals(3, desc.records().size());
      assertTrue(desc.nextCursor().isPresent());
      assertFalse(desc.records().get(0).timestamp().isBefore(desc.records().get(2).timestamp()));
      List<LogRecord> all = searchAll(q, marker, SortOrder.DESC);
      List<LogRecord> asc = searchAll(q, marker, SortOrder.ASC);
      assertEquals(all.size(), asc.size());
      assertTrue(all.size() >= 8, "search all " + all.size());
      assertFalse(asc.get(0).timestamp().isAfter(asc.get(asc.size() - 1).timestamp()));
      List<String> streams = q.listStreamsAsync().join();
      assertTrue(streams.containsAll(List.of("logclient", "exporter", "s1")), streams.toString());
      assertThrows(UnsupportedException.class, q::listLabels);
    }
  }

  private static List<LogRecord> searchAll(CloudWatchLogsQueryClient q, String query, SortOrder order) {
    List<LogRecord> out = new ArrayList<>();
    String cursor = null;
    do {
      LogSearchResult r =
          q.search(query, SearchOptions.builder().limit(4).order(order).cursor(cursor).build());
      out.addAll(r.records());
      cursor = r.nextCursor().orElse(null);
    } while (cursor != null);
    return out;
  }

  private static void waitFor(Supplier<Boolean> cond, LiveTailHandle h) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (!cond.get() && System.nanoTime() < deadline) {
      if (h != null && h.isClosed()) {
        Thread.sleep(50);
        break;
      }
      Thread.sleep(50);
    }
  }

  @Test
  @Order(6)
  void liveTail() throws Exception {
    // Moto resolves every logGroupIdentifier to the most recently created group and replays
    // existing events once, so create the group last and put the events before tailing.
    String tail = prefix + "/tail";
    c.ensureLogGroup(tail);
    c.putLogEvents(tail, "t1", List.of(LogEvent.of("tail one " + marker), LogEvent.of("tail two " + marker)));
    List<LiveTailEvent> got = Collections.synchronizedList(new ArrayList<>());
    List<LiveTailUpdate> updates = Collections.synchronizedList(new ArrayList<>());
    LiveTailHandle h =
        c.startLiveTail(
            LiveTailRequest.builder().groups(List.of(tail)).pattern("tail").build(),
            got::add,
            LiveTailOptions.builder().onUpdate(updates::add).build());
    waitFor(() -> got.size() >= 2, h);
    h.close();
    h.close();
    assertTrue(got.size() >= 2, "live tail " + got);
    assertTrue(got.get(0).message().contains("tail one"));
    assertEquals("t1", got.get(0).logStreamName().orElse(""));
    assertFalse(h.sessionId().isEmpty());
    assertFalse(updates.isEmpty());
    assertTrue(h.error().isEmpty(), () -> h.error().get().toString());
    assertTrue(h.isClosed());
    CloudWatchLogsQueryClient tq = c.logQueryClient(tail);
    List<LogRecord> recs = Collections.synchronizedList(new ArrayList<>());
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    TailHandle th = tq.tail("tail two", recs::add, TailOptions.defaults().withOnError(errors::add));
    waitFor(() -> !recs.isEmpty(), null);
    th.stop();
    assertEquals(1, recs.size(), recs.toString());
    assertTrue(recs.get(0).body().contains("tail two"));
    LiveTailHandle ah =
        c.startLiveTailAsync(
                LiveTailRequest.builder().groups(List.of(c.logGroupArn(tail))).streams(List.of("t1")).build(),
                ev -> {},
                null)
            .join();
    ah.close();
    assertTrue(ah.await(Duration.ofSeconds(5)));
  }

  @Test
  @Order(7)
  void permissionDeniedWithReadOnlyUser() {
    String ak = env.get("GROW_TEST_READONLY_ACCESS_KEY_ID");
    assumeTrue(ak != null && !ak.isEmpty(), "no read-only user in /bootstrap/env");
    try (CloudWatchLogsClient ro =
        CloudWatchLogsClient.create(
            CloudWatchLogsConfig.builder()
                .url(c.config().url())
                .region(c.region())
                .accessKeyId(ak)
                .secretAccessKey(env.get("GROW_TEST_READONLY_SECRET_ACCESS_KEY"))
                .build())) {
      ro.ping();
      assertThrows(PermissionDeniedException.class, () -> ro.ensureLogGroup(prefix + "/ro"));
      assertThrows(
          PermissionDeniedException.class,
          () -> ro.putLogEvents(group, "s1", List.of(LogEvent.of("x"))));
    }
  }

  @Test
  @Order(8)
  void close() {
    CloudWatchLogsClient cc = CloudWatchLogsClient.create(CloudWatchLogsConfig.fromEnv(env));
    cc.close();
    cc.close();
    assertTrue(cc.isClosed());
    assertThrows(CancelledException.class, cc::ping);
  }
}
