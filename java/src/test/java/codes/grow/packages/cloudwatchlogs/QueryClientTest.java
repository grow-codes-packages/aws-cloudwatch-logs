package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.cloudwatchlogs.core.NotFoundException;
import codes.grow.packages.cloudwatchlogs.core.UnsupportedException;
import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import codes.grow.packages.cloudwatchlogs.logging.BatchOptions;
import codes.grow.packages.cloudwatchlogs.logging.BatchingLogClient;
import codes.grow.packages.cloudwatchlogs.logging.ExportOptions;
import codes.grow.packages.cloudwatchlogs.logging.ExportResult;
import codes.grow.packages.cloudwatchlogs.logging.LogInput;
import codes.grow.packages.cloudwatchlogs.logging.LogRecord;
import codes.grow.packages.cloudwatchlogs.logging.LogSearchResult;
import codes.grow.packages.cloudwatchlogs.logging.RedactionProcessor;
import codes.grow.packages.cloudwatchlogs.logging.SearchOptions;
import codes.grow.packages.cloudwatchlogs.logging.Severity;
import codes.grow.packages.cloudwatchlogs.logging.SortOrder;
import codes.grow.packages.cloudwatchlogs.logging.TailHandle;
import codes.grow.packages.cloudwatchlogs.logging.TailOptions;
import codes.grow.packages.cloudwatchlogs.logging.WaitForLogOptions;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryClientTest {
  AwsMock mock;
  CloudWatchLogsClient client;
  CloudWatchLogsQueryClient q;

  @BeforeEach
  void start() throws Exception {
    mock = new AwsMock();
    client = mock.client(mock.config().accountId("123456789012"), i -> {});
    q = client.logQueryClient();
  }

  @AfterEach
  void stop() {
    client.close();
    mock.close();
  }

  static String events(long... timestamps) {
    StringBuilder sb = new StringBuilder("{\"events\":[");
    for (int i = 0; i < timestamps.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"eventId\":\"e")
          .append(i)
          .append("\",\"logStreamName\":\"s\",\"message\":\"m")
          .append(i)
          .append("\",\"timestamp\":")
          .append(timestamps[i])
          .append('}');
    }
    return sb.append("]}").toString();
  }

  /** Answers FilterLogEvents with the events inside the requested [startTime, endTime]. */
  codes.grow.packages.cloudwatchlogs.core.MockServer.Responder timed(long... ts) {
    return ex -> {
      JsonNode req = mock.lastBody("FilterLogEvents");
      long lo = req.path("startTime").asLong(Long.MIN_VALUE);
      long hi = req.path("endTime").asLong(Long.MAX_VALUE);
      List<Long> in = new ArrayList<>();
      for (long t : ts) {
        if (t >= lo && t <= hi) {
          in.add(t);
        }
      }
      long[] arr = in.stream().mapToLong(Long::longValue).toArray();
      AwsMock.send(ex, 200, events(arr), "application/x-amz-json-1.1");
    };
  }

  @Test
  void filterPatternAndRecords() {
    assertThat(CloudWatchLogsQueryClient.filterPattern(null)).isEmpty();
    assertThat(CloudWatchLogsQueryClient.filterPattern("  ")).isEmpty();
    assertThat(CloudWatchLogsQueryClient.filterPattern("a \"b\" \\c"))
        .isEqualTo("\"a \\\"b\\\" \\\\c\"");
    for (String p : List.of("{ $.x = 1 }", "?a ?b", "[w1]", "-x", "\"q\"")) {
      assertThat(CloudWatchLogsQueryClient.filterPattern(p)).isEqualTo(p);
    }
    Instant t = Instant.ofEpochMilli(5);
    LogRecord plain =
        CloudWatchLogsQueryClient.messageToRecord("hello", t, Instant.ofEpochMilli(6));
    assertThat(plain.body()).isEqualTo("hello");
    assertThat(plain.severity()).isEqualTo(Severity.INFO);
    assertThat(plain.observedTimestamp()).contains(Instant.ofEpochMilli(6));
    LogRecord json =
        CloudWatchLogsQueryClient.messageToRecord(
            "{\"timestamp\":\"2026-11-18T10:00:00Z\",\"body\":\"b\",\"severity\":17,"
                + "\"attributes\":{\"k\":\"v\"}}",
            t,
            null);
    assertThat(json.body()).isEqualTo("b");
    assertThat(json.severity()).isEqualTo(Severity.ERROR);
    assertThat(CloudWatchLogsQueryClient.messageToRecord("{\"level\":\"info\"}", t, null).body())
        .isEqualTo("{\"level\":\"info\"}");
    assertThat(CloudWatchLogsQueryClient.messageToRecord("{not json", t, null).body())
        .isEqualTo("{not json");
    assertThat(CloudWatchLogsQueryClient.messageToRecord("[1]", t, null).body()).isEqualTo("[1]");
    LogRecord filtered =
        CloudWatchLogsQueryClient.filteredEventToRecord(
            new FilteredLogEvent(
                t,
                "{\"timestamp\":\"2026-11-18T10:00:00Z\",\"body\":\"b\",\"attributes\":{\"k\":1}}",
                Optional.empty(),
                Optional.of("s1"),
                Optional.of("e1")));
    assertThat(filtered.attributes())
        .containsEntry("k", 1L)
        .containsEntry(CloudWatchLogsQueryClient.ATTR_LOG_STREAM, "s1")
        .containsEntry(CloudWatchLogsQueryClient.ATTR_EVENT_ID, "e1");
    LogRecord filteredPlain =
        CloudWatchLogsQueryClient.filteredEventToRecord(
            new FilteredLogEvent(t, "x", Optional.of(t), Optional.empty(), Optional.empty()));
    assertThat(filteredPlain.attributes()).isEmpty();
    LogRecord tailPlain =
        CloudWatchLogsQueryClient.liveTailEventToRecord(
            new LiveTailEvent(t, Optional.empty(), "x", Optional.of("s"), Optional.of("arn")));
    assertThat(tailPlain.attributes())
        .containsEntry(CloudWatchLogsQueryClient.ATTR_LOG_GROUP, "arn")
        .containsEntry(CloudWatchLogsQueryClient.ATTR_LOG_STREAM, "s");
    LogRecord tailJson =
        CloudWatchLogsQueryClient.liveTailEventToRecord(
            new LiveTailEvent(
                t,
                Optional.of(t),
                "{\"timestamp\":\"2026-11-18T10:00:00Z\",\"body\":\"b\"}",
                Optional.empty(),
                Optional.of("arn")));
    assertThat(tailJson.attributes()).doesNotContainKey(CloudWatchLogsQueryClient.ATTR_LOG_GROUP);
  }

  @Test
  void searchAscendingWithCursor() {
    mock.on("FilterLogEvents", 200, "{\"events\":[],\"nextToken\":\"t0\"}")
        .on("FilterLogEvents", 200, events(1, 2).replace("]}", "],\"nextToken\":\"t1\"}"));
    LogSearchResult r =
        q.search(
            "marker",
            SearchOptions.builder()
                .order(SortOrder.ASC)
                .limit(2)
                .start(Instant.ofEpochMilli(1))
                .end(Instant.ofEpochMilli(9))
                .build());
    assertThat(r.records()).hasSize(2);
    assertThat(r.records().get(0).attributes()).containsEntry("aws.event.id", "e0");
    assertThat(r.nextCursor()).isPresent();
    JsonNode body = mock.lastBody("FilterLogEvents");
    assertThat(body.get("filterPattern").asText()).isEqualTo("\"marker\"");
    assertThat(body.get("nextToken").asText()).isEqualTo("t0");
    mock.on("FilterLogEvents", 200, events(3, 4, 5));
    LogSearchResult next =
        q.search(
            "marker",
            SearchOptions.builder()
                .order(SortOrder.ASC)
                .limit(2)
                .cursor(r.nextCursor().get())
                .build());
    assertThat(next.records()).hasSize(2);
    assertThat(next.nextCursor()).isEmpty();
    assertThat(mock.lastBody("FilterLogEvents").get("nextToken").asText()).isEqualTo("t1");
    assertThatThrownBy(
            () -> q.search("m", SearchOptions.builder().cursor(r.nextCursor().get()).build()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("other order");
  }

  @Test
  void searchDescendingWalksSlices() {
    long now = System.currentTimeMillis();
    // group created 2 hours ago: the window starts 3 hours ago
    mock.on(
        "DescribeLogGroups",
        200,
        "{\"logGroups\":[{\"logGroupName\":\"g\",\"creationTime\":" + (now - 7_200_000) + "}]}");
    mock.always("FilterLogEvents", timed(now - 1000, now - 2000, now - 3_600_000));
    LogSearchResult r =
        q.search("", SearchOptions.builder().limit(2).end(Instant.ofEpochMilli(now)).build());
    assertThat(r.records()).hasSize(2);
    assertThat(r.records().get(0).timestamp()).isAfter(r.records().get(1).timestamp());
    assertThat(r.nextCursor()).isPresent();
    assertThat(mock.lastBody("FilterLogEvents").has("filterPattern")).isFalse();
    LogSearchResult r2 =
        q.search(
            "",
            SearchOptions.builder()
                .limit(2)
                .end(Instant.ofEpochMilli(now))
                .cursor(r.nextCursor().get())
                .build());
    assertThat(r2.records()).hasSize(1);
    // creation time is cached: one DescribeLogGroups only
    assertThat(mock.requests("DescribeLogGroups")).hasSize(1);
    // the window is exhausted by walking empty slices
    mock.always("FilterLogEvents", "{\"events\":[]}");
    LogSearchResult rest = q.search("x", SearchOptions.builder().limit(5).build());
    assertThat(rest.records()).isEmpty();
    assertThat(rest.nextCursor()).isEmpty();
    // explicit start, ending mid-window with records remaining
    mock.always("FilterLogEvents", events(now - 10, now - 20));
    LogSearchResult limited =
        q.search(
            "x",
            SearchOptions.builder()
                .limit(2)
                .start(Instant.ofEpochMilli(now - 86_400_000L))
                .end(Instant.ofEpochMilli(now))
                .build());
    assertThat(limited.records()).hasSize(2);
    assertThat(limited.nextCursor()).isPresent();
  }

  @Test
  void searchDescendingWithoutCreationTimeAndNarrowing() {
    long now = System.currentTimeMillis();
    mock.on("DescribeLogGroups", 200, "{\"logGroups\":[]}");
    long[] many = new long[CloudWatchLogsQueryClient.SEARCH_SCAN_CAP + 1];
    for (int i = 0; i < many.length; i++) {
      many[i] = now - i;
    }
    String big = events(many);
    // the first two scans exceed the cap and narrow the slice, then a small slice answers
    mock.on("FilterLogEvents", 200, big)
        .on("FilterLogEvents", 200, big)
        .always("FilterLogEvents", events(now - 5));
    LogSearchResult r = q.search("x", SearchOptions.builder().limit(1).build());
    assertThat(r.records()).hasSize(1);
  }

  @Test
  void searchValidation() {
    assertThatThrownBy(() -> q.search("x", SearchOptions.builder().limit(10_001).build()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> q.search("x", SearchOptions.builder().cursor("!!").build()))
        .isInstanceOf(ValidationException.class);
    String badDesc =
        Base64.getUrlEncoder().encodeToString("{\"o\":\"d\",\"e\":1,\"w\":0,\"k\":0}".getBytes());
    assertThatThrownBy(() -> q.search("x", SearchOptions.builder().cursor(badDesc).build()))
        .isInstanceOf(ValidationException.class);
    String badAsc = Base64.getUrlEncoder().encodeToString("{\"o\":\"a\"}".getBytes());
    assertThatThrownBy(
            () ->
                q.search("x", SearchOptions.builder().order(SortOrder.ASC).cursor(badAsc).build()))
        .isInstanceOf(ValidationException.class);
    String negative =
        Base64.getUrlEncoder().encodeToString("{\"o\":\"d\",\"e\":1,\"w\":1,\"k\":-1}".getBytes());
    assertThatThrownBy(() -> q.search("x", SearchOptions.builder().cursor(negative).build()))
        .isInstanceOf(ValidationException.class);
    String strings =
        Base64.getUrlEncoder()
            .encodeToString("{\"o\":\"d\",\"e\":\"1\",\"w\":1,\"k\":0}".getBytes());
    assertThatThrownBy(() -> q.search("x", SearchOptions.builder().cursor(strings).build()))
        .isInstanceOf(ValidationException.class);
    mock.fixture("filterLogEvents.not-found");
    assertThatThrownBy(() -> q.search("x", SearchOptions.builder().order(SortOrder.ASC).build()))
        .isInstanceOf(NotFoundException.class);
    assertThatThrownBy(q::listLabels).isInstanceOf(UnsupportedException.class);
  }

  @Test
  void waitForLogAndListStreams() {
    mock.always(
        "FilterLogEvents",
        "{\"events\":[{\"eventId\":\"1\",\"message\":\"needle\",\"timestamp\":"
            + System.currentTimeMillis()
            + "}]}");
    mock.always("DescribeLogGroups", "{\"logGroups\":[]}");
    LogRecord r =
        q.waitForLog("needle", WaitForLogOptions.of(Duration.ofSeconds(5), Duration.ofMillis(10)));
    assertThat(r.body()).isEqualTo("needle");
    mock.fixture("describeLogStreams.all");
    assertThat(q.listStreams()).isNotEmpty();
    assertThat(q.group()).isEqualTo("g");
  }

  @Test
  void tailUsesLiveTail() throws Exception {
    mock.fixture("startLiveTail.seed");
    List<LogRecord> got = Collections.synchronizedList(new ArrayList<>());
    TailHandle h = q.tail("grow-seed-event", got::add, TailOptions.defaults());
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (got.size() < 3 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    h.stop();
    h.close();
    assertThat(got).hasSize(3);
    assertThat(got.get(0).attributes()).containsKey(CloudWatchLogsQueryClient.ATTR_LOG_STREAM);
    assertThat(mock.lastBody("StartLiveTail").get("logEventFilterPattern").asText())
        .isEqualTo("\"grow-seed-event\"");
    // a start failure goes to onError
    mock.on("StartLiveTail", AwsMock.responder(AwsMock.load("ensureLogGroup.access-denied")));
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    q.tail("x", r -> {}, TailOptions.defaults().withOnError(errors::add));
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (errors.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertThat(errors).hasSize(1);
    // stopping before the session starts closes it when it arrives; callbacks stay silent
    mock.fixture("startLiveTail.seed");
    List<LogRecord> none = Collections.synchronizedList(new ArrayList<>());
    TailHandle early =
        q.tail(
            "x",
            none::add,
            TailOptions.defaults()
                .withOnError(
                    e -> {
                      throw new IllegalStateException("ignored");
                    }));
    early.stop();
    Thread.sleep(300);
    assertThat(none).isEmpty();
    mock.on("StartLiveTail", AwsMock.responder(AwsMock.load("ensureLogGroup.access-denied")));
    q.tail(
            "x",
            r -> {},
            TailOptions.defaults()
                .withOnError(
                    e -> {
                      throw new IllegalStateException("callbacks never break the session");
                    }))
        .stop();
    q.tail("x", r -> {}, null).stop();
  }

  // ---------------------------------------------------------------- exporter + log client

  @Test
  void recordToMessage() throws Exception {
    LogRecord small = LogRecord.builder().timestamp(Instant.EPOCH).body("hi").build();
    assertThat(CloudWatchLogsExporter.recordToMessage(small, 1000, OversizePolicy.TRUNCATE))
        .contains("\"body\":\"hi\"");
    LogRecord big = LogRecord.builder().timestamp(Instant.EPOCH).body("é".repeat(5000)).build();
    String cut = CloudWatchLogsExporter.recordToMessage(big, 2000, OversizePolicy.TRUNCATE);
    assertThat(LogEventBatching.utf8Bytes(cut)).isLessThanOrEqualTo(2000);
    JsonNode parsed = AwsMock.JSON.readTree(cut);
    assertThat(parsed.get("body").asText()).contains("truncated");
    assertThat(CloudWatchLogsExporter.recordToMessage(big, 2000, OversizePolicy.DROP)).isNull();
    assertThat(CloudWatchLogsExporter.recordToMessage(big, 2000, OversizePolicy.ERROR)).isNull();
    LogRecord fat =
        LogRecord.builder()
            .timestamp(Instant.EPOCH)
            .body("b".repeat(100))
            .attribute("k", "v".repeat(5000))
            .build();
    String plainCut = CloudWatchLogsExporter.recordToMessage(fat, 1000, OversizePolicy.TRUNCATE);
    assertThat(LogEventBatching.utf8Bytes(plainCut)).isLessThanOrEqualTo(1000);
    assertThat(plainCut).contains("truncated");
  }

  @Test
  void exporterExports() {
    mock.always("PutLogEvents", "{}");
    CloudWatchLogsExporter exp = client.exporter("g", "exp", false);
    ExportResult ok =
        exp.export(
                List.of(LogRecord.builder().timestamp(Instant.now()).body("x").build()),
                new ExportOptions(true, Duration.ofSeconds(5)))
            .join();
    assertThat(ok.accepted()).isEqualTo(1);
    assertThat(ok.error()).isEmpty();
    assertThat(
            exp.export(List.of(), new ExportOptions(false, Duration.ofSeconds(1)))
                .join()
                .accepted())
        .isZero();
    assertThat(exp.shutdown()).isDone();
    try (CloudWatchLogsClient dropping =
        mock.client(
            mock.config().maxEventBytes(100).oversizePolicy(OversizePolicy.DROP), i -> {})) {
      ExportResult r =
          dropping
              .exporter(null, null, true)
              .export(
                  List.of(
                      LogRecord.builder().timestamp(Instant.now()).body("x".repeat(500)).build()),
                  new ExportOptions(false, Duration.ofSeconds(5)))
              .join();
      assertThat(r.rejected()).isEqualTo(1);
      assertThat(r.error()).isPresent();
    }
    mock.on("PutLogEvents", 200, "{\"rejectedLogEventsInfo\":{\"tooOldLogEventEndIndex\":0}}");
    ExportResult rejected =
        exp.export(
                List.of(LogRecord.builder().timestamp(Instant.now()).body("x").build()),
                new ExportOptions(false, Duration.ofSeconds(5)))
            .join();
    assertThat(rejected.rejected()).isEqualTo(1);
  }

  @Test
  void logClientSendsNormalizedJson() throws Exception {
    mock.always("PutLogEvents", "{}");
    BatchingLogClient logs =
        client.logClient(
            CloudWatchLogClientOptions.defaults()
                .withTarget("g2", "s2")
                .withCreateGroup(false)
                .withBatchOptions(BatchOptions.defaults())
                .withResource(Map.of("service.name", "svc"))
                .withRedaction(RedactionProcessor.defaults()));
    logs.send(LogInput.of(Severity.WARN, "hello", Map.of("password", "hunter2")));
    logs.flush();
    JsonNode body = mock.lastBody("PutLogEvents");
    assertThat(body.get("logGroupName").asText()).isEqualTo("g2");
    assertThat(body.get("logStreamName").asText()).isEqualTo("s2");
    JsonNode rec = AwsMock.JSON.readTree(body.get("logEvents").get(0).get("message").asText());
    assertThat(rec.get("body").asText()).isEqualTo("hello");
    assertThat(rec.get("resource").get("service.name").asText()).isEqualTo("svc");
    assertThat(rec.toString()).doesNotContain("hunter2");
    BatchingLogClient defaults = client.logClient();
    defaults.send(LogInput.of("bye"));
    client.close();
    assertThat(mock.requests("PutLogEvents")).hasSize(2);
    assertThat(mock.lastBody("PutLogEvents").get("logGroupName").asText()).isEqualTo("g");
    assertThat(CloudWatchLogClientOptions.defaults().withCreateGroup(true).createGroup()).isTrue();
  }
}
