package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BatchingEmfTest {
  static final Instant T = Instant.ofEpochMilli(1_795_034_916_000L);

  @Test
  void sortsAndKeepsOneBatch() {
    LogEventBatching.BatchPlan p =
        LogEventBatching.plan(
            List.of(LogEvent.of(T.plusMillis(2), "c"), LogEvent.of(T, "a"), LogEvent.of(T, "b")),
            1000,
            OversizePolicy.TRUNCATE);
    assertThat(p.batches()).hasSize(1);
    assertThat(p.batches().get(0))
        .extracting(LogEventBatching.WireEvent::message)
        .containsExactly("a", "b", "c");
    assertThat(LogEventBatching.plan(List.of(), 10, OversizePolicy.DROP).batches()).isEmpty();
  }

  @Test
  void splitsByCountBytesAndSpan() {
    List<LogEvent> many = new ArrayList<>(Collections.nCopies(20_001, LogEvent.of(T, "x")));
    assertThat(LogEventBatching.plan(many, 1000, OversizePolicy.TRUNCATE).batches())
        .extracting(List::size)
        .containsExactly(10_000, 10_000, 1);
    String big = "y".repeat(200_000);
    List<LogEvent> heavy = new ArrayList<>(Collections.nCopies(6, LogEvent.of(T, big)));
    assertThat(LogEventBatching.plan(heavy, 262_144, OversizePolicy.TRUNCATE).batches())
        .extracting(List::size)
        .containsExactly(5, 1);
    List<LogEvent> span =
        List.of(
            LogEvent.of(T, "a"),
            LogEvent.of(T.plusMillis(LogEventBatching.MAX_BATCH_SPAN_MS), "b"),
            LogEvent.of(T.plusMillis(LogEventBatching.MAX_BATCH_SPAN_MS + 1), "c"));
    assertThat(LogEventBatching.plan(span, 1000, OversizePolicy.TRUNCATE).batches())
        .extracting(List::size)
        .containsExactly(2, 1);
  }

  @Test
  void oversizePolicies() {
    String big = "z".repeat(500);
    LogEventBatching.BatchPlan t =
        LogEventBatching.plan(List.of(LogEvent.of(T, big)), 100, OversizePolicy.TRUNCATE);
    assertThat(t.truncated()).isEqualTo(1);
    String msg = t.batches().get(0).get(0).message();
    assertThat(LogEventBatching.utf8Bytes(msg)).isLessThanOrEqualTo(100);
    assertThat(msg).endsWith("…[truncated " + (500 - msg.indexOf(' ')) + " bytes]");
    LogEventBatching.BatchPlan d =
        LogEventBatching.plan(List.of(LogEvent.of(T, big)), 100, OversizePolicy.DROP);
    assertThat(d.dropped()).isEqualTo(1);
    assertThat(d.batches()).isEmpty();
    assertThatThrownBy(
            () -> LogEventBatching.plan(List.of(LogEvent.of(T, big)), 100, OversizePolicy.ERROR))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> LogEventBatching.plan(List.of(), 0, OversizePolicy.ERROR))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> LogEventBatching.plan(List.of(), 2_000_000, OversizePolicy.ERROR))
        .isInstanceOf(ValidationException.class);
    List<LogEvent> withNull = new ArrayList<>();
    withNull.add(null);
    assertThatThrownBy(() -> LogEventBatching.plan(withNull, 10, OversizePolicy.ERROR))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void truncateOnUtf8Boundaries() {
    assertThat(LogEventBatching.truncateMessage("short", 10)).isEqualTo("short");
    String euro = "€".repeat(100); // 3 bytes each
    for (int max : new int[] {30, 31, 32, 33, 50, 299}) {
      String cut = LogEventBatching.truncateMessage(euro, max);
      assertThat(LogEventBatching.utf8Bytes(cut)).isLessThanOrEqualTo(max);
      assertThat(cut).doesNotContain("�");
    }
    // the marker alone does not fit: a plain cut
    assertThat(LogEventBatching.utf8Bytes(LogEventBatching.truncateMessage(euro, 5)))
        .isLessThanOrEqualTo(5);
    assertThat(LogEvent.of("x").timestamp()).isNotNull();
    assertThatThrownBy(() -> new LogEvent(null, "x")).isInstanceOf(NullPointerException.class);
  }

  @Test
  void emfDocuments() {
    Map<String, Object> doc =
        Emf.build(
            "latency",
            42,
            Map.of("service", "api"),
            Emf.Options.defaults()
                .withNamespace("NS")
                .withUnit("Milliseconds")
                .withTimestamp(T)
                .withProperties(Map.of("marker", "m")));
    assertThat(Emf.toJson(doc))
        .startsWith(
            "{\"_aws\":{\"Timestamp\":1795034916000,\"CloudWatchMetrics\":[{\"Namespace\":\"NS\"")
        .contains("\"Dimensions\":[[\"service\"]]")
        .contains("\"Metrics\":[{\"Name\":\"latency\",\"Unit\":\"Milliseconds\"}]")
        .contains("\"marker\":\"m\"")
        .contains("\"latency\":42.0");
    Map<String, Object> list = Emf.build("m", List.of(1.0, 2.0), null, null);
    assertThat(list.get("m")).isEqualTo(List.of(1.0, 2.0));
    assertThat(Emf.Options.defaults().unit()).isEqualTo("None");
  }

  @Test
  void emfValidation() {
    Emf.Options o = Emf.Options.defaults();
    assertThatThrownBy(() -> Emf.build("", 1, null, o)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build(null, 1, null, o)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("x".repeat(1025), 1, null, o))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("m", 1, null, o.withNamespace("")))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("m", 1, null, o.withNamespace("n".repeat(1025))))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("m", 1, null, o.withUnit("Parsecs")))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("m", Double.NaN, null, o))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("m", List.of(), null, o))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("m", (List<Double>) null, null, o))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Emf.build("m", Collections.nCopies(101, 1.0), null, o))
        .isInstanceOf(ValidationException.class);
    List<Double> withNull = new ArrayList<>();
    withNull.add(null);
    assertThatThrownBy(() -> Emf.build("m", withNull, null, o))
        .isInstanceOf(ValidationException.class);
    Map<String, String> dims = new HashMap<>();
    for (int i = 0; i < 31; i++) {
      dims.put("d" + i, "v");
    }
    assertThatThrownBy(() -> Emf.build("m", 1, dims, o)).isInstanceOf(ValidationException.class);
    for (String bad : new String[] {"", "_aws", "m"}) {
      assertThatThrownBy(() -> Emf.build("m", 1, Map.of(bad, "v"), o))
          .isInstanceOf(ValidationException.class);
    }
    Map<String, String> nullValue = new HashMap<>();
    nullValue.put("k", null);
    assertThatThrownBy(() -> Emf.build("m", 1, nullValue, o))
        .isInstanceOf(ValidationException.class);
    Map<String, String> nullKey = new HashMap<>();
    nullKey.put(null, "v");
    assertThatThrownBy(() -> Emf.build("m", 1, nullKey, o)).isInstanceOf(ValidationException.class);
    for (String reserved : new String[] {"_aws", "m"}) {
      assertThatThrownBy(() -> Emf.build("m", 1, null, o.withProperties(Map.of(reserved, 1))))
          .isInstanceOf(ValidationException.class);
    }
  }
}
