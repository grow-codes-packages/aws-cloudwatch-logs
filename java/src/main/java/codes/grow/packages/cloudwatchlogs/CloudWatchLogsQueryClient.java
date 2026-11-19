package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.HttpTransport;
import codes.grow.packages.cloudwatchlogs.core.Json;
import codes.grow.packages.cloudwatchlogs.core.UnsupportedException;
import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import codes.grow.packages.cloudwatchlogs.logging.LogQueryClient;
import codes.grow.packages.cloudwatchlogs.logging.LogRecord;
import codes.grow.packages.cloudwatchlogs.logging.LogRecordJson;
import codes.grow.packages.cloudwatchlogs.logging.LogSearchResult;
import codes.grow.packages.cloudwatchlogs.logging.SearchOptions;
import codes.grow.packages.cloudwatchlogs.logging.Severity;
import codes.grow.packages.cloudwatchlogs.logging.SortOrder;
import codes.grow.packages.cloudwatchlogs.logging.TailHandle;
import codes.grow.packages.cloudwatchlogs.logging.TailOptions;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * {@link LogQueryClient} over one log group (SPEC section 4, DECISIONS 2). {@code search} uses
 * {@code FilterLogEvents}: {@link SortOrder#ASC} reads natively (oldest first, cursor = the service
 * token); {@link SortOrder#DESC} (default) walks the time window backwards in slices (5 minutes,
 * each 4 times wider), from one hour before the group's creation time (else 30 days), newest slice
 * first, and sorts each slice newest first. {@code tail} uses Live Tail; {@code listStreams} uses
 * {@code DescribeLogStreams}; {@code listLabels} is unsupported (no label inventory).
 */
public final class CloudWatchLogsQueryClient implements LogQueryClient {
  /** Attribute holding the log stream of a record. */
  public static final String ATTR_LOG_STREAM = "aws.log.stream";

  /** Attribute holding the event id of a record. */
  public static final String ATTR_EVENT_ID = "aws.event.id";

  /** Attribute holding the log group of a plain live-tail record. */
  public static final String ATTR_LOG_GROUP = "aws.log.group";

  /** Look-back of a descending search without start when the creation time is unknown (30 d). */
  public static final long DEFAULT_SEARCH_LOOKBACK_MS = 30L * 24 * 60 * 60 * 1000;

  /** First time slice of a descending search (5 minutes); each further slice is 4 times wider. */
  public static final long SEARCH_FIRST_SLICE_MS = 5 * 60_000L;

  /** Largest time slice of a descending search (30 days). */
  public static final long SEARCH_MAX_SLICE_MS = 30L * 24 * 60 * 60 * 1000;

  /** Events read per slice before the slice is narrowed (newest half kept). */
  public static final int SEARCH_SCAN_CAP = 10_000;

  /** Largest page size of {@code search}. */
  public static final int MAX_SEARCH_LIMIT = 10_000;

  private static final Set<Character> NATIVE_PATTERN_START = Set.of('{', '?', '[', '-', '"');
  private static final Comparator<FilteredLogEvent> NEWEST_FIRST =
      Comparator.comparing(FilteredLogEvent::timestamp)
          .thenComparing(e -> e.eventId().orElse(""))
          .reversed();

  private final CloudWatchLogsClient client;
  private final String group;
  private volatile Long createdMs;

  CloudWatchLogsQueryClient(CloudWatchLogsClient client, String group) {
    this.client = client;
    this.group = group;
  }

  /**
   * Returns the log group searched.
   *
   * @return the group
   */
  public String group() {
    return group;
  }

  /**
   * Converts a search query to a CloudWatch filter pattern: empty matches everything; a query that
   * starts with <code>{</code>, <code>?</code>, <code>[</code>, <code>-</code> or <code>"</code> is
   * passed through as a native pattern; anything else becomes an exact phrase ({@code "…"}, inner
   * quotes escaped).
   *
   * @param query the query
   * @return the filter pattern
   */
  public static String filterPattern(String query) {
    String q = query == null ? "" : query.trim();
    if (q.isEmpty()) {
      return "";
    }
    if (NATIVE_PATTERN_START.contains(q.charAt(0))) {
      return q;
    }
    return "\"" + q.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  /**
   * Normalizes a CloudWatch message: a normalized LogRecord JSON document (with {@code timestamp}
   * and {@code body}) is decoded; any other message becomes an INFO record with the message as
   * body, {@code timestamp} as time and {@code ingestionTime} as observed time.
   *
   * @param message the message
   * @param timestamp event time
   * @param ingestionTime ingestion time, may be null
   * @return the record
   */
  public static LogRecord messageToRecord(
      String message, Instant timestamp, Instant ingestionTime) {
    LogRecord decoded = decode(message, timestamp);
    if (decoded != null) {
      return decoded;
    }
    return plain(message, timestamp, ingestionTime).build();
  }

  private static LogRecord decode(String message, Instant timestamp) {
    String trimmed = message.trim();
    if (!trimmed.startsWith("{")) {
      return null;
    }
    try {
      JsonNode node = Json.tree(trimmed.getBytes(StandardCharsets.UTF_8));
      if (!node.isObject() || !node.has("timestamp") || !node.has("body")) {
        return null;
      }
      LogRecord r = LogRecordJson.fromJsonNode(node);
      return r;
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static LogRecord.Builder plain(String message, Instant timestamp, Instant ingestion) {
    LogRecord.Builder b =
        LogRecord.builder()
            .timestamp(timestamp)
            .severity(Severity.INFO)
            .severityText(Severity.INFO.text())
            .body(message);
    if (ingestion != null) {
      b.observedTimestamp(ingestion);
    }
    return b;
  }

  /**
   * Converts a {@code FilterLogEvents} event to a record (see {@link #messageToRecord}) with the
   * attributes {@code aws.log.stream} and {@code aws.event.id}.
   *
   * @param e the event
   * @return the record
   */
  public static LogRecord filteredEventToRecord(FilteredLogEvent e) {
    LogRecord decoded = decode(e.message(), e.timestamp());
    LogRecord.Builder b =
        decoded != null
            ? decoded.toBuilder()
            : plain(e.message(), e.timestamp(), e.ingestionTime().orElse(null));
    Map<String, Object> attrs =
        new LinkedHashMap<>(decoded != null ? decoded.attributes() : Map.of());
    e.logStreamName().ifPresent(s -> attrs.put(ATTR_LOG_STREAM, s));
    e.eventId().ifPresent(s -> attrs.put(ATTR_EVENT_ID, s));
    return b.attributes(attrs).build();
  }

  /**
   * Converts a live-tail event to a record (see {@link #messageToRecord}) with the attribute {@code
   * aws.log.stream} and, for plain messages, {@code aws.log.group}.
   *
   * @param e the event
   * @return the record
   */
  public static LogRecord liveTailEventToRecord(LiveTailEvent e) {
    LogRecord decoded = decode(e.message(), e.timestamp());
    LogRecord.Builder b =
        decoded != null
            ? decoded.toBuilder()
            : plain(e.message(), e.timestamp(), e.ingestionTime().orElse(null));
    Map<String, Object> attrs =
        new LinkedHashMap<>(decoded != null ? decoded.attributes() : Map.of());
    e.logStreamName().ifPresent(s -> attrs.put(ATTR_LOG_STREAM, s));
    if (decoded == null) {
      e.logGroupIdentifier().ifPresent(s -> attrs.put(ATTR_LOG_GROUP, s));
    }
    return b.attributes(attrs).build();
  }

  // cursor: {"o":"a","t":token} or {"o":"d","e":sliceEnd,"w":width,"k":skip}
  private static String encode(Map<String, Object> cursor) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(Json.stringify(cursor).getBytes(StandardCharsets.UTF_8));
  }

  private static JsonNode decodeCursor(String text) {
    try {
      JsonNode n = Json.tree(Base64.getUrlDecoder().decode(text));
      String o = n.path("o").asText("");
      if (o.equals("a") && n.path("t").isTextual()) {
        return n;
      }
      if (o.equals("d")
          && n.path("e").isNumber()
          && n.path("w").isNumber()
          && n.path("k").isNumber()
          && n.path("w").asLong() > 0
          && n.path("k").asLong() >= 0) {
        return n;
      }
    } catch (RuntimeException e) {
      // fall through
    }
    throw new ValidationException("logging.search: invalid cursor");
  }

  @Override
  public LogSearchResult search(String query, SearchOptions options) {
    SearchOptions o = options == null ? SearchOptions.defaults() : options;
    int limit = o.limit();
    if (limit < 1 || limit > MAX_SEARCH_LIMIT) {
      throw new ValidationException(
          "logging.search: limit must be an integer from 1 to " + MAX_SEARCH_LIMIT);
    }
    String pattern = filterPattern(query);
    JsonNode cursor = o.cursor().map(CloudWatchLogsQueryClient::decodeCursor).orElse(null);
    String want = o.order() == SortOrder.ASC ? "a" : "d";
    if (cursor != null && !cursor.path("o").asText().equals(want)) {
      throw new ValidationException("logging.search: the cursor belongs to the other order");
    }
    return o.order() == SortOrder.ASC
        ? searchAsc(pattern, limit, o, cursor)
        : searchDesc(pattern, limit, o, cursor);
  }

  private LogSearchResult searchAsc(String pattern, int limit, SearchOptions o, JsonNode cursor) {
    List<LogRecord> records = new ArrayList<>();
    String token = cursor == null ? null : cursor.path("t").asText();
    // FilterLogEvents can return empty pages with a token: keep reading until limit or the end.
    do {
      FilterLogEventsOptions.Builder fo =
          FilterLogEventsOptions.builder()
              .group(group)
              .pattern(pattern)
              .pageSize(limit - records.size());
      o.start().ifPresent(fo::start);
      o.end().ifPresent(fo::end);
      FilterLogEventsPage page = client.filterLogEventsPage(fo.build(), token);
      for (FilteredLogEvent e : page.events()) {
        if (records.size() >= limit) {
          break;
        }
        records.add(filteredEventToRecord(e));
      }
      token = page.nextToken().orElse(null);
    } while (token != null && records.size() < limit);
    Optional<String> next =
        token == null ? Optional.empty() : Optional.of(encode(Map.of("o", "a", "t", token)));
    return new LogSearchResult(records, next, OptionalLong.empty());
  }

  private LogSearchResult searchDesc(String pattern, int limit, SearchOptions o, JsonNode cursor) {
    long endMs = o.end().orElseGet(Instant::now).toEpochMilli();
    long startMs = o.start().isPresent() ? o.start().get().toEpochMilli() : defaultStart(endMs);
    long sliceEnd = cursor == null ? endMs + 1 : cursor.path("e").asLong();
    long width = cursor == null ? SEARCH_FIRST_SLICE_MS : cursor.path("w").asLong();
    int skip = cursor == null ? 0 : cursor.path("k").asInt();
    List<LogRecord> records = new ArrayList<>();
    while (records.size() < limit && sliceEnd > startMs) {
      long sliceStart = Math.max(startMs, sliceEnd - width);
      List<FilteredLogEvent> events = scan(pattern, sliceStart, sliceEnd - 1);
      while (events.size() > SEARCH_SCAN_CAP && sliceEnd - sliceStart > 1000) {
        width = Math.max(1000, (sliceEnd - sliceStart) / 2);
        sliceStart = Math.max(startMs, sliceEnd - width);
        events = scan(pattern, sliceStart, sliceEnd - 1);
      }
      events.sort(NEWEST_FIRST);
      List<FilteredLogEvent> available =
          events.subList(Math.min(skip, events.size()), events.size());
      int take = Math.min(available.size(), limit - records.size());
      for (int i = 0; i < take; i++) {
        records.add(filteredEventToRecord(available.get(i)));
      }
      if (take < available.size()) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("o", "d");
        c.put("e", sliceEnd);
        c.put("w", width);
        c.put("k", skip + take);
        return new LogSearchResult(records, Optional.of(encode(c)), OptionalLong.empty());
      }
      sliceEnd = sliceStart;
      skip = 0;
      width = Math.min(width * 4, SEARCH_MAX_SLICE_MS);
    }
    Optional<String> next = Optional.empty();
    if (sliceEnd > startMs) {
      Map<String, Object> c = new LinkedHashMap<>();
      c.put("o", "d");
      c.put("e", sliceEnd);
      c.put("w", width);
      c.put("k", 0);
      next = Optional.of(encode(c));
    }
    return new LogSearchResult(records, next, OptionalLong.empty());
  }

  /** One hour before the group's creation time, else end minus 30 days. */
  private long defaultStart(long endMs) {
    if (createdMs == null) {
      client
          .getLogGroup(group)
          .flatMap(LogGroupInfo::creationTime)
          .ifPresent(t -> createdMs = t.toEpochMilli());
    }
    Long created = createdMs;
    return created == null
        ? endMs - DEFAULT_SEARCH_LOOKBACK_MS
        : Math.min(endMs, created - 3_600_000);
  }

  /** Reads up to SEARCH_SCAN_CAP + 1 events of [start, end] (inclusive). */
  private List<FilteredLogEvent> scan(String pattern, long start, long end) {
    List<FilteredLogEvent> out = new ArrayList<>();
    for (FilteredLogEvent e :
        client.filterLogEvents(
            FilterLogEventsOptions.builder()
                .group(group)
                .pattern(pattern)
                .start(Instant.ofEpochMilli(start))
                .end(Instant.ofEpochMilli(end))
                .limit(SEARCH_SCAN_CAP + 1)
                .build())) {
      out.add(e);
    }
    return out;
  }

  /**
   * Streams records with Live Tail (filter pattern built like {@link #search}). Errors and the end
   * of the session are reported to {@code options.onError}; {@code start} and {@code interval} do
   * not apply (the service pushes events).
   *
   * @param query search query
   * @param onRecord called for every record
   * @param options error callback
   * @return a handle that stops the session
   */
  @Override
  public TailHandle tail(String query, Consumer<LogRecord> onRecord, TailOptions options) {
    TailOptions o = options == null ? TailOptions.defaults() : options;
    AtomicBoolean stopped = new AtomicBoolean();
    AtomicReference<LiveTailHandle> session = new AtomicReference<>();
    Consumer<Throwable> report =
        e -> {
          if (!stopped.get()) {
            try {
              o.onError().accept(e);
            } catch (RuntimeException ignored) {
              // callbacks must never break the session
            }
          }
        };
    client
        .startLiveTailAsync(
            LiveTailRequest.builder().groups(List.of(group)).pattern(filterPattern(query)).build(),
            ev -> {
              if (!stopped.get()) {
                onRecord.accept(liveTailEventToRecord(ev));
              }
            },
            LiveTailOptions.builder().onError(report::accept).build())
        .whenComplete(
            (h, t) -> {
              if (t != null) {
                report.accept(HttpTransport.asSdkException(t, "logs.tail"));
                return;
              }
              session.set(h);
              if (stopped.get()) {
                h.close();
              }
            });
    return () -> {
      stopped.set(true);
      LiveTailHandle h = session.get();
      if (h != null) {
        h.close();
      }
    };
  }

  /**
   * Names of the streams of the group ({@code DescribeLogStreams}, all pages).
   *
   * @return the stream names
   */
  @Override
  public List<String> listStreams() {
    List<String> names = new ArrayList<>();
    for (LogStreamInfo s : client.describeLogStreams(group)) {
      names.add(s.name());
    }
    return names;
  }

  /**
   * Always throws: CloudWatch Logs has no label inventory.
   *
   * @return never returns
   * @throws UnsupportedException always
   */
  @Override
  public List<String> listLabels() {
    throw new UnsupportedException("logs.listLabels: CloudWatch Logs has no label inventory");
  }
}
