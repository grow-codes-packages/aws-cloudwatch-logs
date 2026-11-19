package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Client-side PutLogEvents batching (DECISIONS 5, API.md section 2.2).
 *
 * <p>CloudWatch Logs rejects a PutLogEvents call with more than 10 000 events, more than 1 048 576
 * bytes (sum of UTF-8 message bytes + 26 per event), events spanning more than 24 hours or events
 * out of chronological order. These limits are enforced here, before anything is sent.
 */
public final class LogEventBatching {
  /** Most events in one PutLogEvents call. */
  public static final int MAX_EVENTS_PER_BATCH = 10_000;

  /** Most bytes in one PutLogEvents call (sum of UTF-8 message bytes + 26 per event). */
  public static final int MAX_BATCH_BYTES = 1_048_576;

  /** Bytes AWS adds to every event when it checks the batch size. */
  public static final int EVENT_OVERHEAD_BYTES = 26;

  /** Longest time span (last minus first timestamp) of one batch: 24 hours in milliseconds. */
  public static final long MAX_BATCH_SPAN_MS = 24L * 60 * 60 * 1000;

  private LogEventBatching() {}

  /**
   * An event as sent to AWS.
   *
   * @param timestamp epoch milliseconds
   * @param message message text
   */
  public record WireEvent(long timestamp, String message) {}

  /**
   * Result of {@link #plan}.
   *
   * @param batches batches in send order; each is sorted by timestamp and within every limit
   * @param truncated events that were truncated
   * @param dropped events that were dropped ({@link OversizePolicy#DROP})
   */
  public record BatchPlan(List<List<WireEvent>> batches, int truncated, int dropped) {
    /**
     * Copies the batches.
     *
     * @param batches batches
     * @param truncated truncated count
     * @param dropped dropped count
     */
    public BatchPlan {
      batches = List.copyOf(batches);
    }
  }

  /**
   * Returns the UTF-8 size of a string in bytes.
   *
   * @param text text
   * @return the size
   */
  public static int utf8Bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8).length;
  }

  private static String cutUtf8(byte[] buf, int maxBytes) {
    int end = Math.min(buf.length, Math.max(0, maxBytes));
    while (end > 0 && end < buf.length && (buf[end] & 0xc0) == 0x80) {
      end--;
    }
    return new String(buf, 0, end, StandardCharsets.UTF_8);
  }

  private static String marker(int removed) {
    return " …[truncated " + removed + " bytes]";
  }

  /**
   * Truncates {@code message} on a UTF-8 character boundary so the result, including the appended
   * {@code " …[truncated N bytes]"} marker, is at most {@code maxBytes} UTF-8 bytes. {@code N} is
   * the number of bytes removed from the original. Returns {@code message} when it already fits.
   *
   * @param message the message
   * @param maxBytes the limit in bytes
   * @return the truncated message
   */
  public static String truncateMessage(String message, int maxBytes) {
    byte[] buf = message.getBytes(StandardCharsets.UTF_8);
    if (buf.length <= maxBytes) {
      return message;
    }
    // `removed` only grows (a longer marker leaves less room) and is bounded, so this converges.
    int removed = buf.length - maxBytes;
    while (true) {
      String kept = cutUtf8(buf, maxBytes - utf8Bytes(marker(removed)));
      int actual = buf.length - utf8Bytes(kept);
      if (actual == removed) {
        String out = kept + marker(actual);
        return utf8Bytes(out) <= maxBytes
            ? out
            : cutUtf8(out.getBytes(StandardCharsets.UTF_8), maxBytes);
      }
      removed = actual;
    }
  }

  private record Prepared(long timestamp, String message, int bytes) {}

  /**
   * Applies the per-event size policy, sorts events by timestamp (stable) and splits them into
   * batches that respect every PutLogEvents limit.
   *
   * @param events events in any order
   * @param maxEventBytes per-event limit (1 to 1 048 550)
   * @param policy what to do with larger events
   * @return the plan
   * @throws ValidationException for a null event or an oversized event with {@link
   *     OversizePolicy#ERROR}
   */
  public static BatchPlan plan(List<LogEvent> events, int maxEventBytes, OversizePolicy policy) {
    if (maxEventBytes < 1 || maxEventBytes > CloudWatchLogsConfig.MAX_EVENT_BYTES_HARD_CAP) {
      throw new ValidationException(
          "putLogEvents: maxEventBytes must be an integer from 1 to "
              + CloudWatchLogsConfig.MAX_EVENT_BYTES_HARD_CAP);
    }
    int truncated = 0;
    int dropped = 0;
    List<Prepared> prepared = new ArrayList<>(events.size());
    for (int i = 0; i < events.size(); i++) {
      LogEvent event = events.get(i);
      if (event == null) {
        throw new ValidationException("putLogEvents: event " + i + " is null");
      }
      String message = event.message();
      int bytes = utf8Bytes(message);
      if (bytes > maxEventBytes) {
        if (policy == OversizePolicy.ERROR) {
          throw new ValidationException(
              "putLogEvents: event "
                  + i
                  + " is "
                  + bytes
                  + " bytes, more than maxEventBytes "
                  + maxEventBytes);
        }
        if (policy == OversizePolicy.DROP) {
          dropped++;
          continue;
        }
        message = truncateMessage(message, maxEventBytes);
        bytes = utf8Bytes(message);
        truncated++;
      }
      prepared.add(new Prepared(event.timestamp().toEpochMilli(), message, bytes));
    }
    prepared.sort(Comparator.comparingLong(Prepared::timestamp));
    List<List<WireEvent>> batches = new ArrayList<>();
    List<WireEvent> current = new ArrayList<>();
    long currentBytes = 0;
    long first = 0;
    for (Prepared event : prepared) {
      int size = event.bytes() + EVENT_OVERHEAD_BYTES;
      if (!current.isEmpty()
          && (current.size() >= MAX_EVENTS_PER_BATCH
              || currentBytes + size > MAX_BATCH_BYTES
              || event.timestamp() - first > MAX_BATCH_SPAN_MS)) {
        batches.add(List.copyOf(current));
        current = new ArrayList<>();
        currentBytes = 0;
      }
      if (current.isEmpty()) {
        first = event.timestamp();
      }
      current.add(new WireEvent(event.timestamp(), event.message()));
      currentBytes += size;
    }
    if (!current.isEmpty()) {
      batches.add(List.copyOf(current));
    }
    return new BatchPlan(batches, truncated, dropped);
  }
}
