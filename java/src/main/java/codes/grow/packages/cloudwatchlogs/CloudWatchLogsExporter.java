package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import codes.grow.packages.cloudwatchlogs.logging.ExportOptions;
import codes.grow.packages.cloudwatchlogs.logging.ExportResult;
import codes.grow.packages.cloudwatchlogs.logging.LogExporter;
import codes.grow.packages.cloudwatchlogs.logging.LogRecord;
import codes.grow.packages.cloudwatchlogs.logging.LogRecordJson;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Exports log records with {@code PutLogEvents} to one group/stream (SPEC section 4 {@code
 * LogExporter}, API.md section 3). The message of each event is the normalized LogRecord JSON
 * (redaction already applied by the log client); the event timestamp is the record timestamp in
 * milliseconds. The {@code gzip} option is ignored: PutLogEvents has no request compression in the
 * AWS API model.
 */
public final class CloudWatchLogsExporter implements LogExporter {
  private final CloudWatchLogsClient client;
  private final String group;
  private final String stream;
  private final boolean createGroup;

  CloudWatchLogsExporter(
      CloudWatchLogsClient client, String group, String stream, boolean createGroup) {
    this.client = client;
    this.group = group;
    this.stream = stream;
    this.createGroup = createGroup;
  }

  /**
   * Returns the log group.
   *
   * @return the group
   */
  public String group() {
    return group;
  }

  /**
   * Returns the log stream.
   *
   * @return the stream
   */
  public String stream() {
    return stream;
  }

  /**
   * Serialises a record to its normalized JSON message. When the message is larger than {@code
   * maxEventBytes}, {@link OversizePolicy#TRUNCATE} shortens the record body so the message stays
   * valid JSON (falling back to a plain cut only when the other fields alone are too large); {@link
   * OversizePolicy#DROP} and {@link OversizePolicy#ERROR} return null.
   *
   * @param record the record
   * @param maxEventBytes per-event limit
   * @param policy oversize policy
   * @return the message, or null when the record is left out
   */
  public static String recordToMessage(LogRecord record, int maxEventBytes, OversizePolicy policy) {
    String message = LogRecordJson.toJson(record);
    int size = LogEventBatching.utf8Bytes(message);
    if (size <= maxEventBytes) {
      return message;
    }
    if (policy != OversizePolicy.TRUNCATE) {
      return null;
    }
    String body = record.body();
    while (size > maxEventBytes && !body.isEmpty()) {
      int bodyBytes = LogEventBatching.utf8Bytes(body);
      int target = Math.max(0, bodyBytes - (size - maxEventBytes) - 64);
      String next = target < 48 ? "" : LogEventBatching.truncateMessage(body, target);
      body = next.length() < body.length() ? next : "";
      message = LogRecordJson.toJson(record.toBuilder().body(body).build());
      size = LogEventBatching.utf8Bytes(message);
    }
    return size <= maxEventBytes
        ? message
        : LogEventBatching.truncateMessage(message, maxEventBytes);
  }

  @Override
  public CompletableFuture<ExportResult> export(List<LogRecord> records, ExportOptions options) {
    CloudWatchLogsConfig cfg = client.config();
    List<LogEvent> events = new ArrayList<>();
    int oversized = 0;
    for (LogRecord r : records) {
      String message = recordToMessage(r, cfg.maxEventBytes(), cfg.oversizePolicy());
      if (message == null) {
        oversized++;
      } else {
        events.add(new LogEvent(r.timestamp(), message));
      }
    }
    int dropped = oversized;
    CompletableFuture<PutLogEventsResult> sent =
        events.isEmpty()
            ? CompletableFuture.completedFuture(new PutLogEventsResult(0, 0, 0, 0, 0, List.of()))
            : client.putLogEventsAsync(
                group,
                stream,
                events,
                PutLogEventsOptions.builder().autoCreate(true).createGroup(createGroup).build());
    return sent.thenApply(
        res -> {
          int rejected = dropped + res.rejected();
          if (rejected == 0) {
            return ExportResult.success(res.accepted());
          }
          return ExportResult.failure(
              res.accepted(),
              rejected,
              new ValidationException(
                  "logging.export: "
                      + rejected
                      + " records rejected (oversized, too old, too new or expired)"));
        });
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return CompletableFuture.completedFuture(null);
  }
}
