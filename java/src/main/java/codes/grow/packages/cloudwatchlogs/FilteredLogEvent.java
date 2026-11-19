package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.Optional;

/**
 * One event returned by {@code filterLogEvents}.
 *
 * @param timestamp event time
 * @param message message text
 * @param ingestionTime ingestion time
 * @param logStreamName stream that holds the event
 * @param eventId event id (unique within the group)
 */
public record FilteredLogEvent(
    Instant timestamp,
    String message,
    Optional<Instant> ingestionTime,
    Optional<String> logStreamName,
    Optional<String> eventId) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param timestamp event time
   * @param message message text
   * @param ingestionTime ingestion time
   * @param logStreamName stream that holds the event
   * @param eventId event id (unique within the group)
   */
  public FilteredLogEvent {
    ingestionTime = ingestionTime == null ? Optional.empty() : ingestionTime;
    logStreamName = logStreamName == null ? Optional.empty() : logStreamName;
    eventId = eventId == null ? Optional.empty() : eventId;
  }
}
