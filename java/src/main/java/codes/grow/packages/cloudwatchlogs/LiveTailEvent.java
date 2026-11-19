package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.Optional;

/**
 * One live-tail event.
 *
 * @param timestamp event time
 * @param ingestionTime ingestion time
 * @param message message text
 * @param logStreamName stream
 * @param logGroupIdentifier log group ARN
 */
public record LiveTailEvent(
    Instant timestamp,
    Optional<Instant> ingestionTime,
    String message,
    Optional<String> logStreamName,
    Optional<String> logGroupIdentifier) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param timestamp event time
   * @param ingestionTime ingestion time
   * @param message message text
   * @param logStreamName stream
   * @param logGroupIdentifier log group ARN
   */
  public LiveTailEvent {
    ingestionTime = ingestionTime == null ? Optional.empty() : ingestionTime;
    logStreamName = logStreamName == null ? Optional.empty() : logStreamName;
    logGroupIdentifier = logGroupIdentifier == null ? Optional.empty() : logGroupIdentifier;
  }
}
