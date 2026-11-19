package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.Optional;

/**
 * One event returned by {@code getLogEvents}.
 *
 * @param timestamp event time
 * @param message message text
 * @param ingestionTime ingestion time
 */
public record StreamLogEvent(Instant timestamp, String message, Optional<Instant> ingestionTime) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param timestamp event time
   * @param message message text
   * @param ingestionTime ingestion time
   */
  public StreamLogEvent {
    ingestionTime = ingestionTime == null ? Optional.empty() : ingestionTime;
  }
}
