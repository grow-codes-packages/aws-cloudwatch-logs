package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.Objects;

/**
 * One log event to send with {@code putLogEvents}.
 *
 * @param timestamp event time (sent as epoch milliseconds, sub-millisecond part truncated)
 * @param message message text
 */
public record LogEvent(Instant timestamp, String message) {
  /**
   * Validates the fields.
   *
   * @param timestamp event time
   * @param message message text
   */
  public LogEvent {
    Objects.requireNonNull(timestamp, "timestamp");
    Objects.requireNonNull(message, "message");
  }

  /**
   * An event stamped with the current time.
   *
   * @param message message text
   * @return the event
   */
  public static LogEvent of(String message) {
    return new LogEvent(Instant.now(), message);
  }

  /**
   * An event with an explicit time.
   *
   * @param timestamp event time
   * @param message message text
   * @return the event
   */
  public static LogEvent of(Instant timestamp, String message) {
    return new LogEvent(timestamp, message);
  }
}
