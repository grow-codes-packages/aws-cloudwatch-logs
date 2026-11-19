package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.Optional;

/**
 * A log stream ({@code DescribeLogStreams}).
 *
 * @param name log stream name
 * @param arn log stream ARN
 * @param creationTime creation time
 * @param firstEventTime time of the first event
 * @param lastEventTime time of the last event (eventually consistent on AWS; do not rely on it)
 * @param lastIngestionTime last ingestion time
 */
public record LogStreamInfo(
    String name,
    Optional<String> arn,
    Optional<Instant> creationTime,
    Optional<Instant> firstEventTime,
    Optional<Instant> lastEventTime,
    Optional<Instant> lastIngestionTime) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param name log stream name
   * @param arn log stream ARN
   * @param creationTime creation time
   * @param firstEventTime time of the first event
   * @param lastEventTime time of the last event (eventually consistent on AWS; do not rely on it)
   * @param lastIngestionTime last ingestion time
   */
  public LogStreamInfo {
    arn = arn == null ? Optional.empty() : arn;
    creationTime = creationTime == null ? Optional.empty() : creationTime;
    firstEventTime = firstEventTime == null ? Optional.empty() : firstEventTime;
    lastEventTime = lastEventTime == null ? Optional.empty() : lastEventTime;
    lastIngestionTime = lastIngestionTime == null ? Optional.empty() : lastIngestionTime;
  }
}
