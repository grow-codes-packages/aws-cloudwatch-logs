package codes.grow.packages.cloudwatchlogs;

import java.util.OptionalInt;

/**
 * Events of one batch that the service did not store (indexes into the sorted batch). As returned
 * by AWS the end indexes are exclusive and the start index is inclusive; Moto 5.2.4 returns end
 * indexes one lower.
 *
 * @param batch index of the batch within the call (0-based)
 * @param tooOldLogEventEndIndex events before this index (exclusive end) were older than 14 days or
 *     the retention
 * @param tooNewLogEventStartIndex events from this index on were more than 2 hours in the future
 * @param expiredLogEventEndIndex events before this index (exclusive end) were older than the
 *     retention period
 */
public record RejectedLogEventsInfo(
    int batch,
    OptionalInt tooOldLogEventEndIndex,
    OptionalInt tooNewLogEventStartIndex,
    OptionalInt expiredLogEventEndIndex) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param batch index of the batch within the call (0-based)
   * @param tooOldLogEventEndIndex events before this index (exclusive end) were older than 14 days
   *     or the retention
   * @param tooNewLogEventStartIndex events from this index on were more than 2 hours in the future
   * @param expiredLogEventEndIndex events before this index (exclusive end) were older than the
   *     retention period
   */
  public RejectedLogEventsInfo {
    tooOldLogEventEndIndex =
        tooOldLogEventEndIndex == null ? OptionalInt.empty() : tooOldLogEventEndIndex;
    tooNewLogEventStartIndex =
        tooNewLogEventStartIndex == null ? OptionalInt.empty() : tooNewLogEventStartIndex;
    expiredLogEventEndIndex =
        expiredLogEventEndIndex == null ? OptionalInt.empty() : expiredLogEventEndIndex;
  }
}
