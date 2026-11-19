package codes.grow.packages.cloudwatchlogs;

import java.util.List;

/**
 * Result of {@code putLogEvents}.
 *
 * @param accepted events the service accepted
 * @param rejected events the service rejected (too old, too new, expired)
 * @param truncated events truncated by the size policy before sending
 * @param dropped events dropped by the size policy ({@link OversizePolicy#DROP})
 * @param batches PutLogEvents calls made
 * @param rejectedInfo details of rejected events per batch
 */
public record PutLogEventsResult(
    int accepted,
    int rejected,
    int truncated,
    int dropped,
    int batches,
    List<RejectedLogEventsInfo> rejectedInfo) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param accepted events the service accepted
   * @param rejected events the service rejected (too old, too new, expired)
   * @param truncated events truncated by the size policy before sending
   * @param dropped events dropped by the size policy ({@link OversizePolicy#DROP})
   * @param batches PutLogEvents calls made
   * @param rejectedInfo details of rejected events per batch
   */
  public PutLogEventsResult {
    rejectedInfo = rejectedInfo == null ? List.of() : List.copyOf(rejectedInfo);
  }
}
