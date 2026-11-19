package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.Optional;

/**
 * One Logs Insights query ({@code describeQueries}).
 *
 * @param queryId query id
 * @param queryString query text
 * @param status status
 * @param createTime creation time
 * @param logGroupName log group
 */
public record QueryInfo(
    String queryId,
    String queryString,
    QueryStatus status,
    Optional<Instant> createTime,
    Optional<String> logGroupName) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param queryId query id
   * @param queryString query text
   * @param status status
   * @param createTime creation time
   * @param logGroupName log group
   */
  public QueryInfo {
    createTime = createTime == null ? Optional.empty() : createTime;
    logGroupName = logGroupName == null ? Optional.empty() : logGroupName;
  }
}
