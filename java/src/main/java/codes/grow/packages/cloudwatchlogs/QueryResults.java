package codes.grow.packages.cloudwatchlogs;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Result of {@code getQueryResults} / {@code query}.
 *
 * @param queryId query id
 * @param status query status
 * @param rows result rows: field name to value ({@code @timestamp}, {@code @message}, {@code @ptr},
 *     …)
 * @param statistics scan statistics when the service reports them
 */
public record QueryResults(
    String queryId,
    QueryStatus status,
    List<Map<String, String>> rows,
    Optional<QueryStatistics> statistics) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param queryId query id
   * @param status query status
   * @param rows result rows: field name to value ({@code @timestamp}, {@code @message},
   *     {@code @ptr}, …)
   * @param statistics scan statistics when the service reports them
   */
  public QueryResults {
    rows = rows == null ? List.of() : List.copyOf(rows);
    statistics = statistics == null ? Optional.empty() : statistics;
  }
}
