package codes.grow.packages.cloudwatchlogs;

import java.util.List;
import java.util.Optional;

/**
 * One {@code FilterLogEvents} page.
 *
 * @param events events ordered by timestamp, then event id
 * @param nextToken token of the next page (empty at the end; AWS may return empty pages that still
 *     carry a token)
 */
public record FilterLogEventsPage(List<FilteredLogEvent> events, Optional<String> nextToken) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param events events ordered by timestamp, then event id
   * @param nextToken token of the next page (empty at the end; AWS may return empty pages that
   *     still carry a token)
   */
  public FilterLogEventsPage {
    events = events == null ? List.of() : List.copyOf(events);
    nextToken = nextToken == null ? Optional.empty() : nextToken;
  }
}
