package codes.grow.packages.cloudwatchlogs;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Options of {@code startQuery} and {@code query} (Logs Insights).
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param groups log group names or ARNs (1–50; default {@code [config.logGroup]})
 * @param start start of the range (default one hour before end)
 * @param end end of the range (default now)
 * @param limit maximum rows (1–10 000)
 * @param timeout {@code query} gives up after this long (default 60 s); the query is stopped
 *     best-effort
 * @param pollInterval first poll interval of {@code query} (default 1 s), doubled up to 5 s
 * @param retryOnQueryLimit StartQuery attempts of {@code query} on {@code LimitExceededException}
 *     (default 3)
 */
public record QueryOptions(
    List<String> groups,
    Instant start,
    Instant end,
    Integer limit,
    Duration timeout,
    Duration pollInterval,
    Integer retryOnQueryLimit) {
  /**
   * Applies defaults.
   *
   * @param groups log group names or ARNs (1–50; default {@code [config.logGroup]})
   * @param start start of the range (default one hour before end)
   * @param end end of the range (default now)
   * @param limit maximum rows (1–10 000)
   * @param timeout {@code query} gives up after this long (default 60 s); the query is stopped
   *     best-effort
   * @param pollInterval first poll interval of {@code query} (default 1 s), doubled up to 5 s
   * @param retryOnQueryLimit StartQuery attempts of {@code query} on {@code LimitExceededException}
   *     (default 3)
   */
  public QueryOptions {
    groups = groups == null ? null : List.copyOf(groups);
    timeout = timeout == null ? Duration.ofSeconds(60) : timeout;
    pollInterval = pollInterval == null ? Duration.ofSeconds(1) : pollInterval;
    retryOnQueryLimit = retryOnQueryLimit == null ? 3 : retryOnQueryLimit;
  }

  /**
   * Options with every default.
   *
   * @return the options
   */
  public static QueryOptions defaults() {
    return builder().build();
  }

  /**
   * Starts a builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a builder holding these values.
   *
   * @return a new builder
   */
  public Builder toBuilder() {
    Builder b = new Builder();
    b.groups = groups;
    b.start = start;
    b.end = end;
    b.limit = limit;
    b.timeout = timeout;
    b.pollInterval = pollInterval;
    b.retryOnQueryLimit = retryOnQueryLimit;
    return b;
  }

  /** Builder of {@link QueryOptions}. */
  public static final class Builder {
    private List<String> groups;
    private Instant start;
    private Instant end;
    private Integer limit;
    private Duration timeout;
    private Duration pollInterval;
    private Integer retryOnQueryLimit;

    private Builder() {}

    /**
     * Sets groups: log group names or ARNs (1–50; default {@code [config.logGroup]}).
     *
     * @param value the value
     * @return this builder
     */
    public Builder groups(List<String> value) {
      this.groups = value;
      return this;
    }

    /**
     * Sets start: start of the range (default one hour before end).
     *
     * @param value the value
     * @return this builder
     */
    public Builder start(Instant value) {
      this.start = value;
      return this;
    }

    /**
     * Sets end: end of the range (default now).
     *
     * @param value the value
     * @return this builder
     */
    public Builder end(Instant value) {
      this.end = value;
      return this;
    }

    /**
     * Sets limit: maximum rows (1–10 000).
     *
     * @param value the value
     * @return this builder
     */
    public Builder limit(Integer value) {
      this.limit = value;
      return this;
    }

    /**
     * Sets timeout: {@code query} gives up after this long (default 60 s); the query is stopped
     * best-effort.
     *
     * @param value the value
     * @return this builder
     */
    public Builder timeout(Duration value) {
      this.timeout = value;
      return this;
    }

    /**
     * Sets pollInterval: first poll interval of {@code query} (default 1 s), doubled up to 5 s.
     *
     * @param value the value
     * @return this builder
     */
    public Builder pollInterval(Duration value) {
      this.pollInterval = value;
      return this;
    }

    /**
     * Sets retryOnQueryLimit: StartQuery attempts of {@code query} on {@code
     * LimitExceededException} (default 3).
     *
     * @param value the value
     * @return this builder
     */
    public Builder retryOnQueryLimit(Integer value) {
      this.retryOnQueryLimit = value;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public QueryOptions build() {
      return new QueryOptions(groups, start, end, limit, timeout, pollInterval, retryOnQueryLimit);
    }
  }
}
