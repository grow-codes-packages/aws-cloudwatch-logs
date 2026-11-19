package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.List;

/**
 * Options of {@code filterLogEvents}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param group log group name or ARN (default {@code config.logGroup})
 * @param streams only these streams (1–100; not with streamPrefix)
 * @param streamPrefix only streams with this prefix (not with streams)
 * @param pattern CloudWatch filter pattern
 * @param start inclusive start
 * @param end inclusive end
 * @param pageSize events per request, 1–10 000
 * @param limit stop after this many events
 */
public record FilterLogEventsOptions(
    String group,
    List<String> streams,
    String streamPrefix,
    String pattern,
    Instant start,
    Instant end,
    Integer pageSize,
    Integer limit) {
  /**
   * Applies defaults.
   *
   * @param group log group name or ARN (default {@code config.logGroup})
   * @param streams only these streams (1–100; not with streamPrefix)
   * @param streamPrefix only streams with this prefix (not with streams)
   * @param pattern CloudWatch filter pattern
   * @param start inclusive start
   * @param end inclusive end
   * @param pageSize events per request, 1–10 000
   * @param limit stop after this many events
   */
  public FilterLogEventsOptions {
    streams = streams == null ? null : List.copyOf(streams);
  }

  /**
   * Options with every default.
   *
   * @return the options
   */
  public static FilterLogEventsOptions defaults() {
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
    b.group = group;
    b.streams = streams;
    b.streamPrefix = streamPrefix;
    b.pattern = pattern;
    b.start = start;
    b.end = end;
    b.pageSize = pageSize;
    b.limit = limit;
    return b;
  }

  /** Builder of {@link FilterLogEventsOptions}. */
  public static final class Builder {
    private String group;
    private List<String> streams;
    private String streamPrefix;
    private String pattern;
    private Instant start;
    private Instant end;
    private Integer pageSize;
    private Integer limit;

    private Builder() {}

    /**
     * Sets group: log group name or ARN (default {@code config.logGroup}).
     *
     * @param value the value
     * @return this builder
     */
    public Builder group(String value) {
      this.group = value;
      return this;
    }

    /**
     * Sets streams: only these streams (1–100; not with streamPrefix).
     *
     * @param value the value
     * @return this builder
     */
    public Builder streams(List<String> value) {
      this.streams = value;
      return this;
    }

    /**
     * Sets streamPrefix: only streams with this prefix (not with streams).
     *
     * @param value the value
     * @return this builder
     */
    public Builder streamPrefix(String value) {
      this.streamPrefix = value;
      return this;
    }

    /**
     * Sets pattern: CloudWatch filter pattern.
     *
     * @param value the value
     * @return this builder
     */
    public Builder pattern(String value) {
      this.pattern = value;
      return this;
    }

    /**
     * Sets start: inclusive start.
     *
     * @param value the value
     * @return this builder
     */
    public Builder start(Instant value) {
      this.start = value;
      return this;
    }

    /**
     * Sets end: inclusive end.
     *
     * @param value the value
     * @return this builder
     */
    public Builder end(Instant value) {
      this.end = value;
      return this;
    }

    /**
     * Sets pageSize: events per request, 1–10 000.
     *
     * @param value the value
     * @return this builder
     */
    public Builder pageSize(Integer value) {
      this.pageSize = value;
      return this;
    }

    /**
     * Sets limit: stop after this many events.
     *
     * @param value the value
     * @return this builder
     */
    public Builder limit(Integer value) {
      this.limit = value;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public FilterLogEventsOptions build() {
      return new FilterLogEventsOptions(
          group, streams, streamPrefix, pattern, start, end, pageSize, limit);
    }
  }
}
