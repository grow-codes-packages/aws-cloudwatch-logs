package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;

/**
 * Options of {@code getLogEvents}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param startFromHead true: oldest first; false (default): newest page first (ascending inside a
 *     page)
 * @param start inclusive start
 * @param end exclusive end
 * @param pageSize events per request, 1–10 000
 * @param limit stop after this many events
 */
public record GetLogEventsOptions(
    Boolean startFromHead, Instant start, Instant end, Integer pageSize, Integer limit) {
  /**
   * Options with every default.
   *
   * @return the options
   */
  public static GetLogEventsOptions defaults() {
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
    b.startFromHead = startFromHead;
    b.start = start;
    b.end = end;
    b.pageSize = pageSize;
    b.limit = limit;
    return b;
  }

  /** Builder of {@link GetLogEventsOptions}. */
  public static final class Builder {
    private Boolean startFromHead;
    private Instant start;
    private Instant end;
    private Integer pageSize;
    private Integer limit;

    private Builder() {}

    /**
     * Sets startFromHead: true: oldest first; false (default): newest page first (ascending inside
     * a page).
     *
     * @param value the value
     * @return this builder
     */
    public Builder startFromHead(Boolean value) {
      this.startFromHead = value;
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
     * Sets end: exclusive end.
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
    public GetLogEventsOptions build() {
      return new GetLogEventsOptions(startFromHead, start, end, pageSize, limit);
    }
  }
}
