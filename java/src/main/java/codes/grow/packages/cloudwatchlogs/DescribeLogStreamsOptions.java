package codes.grow.packages.cloudwatchlogs;

/**
 * Options of {@code describeLogStreams}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param prefix name prefix (not allowed with {@code orderBy LAST_EVENT_TIME})
 * @param orderBy sort key (default {@code LOG_STREAM_NAME})
 * @param descending descending order (default false)
 * @param pageSize streams per request, 1–50 (default 50)
 * @param limit stop after this many streams
 */
public record DescribeLogStreamsOptions(
    String prefix, LogStreamOrder orderBy, Boolean descending, Integer pageSize, Integer limit) {
  /**
   * Options with every default.
   *
   * @return the options
   */
  public static DescribeLogStreamsOptions defaults() {
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
    b.prefix = prefix;
    b.orderBy = orderBy;
    b.descending = descending;
    b.pageSize = pageSize;
    b.limit = limit;
    return b;
  }

  /** Builder of {@link DescribeLogStreamsOptions}. */
  public static final class Builder {
    private String prefix;
    private LogStreamOrder orderBy;
    private Boolean descending;
    private Integer pageSize;
    private Integer limit;

    private Builder() {}

    /**
     * Sets prefix: name prefix (not allowed with {@code orderBy LAST_EVENT_TIME}).
     *
     * @param value the value
     * @return this builder
     */
    public Builder prefix(String value) {
      this.prefix = value;
      return this;
    }

    /**
     * Sets orderBy: sort key (default {@code LOG_STREAM_NAME}).
     *
     * @param value the value
     * @return this builder
     */
    public Builder orderBy(LogStreamOrder value) {
      this.orderBy = value;
      return this;
    }

    /**
     * Sets descending: descending order (default false).
     *
     * @param value the value
     * @return this builder
     */
    public Builder descending(Boolean value) {
      this.descending = value;
      return this;
    }

    /**
     * Sets pageSize: streams per request, 1–50 (default 50).
     *
     * @param value the value
     * @return this builder
     */
    public Builder pageSize(Integer value) {
      this.pageSize = value;
      return this;
    }

    /**
     * Sets limit: stop after this many streams.
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
    public DescribeLogStreamsOptions build() {
      return new DescribeLogStreamsOptions(prefix, orderBy, descending, pageSize, limit);
    }
  }
}
