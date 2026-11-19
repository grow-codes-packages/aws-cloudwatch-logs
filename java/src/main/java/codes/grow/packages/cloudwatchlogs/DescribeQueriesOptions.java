package codes.grow.packages.cloudwatchlogs;

/**
 * Options of {@code describeQueries}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param group only queries of this log group
 * @param status only queries with this status
 * @param maxResults maximum queries (1–1000)
 */
public record DescribeQueriesOptions(String group, QueryStatus status, Integer maxResults) {
  /**
   * Options with every default.
   *
   * @return the options
   */
  public static DescribeQueriesOptions defaults() {
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
    b.status = status;
    b.maxResults = maxResults;
    return b;
  }

  /** Builder of {@link DescribeQueriesOptions}. */
  public static final class Builder {
    private String group;
    private QueryStatus status;
    private Integer maxResults;

    private Builder() {}

    /**
     * Sets group: only queries of this log group.
     *
     * @param value the value
     * @return this builder
     */
    public Builder group(String value) {
      this.group = value;
      return this;
    }

    /**
     * Sets status: only queries with this status.
     *
     * @param value the value
     * @return this builder
     */
    public Builder status(QueryStatus value) {
      this.status = value;
      return this;
    }

    /**
     * Sets maxResults: maximum queries (1–1000).
     *
     * @param value the value
     * @return this builder
     */
    public Builder maxResults(Integer value) {
      this.maxResults = value;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public DescribeQueriesOptions build() {
      return new DescribeQueriesOptions(group, status, maxResults);
    }
  }
}
