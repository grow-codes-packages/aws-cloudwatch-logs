package codes.grow.packages.cloudwatchlogs;

/**
 * Options of {@code describeLogGroups}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param prefix name prefix
 * @param pageSize groups per request, 1–50 (default 50)
 * @param limit stop after this many groups
 */
public record DescribeLogGroupsOptions(String prefix, Integer pageSize, Integer limit) {
  /**
   * Options with every default.
   *
   * @return the options
   */
  public static DescribeLogGroupsOptions defaults() {
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
    b.pageSize = pageSize;
    b.limit = limit;
    return b;
  }

  /** Builder of {@link DescribeLogGroupsOptions}. */
  public static final class Builder {
    private String prefix;
    private Integer pageSize;
    private Integer limit;

    private Builder() {}

    /**
     * Sets prefix: name prefix.
     *
     * @param value the value
     * @return this builder
     */
    public Builder prefix(String value) {
      this.prefix = value;
      return this;
    }

    /**
     * Sets pageSize: groups per request, 1–50 (default 50).
     *
     * @param value the value
     * @return this builder
     */
    public Builder pageSize(Integer value) {
      this.pageSize = value;
      return this;
    }

    /**
     * Sets limit: stop after this many groups.
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
    public DescribeLogGroupsOptions build() {
      return new DescribeLogGroupsOptions(prefix, pageSize, limit);
    }
  }
}
