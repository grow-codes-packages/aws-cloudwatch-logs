package codes.grow.packages.cloudwatchlogs;

/**
 * Options of {@code putLogEvents}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param autoCreate create a missing log stream and retry once (default {@code config.autoCreate},
 *     true)
 * @param createGroup with autoCreate, also create a missing log group (default false)
 * @param maxEventBytes per-event limit (default {@code config.maxEventBytes})
 * @param oversizePolicy oversize policy (default {@code config.oversizePolicy})
 */
public record PutLogEventsOptions(
    Boolean autoCreate, Boolean createGroup, Integer maxEventBytes, OversizePolicy oversizePolicy) {
  /**
   * Options with every default.
   *
   * @return the options
   */
  public static PutLogEventsOptions defaults() {
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
    b.autoCreate = autoCreate;
    b.createGroup = createGroup;
    b.maxEventBytes = maxEventBytes;
    b.oversizePolicy = oversizePolicy;
    return b;
  }

  /** Builder of {@link PutLogEventsOptions}. */
  public static final class Builder {
    private Boolean autoCreate;
    private Boolean createGroup;
    private Integer maxEventBytes;
    private OversizePolicy oversizePolicy;

    private Builder() {}

    /**
     * Sets autoCreate: create a missing log stream and retry once (default {@code
     * config.autoCreate}, true).
     *
     * @param value the value
     * @return this builder
     */
    public Builder autoCreate(Boolean value) {
      this.autoCreate = value;
      return this;
    }

    /**
     * Sets createGroup: with autoCreate, also create a missing log group (default false).
     *
     * @param value the value
     * @return this builder
     */
    public Builder createGroup(Boolean value) {
      this.createGroup = value;
      return this;
    }

    /**
     * Sets maxEventBytes: per-event limit (default {@code config.maxEventBytes}).
     *
     * @param value the value
     * @return this builder
     */
    public Builder maxEventBytes(Integer value) {
      this.maxEventBytes = value;
      return this;
    }

    /**
     * Sets oversizePolicy: oversize policy (default {@code config.oversizePolicy}).
     *
     * @param value the value
     * @return this builder
     */
    public Builder oversizePolicy(OversizePolicy value) {
      this.oversizePolicy = value;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public PutLogEventsOptions build() {
      return new PutLogEventsOptions(autoCreate, createGroup, maxEventBytes, oversizePolicy);
    }
  }
}
