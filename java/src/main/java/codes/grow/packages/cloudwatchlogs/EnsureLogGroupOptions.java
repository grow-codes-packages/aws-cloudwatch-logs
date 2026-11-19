package codes.grow.packages.cloudwatchlogs;

import java.util.Map;

/**
 * Options of {@code ensureLogGroup}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param retentionDays retention in days (one of {@code RETENTION_DAYS}); 0 = never expire; null =
 *     leave as is
 * @param kmsKeyId KMS key for a new group; an existing group with a different key raises {@code
 *     ConflictException}
 * @param tags tags to add (existing tags are kept; values of the same keys are overwritten)
 */
public record EnsureLogGroupOptions(
    Integer retentionDays, String kmsKeyId, Map<String, String> tags) {
  /**
   * Applies defaults.
   *
   * @param retentionDays retention in days (one of {@code RETENTION_DAYS}); 0 = never expire; null
   *     = leave as is
   * @param kmsKeyId KMS key for a new group; an existing group with a different key raises {@code
   *     ConflictException}
   * @param tags tags to add (existing tags are kept; values of the same keys are overwritten)
   */
  public EnsureLogGroupOptions {
    tags = tags == null ? Map.of() : Map.copyOf(tags);
  }

  /**
   * Options with every default.
   *
   * @return the options
   */
  public static EnsureLogGroupOptions defaults() {
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
    b.retentionDays = retentionDays;
    b.kmsKeyId = kmsKeyId;
    b.tags = tags;
    return b;
  }

  /** Builder of {@link EnsureLogGroupOptions}. */
  public static final class Builder {
    private Integer retentionDays;
    private String kmsKeyId;
    private Map<String, String> tags;

    private Builder() {}

    /**
     * Sets retentionDays: retention in days (one of {@code RETENTION_DAYS}); 0 = never expire; null
     * = leave as is.
     *
     * @param value the value
     * @return this builder
     */
    public Builder retentionDays(Integer value) {
      this.retentionDays = value;
      return this;
    }

    /**
     * Sets kmsKeyId: KMS key for a new group; an existing group with a different key raises {@code
     * ConflictException}.
     *
     * @param value the value
     * @return this builder
     */
    public Builder kmsKeyId(String value) {
      this.kmsKeyId = value;
      return this;
    }

    /**
     * Sets tags: tags to add (existing tags are kept; values of the same keys are overwritten).
     *
     * @param value the value
     * @return this builder
     */
    public Builder tags(Map<String, String> value) {
      this.tags = value;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public EnsureLogGroupOptions build() {
      return new EnsureLogGroupOptions(retentionDays, kmsKeyId, tags);
    }
  }
}
