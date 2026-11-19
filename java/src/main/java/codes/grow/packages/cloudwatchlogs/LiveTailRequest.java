package codes.grow.packages.cloudwatchlogs;

import java.util.List;

/**
 * What {@code startLiveTail} tails.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param groups log group names or ARNs (1–10); names are converted to ARNs
 * @param streams only these streams (only with one group; not with streamPrefixes)
 * @param streamPrefixes only streams with these prefixes (only with one group)
 * @param pattern CloudWatch filter pattern
 */
public record LiveTailRequest(
    List<String> groups, List<String> streams, List<String> streamPrefixes, String pattern) {
  /**
   * Applies defaults.
   *
   * @param groups log group names or ARNs (1–10); names are converted to ARNs
   * @param streams only these streams (only with one group; not with streamPrefixes)
   * @param streamPrefixes only streams with these prefixes (only with one group)
   * @param pattern CloudWatch filter pattern
   */
  public LiveTailRequest {
    groups = groups == null ? null : List.copyOf(groups);
    streams = streams == null ? null : List.copyOf(streams);
    streamPrefixes = streamPrefixes == null ? null : List.copyOf(streamPrefixes);
  }

  /**
   * Options with every default.
   *
   * @return the options
   */
  public static LiveTailRequest defaults() {
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
    b.streams = streams;
    b.streamPrefixes = streamPrefixes;
    b.pattern = pattern;
    return b;
  }

  /** Builder of {@link LiveTailRequest}. */
  public static final class Builder {
    private List<String> groups;
    private List<String> streams;
    private List<String> streamPrefixes;
    private String pattern;

    private Builder() {}

    /**
     * Sets groups: log group names or ARNs (1–10); names are converted to ARNs.
     *
     * @param value the value
     * @return this builder
     */
    public Builder groups(List<String> value) {
      this.groups = value;
      return this;
    }

    /**
     * Sets streams: only these streams (only with one group; not with streamPrefixes).
     *
     * @param value the value
     * @return this builder
     */
    public Builder streams(List<String> value) {
      this.streams = value;
      return this;
    }

    /**
     * Sets streamPrefixes: only streams with these prefixes (only with one group).
     *
     * @param value the value
     * @return this builder
     */
    public Builder streamPrefixes(List<String> value) {
      this.streamPrefixes = value;
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
     * Builds the options.
     *
     * @return the options
     */
    public LiveTailRequest build() {
      return new LiveTailRequest(groups, streams, streamPrefixes, pattern);
    }
  }
}
