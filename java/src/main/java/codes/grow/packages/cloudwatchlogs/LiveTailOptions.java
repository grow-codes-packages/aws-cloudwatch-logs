package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import java.util.function.Consumer;

/**
 * Callbacks of {@code startLiveTail}.
 *
 * <p>Build with {@link #builder()}; unset values are {@code null} and take the documented default.
 *
 * @param onError called with the error that ends the session (mapped to the SDK exceptions)
 * @param onUpdate called for every {@code sessionUpdate} frame (also empty keep-alive updates)
 */
public record LiveTailOptions(
    Consumer<CloudWatchLogsException> onError, Consumer<LiveTailUpdate> onUpdate) {
  /**
   * Options with every default.
   *
   * @return the options
   */
  public static LiveTailOptions defaults() {
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
    b.onError = onError;
    b.onUpdate = onUpdate;
    return b;
  }

  /** Builder of {@link LiveTailOptions}. */
  public static final class Builder {
    private Consumer<CloudWatchLogsException> onError;
    private Consumer<LiveTailUpdate> onUpdate;

    private Builder() {}

    /**
     * Sets onError: called with the error that ends the session (mapped to the SDK exceptions).
     *
     * @param value the value
     * @return this builder
     */
    public Builder onError(Consumer<CloudWatchLogsException> value) {
      this.onError = value;
      return this;
    }

    /**
     * Sets onUpdate: called for every {@code sessionUpdate} frame (also empty keep-alive updates).
     *
     * @param value the value
     * @return this builder
     */
    public Builder onUpdate(Consumer<LiveTailUpdate> value) {
      this.onUpdate = value;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public LiveTailOptions build() {
      return new LiveTailOptions(onError, onUpdate);
    }
  }
}
