package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.logging.BatchOptions;
import codes.grow.packages.cloudwatchlogs.logging.RedactionProcessor;
import java.util.Map;

/**
 * Options of {@code CloudWatchLogsClient.logClient}.
 *
 * @param group log group (null: {@code config.logGroup}, required)
 * @param stream log stream (null: {@code config.logStream}; created on first use)
 * @param createGroup create a missing log group on the first export (default false)
 * @param batchOptions batching options (null: {@link BatchOptions#defaults()})
 * @param resource resource attributes ({@code service.name}, …) added to every record
 * @param redaction redaction applied before records leave the process (null: defaults)
 */
public record CloudWatchLogClientOptions(
    String group,
    String stream,
    boolean createGroup,
    BatchOptions batchOptions,
    Map<String, Object> resource,
    RedactionProcessor redaction) {
  /**
   * Applies defaults.
   *
   * @param group group
   * @param stream stream
   * @param createGroup create the group
   * @param batchOptions batching options
   * @param resource resource attributes
   * @param redaction redaction
   */
  public CloudWatchLogClientOptions {
    batchOptions = batchOptions == null ? BatchOptions.defaults() : batchOptions;
    resource = resource == null ? Map.of() : Map.copyOf(resource);
  }

  /**
   * Options with every default.
   *
   * @return the options
   */
  public static CloudWatchLogClientOptions defaults() {
    return new CloudWatchLogClientOptions(null, null, false, null, null, null);
  }

  /**
   * Returns a copy with another group and stream.
   *
   * @param groupName group (null: default)
   * @param streamName stream (null: default)
   * @return the copy
   */
  public CloudWatchLogClientOptions withTarget(String groupName, String streamName) {
    return new CloudWatchLogClientOptions(
        groupName, streamName, createGroup, batchOptions, resource, redaction);
  }

  /**
   * Returns a copy that creates a missing group.
   *
   * @param value flag
   * @return the copy
   */
  public CloudWatchLogClientOptions withCreateGroup(boolean value) {
    return new CloudWatchLogClientOptions(group, stream, value, batchOptions, resource, redaction);
  }

  /**
   * Returns a copy with other batching options.
   *
   * @param value options
   * @return the copy
   */
  public CloudWatchLogClientOptions withBatchOptions(BatchOptions value) {
    return new CloudWatchLogClientOptions(group, stream, createGroup, value, resource, redaction);
  }

  /**
   * Returns a copy with other resource attributes.
   *
   * @param value attributes
   * @return the copy
   */
  public CloudWatchLogClientOptions withResource(Map<String, Object> value) {
    return new CloudWatchLogClientOptions(
        group, stream, createGroup, batchOptions, value, redaction);
  }

  /**
   * Returns a copy with another redaction processor.
   *
   * @param value processor
   * @return the copy
   */
  public CloudWatchLogClientOptions withRedaction(RedactionProcessor value) {
    return new CloudWatchLogClientOptions(
        group, stream, createGroup, batchOptions, resource, value);
  }
}
