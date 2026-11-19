package codes.grow.packages.cloudwatchlogs;

/**
 * Metadata of one live-tail {@code sessionUpdate} frame (also empty keep-alive updates).
 *
 * @param sampled whether the service sampled the results (above ~500 events/s)
 * @param count events in the update
 */
public record LiveTailUpdate(boolean sampled, int count) {}
