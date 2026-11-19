package codes.grow.packages.cloudwatchlogs;

/**
 * Result of {@code ensureLogStream}.
 *
 * @param created whether this call created the stream
 */
public record EnsureLogStreamResult(boolean created) {}
