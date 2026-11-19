package codes.grow.packages.cloudwatchlogs;

/**
 * Result of {@code ensureLogGroup}.
 *
 * @param created whether this call created the group
 * @param arn group ARN without {@code :*}
 */
public record EnsureLogGroupResult(boolean created, String arn) {}
