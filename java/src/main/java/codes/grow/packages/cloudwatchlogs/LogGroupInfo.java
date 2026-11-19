package codes.grow.packages.cloudwatchlogs;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * A log group ({@code DescribeLogGroups}).
 *
 * @param name log group name
 * @param arn log group ARN without the trailing {@code :*}
 * @param creationTime creation time
 * @param retentionDays retention in days (empty = never expire)
 * @param storedBytes stored bytes (approximate, eventually consistent)
 * @param kmsKeyId KMS key ARN used to encrypt the group
 * @param metricFilterCount number of metric filters
 * @param logGroupClass {@code STANDARD}, {@code INFREQUENT_ACCESS}, …
 */
public record LogGroupInfo(
    String name,
    String arn,
    Optional<Instant> creationTime,
    OptionalInt retentionDays,
    OptionalLong storedBytes,
    Optional<String> kmsKeyId,
    OptionalInt metricFilterCount,
    Optional<String> logGroupClass) {
  /**
   * Normalises absent values (null lists become empty, null optionals empty).
   *
   * @param name log group name
   * @param arn log group ARN without the trailing {@code :*}
   * @param creationTime creation time
   * @param retentionDays retention in days (empty = never expire)
   * @param storedBytes stored bytes (approximate, eventually consistent)
   * @param kmsKeyId KMS key ARN used to encrypt the group
   * @param metricFilterCount number of metric filters
   * @param logGroupClass {@code STANDARD}, {@code INFREQUENT_ACCESS}, …
   */
  public LogGroupInfo {
    creationTime = creationTime == null ? Optional.empty() : creationTime;
    retentionDays = retentionDays == null ? OptionalInt.empty() : retentionDays;
    storedBytes = storedBytes == null ? OptionalLong.empty() : storedBytes;
    kmsKeyId = kmsKeyId == null ? Optional.empty() : kmsKeyId;
    metricFilterCount = metricFilterCount == null ? OptionalInt.empty() : metricFilterCount;
    logGroupClass = logGroupClass == null ? Optional.empty() : logGroupClass;
  }
}
