package codes.grow.packages.cloudwatchlogs;

/**
 * Logs Insights scan statistics (reported by AWS only).
 *
 * @param recordsMatched records that matched
 * @param recordsScanned records scanned
 * @param bytesScanned bytes scanned
 */
public record QueryStatistics(double recordsMatched, double recordsScanned, double bytesScanned) {}
