package codes.grow.packages.cloudwatchlogs;

/** Logs Insights query status. */
public enum QueryStatus {
  /** Waiting to run. */
  SCHEDULED("Scheduled"),
  /** Running (AWS may return partial rows). */
  RUNNING("Running"),
  /** Finished successfully. */
  COMPLETE("Complete"),
  /** Failed in the service. */
  FAILED("Failed"),
  /** Stopped with {@code StopQuery}. */
  CANCELLED("Cancelled"),
  /** Timed out in the service. */
  TIMEOUT("Timeout"),
  /** Unknown status. */
  UNKNOWN("Unknown");

  private final String wire;

  QueryStatus(String wire) {
    this.wire = wire;
  }

  /**
   * Returns the AWS name ({@code Complete}, {@code Running}, …).
   *
   * @return the wire value
   */
  public String wireValue() {
    return wire;
  }

  /**
   * Returns whether the query has ended (anything but scheduled or running).
   *
   * @return true when terminal
   */
  public boolean isTerminal() {
    return this != SCHEDULED && this != RUNNING;
  }

  /**
   * Parses an AWS status; unknown or null values give {@link #UNKNOWN}.
   *
   * @param value AWS status text
   * @return the status
   */
  public static QueryStatus parse(String value) {
    for (QueryStatus s : values()) {
      if (s.wire.equalsIgnoreCase(value == null ? "" : value)) {
        return s;
      }
    }
    return UNKNOWN;
  }
}
