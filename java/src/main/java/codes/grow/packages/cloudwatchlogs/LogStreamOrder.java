package codes.grow.packages.cloudwatchlogs;

/** Sort key of {@code describeLogStreams}. */
public enum LogStreamOrder {
  /** By stream name (default). */
  LOG_STREAM_NAME("LogStreamName"),
  /** By last event time (eventually consistent on AWS; not with a prefix). */
  LAST_EVENT_TIME("LastEventTime");

  private final String wire;

  LogStreamOrder(String wire) {
    this.wire = wire;
  }

  /**
   * Returns the AWS name.
   *
   * @return the wire value
   */
  public String wireValue() {
    return wire;
  }
}
