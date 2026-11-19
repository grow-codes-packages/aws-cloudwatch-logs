package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.ConfigException;
import java.util.Locale;

/** What happens to a log event whose message is larger than {@code maxEventBytes}. */
public enum OversizePolicy {
  /** Cut the message on a UTF-8 boundary and append {@code " …[truncated N bytes]"} (default). */
  TRUNCATE,
  /** Leave the event out (counted in {@link PutLogEventsResult#dropped()}). */
  DROP,
  /** Fail the call with a {@code ValidationException}. */
  ERROR;

  /**
   * Returns the lowercase wire name ({@code truncate}, {@code drop}, {@code error}).
   *
   * @return the wire name
   */
  public String wireValue() {
    return name().toLowerCase(Locale.ROOT);
  }

  /**
   * Parses {@code truncate}, {@code drop} or {@code error} (case-insensitive).
   *
   * @param text the value
   * @return the policy
   * @throws ConfigException when the value is none of them
   */
  public static OversizePolicy parse(String text) {
    String v = text == null ? "" : text.trim().toUpperCase(Locale.ROOT);
    for (OversizePolicy p : values()) {
      if (p.name().equals(v)) {
        return p;
      }
    }
    throw new ConfigException("config: oversizePolicy must be truncate, drop or error");
  }
}
