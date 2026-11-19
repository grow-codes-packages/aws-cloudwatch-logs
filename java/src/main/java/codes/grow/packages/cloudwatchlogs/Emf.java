package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.Json;
import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CloudWatch Embedded Metric Format (EMF) documents (API.md section 2.2). An EMF document is a JSON
 * log event that CloudWatch turns into a metric; the SDK only builds and sends it, metric
 * extraction happens inside AWS.
 */
public final class Emf {
  /** Units accepted by CloudWatch metrics. */
  public static final List<String> UNITS =
      List.of(
          "Seconds",
          "Microseconds",
          "Milliseconds",
          "Bytes",
          "Kilobytes",
          "Megabytes",
          "Gigabytes",
          "Terabytes",
          "Bits",
          "Kilobits",
          "Megabits",
          "Gigabits",
          "Terabits",
          "Percent",
          "Count",
          "Bytes/Second",
          "Kilobytes/Second",
          "Megabytes/Second",
          "Gigabytes/Second",
          "Terabytes/Second",
          "Bits/Second",
          "Kilobits/Second",
          "Megabits/Second",
          "Gigabits/Second",
          "Terabits/Second",
          "Count/Second",
          "None");

  /** Default EMF namespace. */
  public static final String DEFAULT_NAMESPACE = "GrowSdk";

  private Emf() {}

  /**
   * Options of {@link #build} and {@code CloudWatchLogsClient.emf}.
   *
   * @param namespace metric namespace, 1–1024 characters (null: {@code GrowSdk})
   * @param unit CloudWatch unit (null: {@code None})
   * @param timestamp metric time (null: now)
   * @param properties extra top-level properties (searchable fields that are not dimensions)
   * @param group log group for {@code emf()} (null: {@code config.logGroup})
   * @param stream log stream for {@code emf()} (null: {@code config.logStream})
   */
  public record Options(
      String namespace,
      String unit,
      Instant timestamp,
      Map<String, Object> properties,
      String group,
      String stream) {
    /**
     * Applies defaults.
     *
     * @param namespace namespace
     * @param unit unit
     * @param timestamp time
     * @param properties properties
     * @param group group
     * @param stream stream
     */
    public Options {
      namespace = namespace == null ? DEFAULT_NAMESPACE : namespace;
      unit = unit == null ? "None" : unit;
      properties = properties == null ? Map.of() : new LinkedHashMap<>(properties);
    }

    /**
     * Default options.
     *
     * @return the options
     */
    public static Options defaults() {
      return new Options(null, null, null, null, null, null);
    }

    /**
     * Returns a copy with another namespace.
     *
     * @param value namespace
     * @return the copy
     */
    public Options withNamespace(String value) {
      return new Options(value, unit, timestamp, properties, group, stream);
    }

    /**
     * Returns a copy with another unit.
     *
     * @param value unit
     * @return the copy
     */
    public Options withUnit(String value) {
      return new Options(namespace, value, timestamp, properties, group, stream);
    }

    /**
     * Returns a copy with another timestamp.
     *
     * @param value time
     * @return the copy
     */
    public Options withTimestamp(Instant value) {
      return new Options(namespace, unit, value, properties, group, stream);
    }

    /**
     * Returns a copy with other properties.
     *
     * @param value properties
     * @return the copy
     */
    public Options withProperties(Map<String, Object> value) {
      return new Options(namespace, unit, timestamp, value, group, stream);
    }

    /**
     * Returns a copy with another log group and stream.
     *
     * @param groupName log group (null: default)
     * @param streamName log stream (null: default)
     * @return the copy
     */
    public Options withTarget(String groupName, String streamName) {
      return new Options(namespace, unit, timestamp, properties, groupName, streamName);
    }
  }

  /**
   * Builds an EMF document for one metric value.
   *
   * @param metric metric name (1–1024 characters)
   * @param value finite value
   * @param dimensions at most 30 dimensions (one dimension set)
   * @param options namespace, unit, timestamp, properties
   * @return the document (insertion-ordered map)
   * @throws ValidationException on invalid input
   */
  public static Map<String, Object> build(
      String metric, double value, Map<String, String> dimensions, Options options) {
    return build(metric, List.of(value), true, dimensions, options);
  }

  /**
   * Builds an EMF document for 1–100 values of one metric.
   *
   * @param metric metric name (1–1024 characters)
   * @param values 1–100 finite values
   * @param dimensions at most 30 dimensions (one dimension set)
   * @param options namespace, unit, timestamp, properties
   * @return the document (insertion-ordered map)
   * @throws ValidationException on invalid input
   */
  public static Map<String, Object> build(
      String metric, List<Double> values, Map<String, String> dimensions, Options options) {
    return build(metric, values, false, dimensions, options);
  }

  private static Map<String, Object> build(
      String metric,
      List<Double> values,
      boolean single,
      Map<String, String> dimensions,
      Options options) {
    Options o = options == null ? Options.defaults() : options;
    Map<String, String> dims = dimensions == null ? Map.of() : dimensions;
    if (metric == null || metric.isEmpty() || metric.length() > 1024) {
      throw new ValidationException("emf: metric name must be 1-1024 characters");
    }
    if (o.namespace().isEmpty() || o.namespace().length() > 1024) {
      throw new ValidationException("emf: namespace must be 1-1024 characters");
    }
    if (!UNITS.contains(o.unit())) {
      throw new ValidationException("emf: unknown unit " + o.unit());
    }
    if (values == null
        || values.isEmpty()
        || values.size() > 100
        || !values.stream().allMatch(v -> v != null && Double.isFinite(v))) {
      throw new ValidationException("emf: value must be a finite number or 1-100 finite numbers");
    }
    if (dims.size() > 30) {
      throw new ValidationException("emf: at most 30 dimensions");
    }
    for (Map.Entry<String, String> d : dims.entrySet()) {
      String key = d.getKey();
      if (key == null
          || key.isEmpty()
          || key.equals("_aws")
          || key.equals(metric)
          || d.getValue() == null) {
        throw new ValidationException("emf: dimension names must be non-empty and values strings");
      }
    }
    Instant ts = o.timestamp() != null ? o.timestamp() : Instant.now();
    Map<String, Object> metricDef = new LinkedHashMap<>();
    metricDef.put("Name", metric);
    metricDef.put("Unit", o.unit());
    Map<String, Object> directive = new LinkedHashMap<>();
    directive.put("Namespace", o.namespace());
    directive.put("Dimensions", List.of(new ArrayList<>(dims.keySet())));
    directive.put("Metrics", List.of(metricDef));
    Map<String, Object> aws = new LinkedHashMap<>();
    aws.put("Timestamp", ts.toEpochMilli());
    aws.put("CloudWatchMetrics", List.of(directive));
    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("_aws", aws);
    for (Map.Entry<String, Object> p : o.properties().entrySet()) {
      if (Objects.equals(p.getKey(), "_aws") || Objects.equals(p.getKey(), metric)) {
        throw new ValidationException("emf: property " + p.getKey() + " is reserved");
      }
      doc.put(p.getKey(), p.getValue());
    }
    doc.putAll(dims);
    doc.put(metric, single ? values.get(0) : List.copyOf(values));
    return doc;
  }

  /**
   * Serialises a document to its JSON log message.
   *
   * @param document an EMF document
   * @return the JSON text
   */
  public static String toJson(Map<String, Object> document) {
    return Json.stringify(document);
  }
}
