// Created by sdk-kit 0.1.0; owned by this repository.
package codes.grow.packages.cloudwatchlogs.examples;

import codes.grow.packages.cloudwatchlogs.CloudWatchLogClientOptions;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsClient;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsConfig;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsQueryClient;
import codes.grow.packages.cloudwatchlogs.Emf;
import codes.grow.packages.cloudwatchlogs.EnsureLogGroupOptions;
import codes.grow.packages.cloudwatchlogs.EnsureLogGroupResult;
import codes.grow.packages.cloudwatchlogs.FilterLogEventsOptions;
import codes.grow.packages.cloudwatchlogs.FilteredLogEvent;
import codes.grow.packages.cloudwatchlogs.GetLogEventsOptions;
import codes.grow.packages.cloudwatchlogs.LogEvent;
import codes.grow.packages.cloudwatchlogs.PutLogEventsResult;
import codes.grow.packages.cloudwatchlogs.QueryOptions;
import codes.grow.packages.cloudwatchlogs.QueryResults;
import codes.grow.packages.cloudwatchlogs.StreamLogEvent;
import codes.grow.packages.cloudwatchlogs.core.AuthenticationException;
import codes.grow.packages.cloudwatchlogs.core.EnvReader;
import codes.grow.packages.cloudwatchlogs.core.HealthStatus;
import codes.grow.packages.cloudwatchlogs.logging.BatchingLogClient;
import codes.grow.packages.cloudwatchlogs.logging.LogRecord;
import codes.grow.packages.cloudwatchlogs.logging.SearchOptions;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Walks through the Amazon CloudWatch Logs Java SDK: configuration from the environment, health
 * and ping, a log group and stream, writing events, reading them back, a Logs Insights query, EMF,
 * the batching log client with its logger, and search.
 *
 * <p>Uses the standard AWS credential chain. Against the Docker emulator: {@code
 * GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000 AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=...
 * AWS_SECRET_ACCESS_KEY=... mvn -q compile exec:java}
 */
public final class BasicExample {
  private BasicExample() {}

  /**
   * Runs the example.
   *
   * @param args unused
   */
  public static void main(String[] args) {
    String group =
        "grow-example/java-"
            + DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC).format(Instant.now());
    // explicit values win over GROW_AWS_CLOUDWATCH_LOGS_* and the standard AWS variables
    CloudWatchLogsConfig config =
        CloudWatchLogsConfig.builder()
            .logGroup(group)
            .logStream("example")
            .fromEnv(EnvReader.system())
            .build();
    System.out.println("config: " + config); // credentials print as [REDACTED]
    try (CloudWatchLogsClient client = CloudWatchLogsClient.create(config)) {
      HealthStatus health = client.health(); // never throws for an unreachable service
      System.out.println("health: " + health);
      try {
        System.out.println("ping: " + client.ping().toMillis() + " ms");
      } catch (AuthenticationException e) {
        System.out.println("check your AWS credentials: " + e.getMessage());
        return;
      }

      // Bootstrap: group (retention + tags) and stream; both calls are idempotent.
      EnsureLogGroupResult created =
          client.ensureLogGroup(
              group,
              EnsureLogGroupOptions.builder().retentionDays(7).tags(Map.of("team", "sdk")).build());
      System.out.println("log group " + group + " created=" + created.created() + " arn=" + created.arn());
      try {
        // Ingest: events are sorted, size-checked and split into valid batches.
        Instant now = Instant.now();
        PutLogEventsResult put =
            client.putLogEvents(
                group,
                "raw",
                List.of(
                    LogEvent.of(now, "hello from Java"),
                    LogEvent.of(now.plusMillis(1), "something failed: ERROR")));
        System.out.println("put: accepted=" + put.accepted() + " batches=" + put.batches());

        for (StreamLogEvent e :
            client.getLogEvents(group, "raw", GetLogEventsOptions.builder().startFromHead(true).build())) {
          System.out.println("read: " + e.timestamp() + " " + e.message());
        }
        for (FilteredLogEvent e :
            client.filterLogEvents(FilterLogEventsOptions.builder().group(group).pattern("ERROR").build())) {
          System.out.println("filter: " + e.message());
        }

        QueryResults rows =
            client.query(
                "fields @timestamp, @message | sort @timestamp desc | limit 5",
                QueryOptions.builder()
                    .groups(List.of(group))
                    .start(now.minusSeconds(300))
                    .end(Instant.now().plusSeconds(60))
                    .build());
        System.out.println("insights: " + rows.status() + " " + rows.rows().size() + " rows");

        client.emf("latency", 42, Map.of("service", "api"), Emf.Options.defaults().withUnit("Milliseconds"));

        // The logging category: batching client, logger facade and search.
        BatchingLogClient logs =
            client.logClient(
                CloudWatchLogClientOptions.defaults().withResource(Map.of("service.name", "example")));
        logs.logger("example").info("logger hello", Map.of("password", "never-sent")); // redacted
        logs.flush(Duration.ofSeconds(10));
        CloudWatchLogsQueryClient query = client.logQueryClient();
        for (LogRecord r : query.search("hello", SearchOptions.builder().limit(10).build()).records()) {
          System.out.println("search: " + r.severityText() + " " + r.body());
        }
      } finally {
        client.deleteLogGroup(group, true);
      }
    }
  }
}
