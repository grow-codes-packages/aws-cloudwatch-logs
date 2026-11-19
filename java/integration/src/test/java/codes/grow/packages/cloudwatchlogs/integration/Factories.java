package codes.grow.packages.cloudwatchlogs.integration;

import codes.grow.packages.cloudwatchlogs.CloudWatchLogClientOptions;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsClient;
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsConfig;
import codes.grow.packages.cloudwatchlogs.core.EnvReader;
import codes.grow.packages.cloudwatchlogs.logging.BatchOptions;
import codes.grow.packages.cloudwatchlogs.testing.ConformanceRunner;
import codes.grow.packages.cloudwatchlogs.testing.logging.LoggingTarget;
import java.util.UUID;

/**
 * Conformance targets of this service.
 *
 * <p>{@code core} is the client; {@code logging} is a batching log client plus the query client
 * over a log group created for the scenario. The {@code version} capability is left out:
 * CloudWatch Logs exposes no server version (serverVersion is UNSUPPORTED, see {@code
 * docs/compatibility.md}).
 */
final class Factories {
  /** Capabilities the deployment under test offers (see the scenario {@code requires}). */
  static final String[] CAPABILITIES = {"query"};

  private Factories() {}

  static void register(ConformanceRunner.Builder runner, EnvReader env) {
    runner.target("core", scenario -> CloudWatchLogsClient.builder().fromEnv(env).build());
    runner.target(
        "logging",
        scenario -> {
          String run = UUID.randomUUID().toString().substring(0, 8);
          String group = "grow-conformance-java/" + run;
          CloudWatchLogsClient client =
              CloudWatchLogsClient.create(
                  CloudWatchLogsConfig.builder()
                      .logGroup(group)
                      .logStream("conformance-" + run)
                      .fromEnv(env)
                      .build());
          client.ensureLogGroup(group);
          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread(
                      () -> {
                        try {
                          client.deleteLogGroup(group, true);
                        } catch (RuntimeException ignored) {
                          // the stack is disposable
                        }
                        client.close();
                      }));
          BatchOptions batch = LoggingTarget.batchOptions(scenario, BatchOptions.defaults());
          return LoggingTarget.of(
              client.logClient(CloudWatchLogClientOptions.defaults().withBatchOptions(batch)),
              client.logQueryClient());
        });
  }
}
