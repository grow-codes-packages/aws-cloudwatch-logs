// Created by sdk-kit 0.1.0; owned by this repository.
package codes.grow.packages.cloudwatchlogs.examples;

import codes.grow.packages.cloudwatchlogs.CloudWatchLogsClient;
import codes.grow.packages.cloudwatchlogs.core.HealthStatus;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** The {@code …Async} methods: run calls concurrently and cancel a slow one. */
public final class AsyncExample {
  private AsyncExample() {}

  /**
   * Runs the example.
   *
   * @param args unused
   */
  public static void main(String[] args) {
    try (var client = CloudWatchLogsClient.fromEnv()) {
      CompletableFuture<HealthStatus> health = client.healthAsync();
      CompletableFuture<Duration> ping = client.pingAsync();
      CompletableFuture.allOf(health, ping.exceptionally(e -> null)).join();
      System.out.println("health: " + health.join());
      ping.whenComplete(
          (latency, error) ->
              System.out.println(error == null ? "ping: " + latency : "ping failed: " + error));

      // serverVersion is UNSUPPORTED: CloudWatch Logs exposes no server version.
      client
          .serverVersionAsync()
          .whenComplete((v, e) -> System.out.println("server version: " + (e == null ? v : e)));

      // Cancelling a future aborts the HTTP exchange and any pending retry.
      CompletableFuture<Duration> slow = client.pingAsync();
      slow.cancel(true);
      System.out.println("ping cancelled: " + slow.isCancelled());
    }
  }
}
