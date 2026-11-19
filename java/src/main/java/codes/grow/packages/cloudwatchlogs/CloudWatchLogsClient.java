package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.CancelledException;
import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import codes.grow.packages.cloudwatchlogs.core.ConfigException;
import codes.grow.packages.cloudwatchlogs.core.ConflictException;
import codes.grow.packages.cloudwatchlogs.core.EnvReader;
import codes.grow.packages.cloudwatchlogs.core.HealthStatus;
import codes.grow.packages.cloudwatchlogs.core.HttpTransport;
import codes.grow.packages.cloudwatchlogs.core.NotFoundException;
import codes.grow.packages.cloudwatchlogs.core.TimeoutException;
import codes.grow.packages.cloudwatchlogs.core.UnavailableException;
import codes.grow.packages.cloudwatchlogs.core.UnsupportedException;
import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import codes.grow.packages.cloudwatchlogs.logging.BatchingLogClient;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsProfileRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteRetentionPolicyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeQueriesRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilterLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.LiveTailSessionLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.LiveTailSessionStart;
import software.amazon.awssdk.services.cloudwatchlogs.model.LiveTailSessionUpdate;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutRetentionPolicyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartLiveTailRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartLiveTailResponseHandler;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartLiveTailResponseStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.StopQueryRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.TagResourceRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.UntagResourceRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

/**
 * Client for Amazon CloudWatch Logs, built on the official AWS SDK for Java v2 (SPEC section 2.3
 * "Cloud SDK services").
 *
 * <p>Thread-safe; create one per configuration and close it when done. Every I/O method has a
 * blocking form and an {@code …Async} form returning {@link CompletableFuture}; cancelling the
 * future aborts the call. Paginated listings return a lazy {@link Iterable} (blocking) or the
 * collected list (async).
 *
 * <pre>{@code
 * try (CloudWatchLogsClient client = CloudWatchLogsClient.fromEnv()) {
 *   client.ensureLogGroup("my-app/api", EnsureLogGroupOptions.builder().retentionDays(7).build());
 *   client.putLogEvents("my-app/api", "web-1", List.of(LogEvent.of("hello")));
 * }
 * }</pre>
 */
public final class CloudWatchLogsClient implements AutoCloseable {
  /** Allowed {@code retentionDays} values. */
  public static final List<Integer> RETENTION_DAYS =
      List.of(
          1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557,
          2922, 3288, 3653);

  /** CloudWatch Logs API version ({@code 2014-03-28}). */
  public static final String API_VERSION = CloudWatchLogsConfig.API_VERSION;

  private static final ScheduledThreadPoolExecutor TIMERS = timers();
  private static final Pattern INSIGHTS_TIME =
      Pattern.compile("(\\d{4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}:\\d{2})(\\.\\d{1,9})?Z?");

  private final CloudWatchLogsConfig config;
  private final CloudWatchLogsAsyncClient sdk;
  private final CloudWatchLogsAsyncClient probe;
  private final boolean ownsSdk;
  private final GrowHttpClient http;
  private final List<AutoCloseable> owned = new ArrayList<>();
  private final String region;
  private final Internals internals;
  private final Set<BatchingLogClient> logClients = ConcurrentHashMap.newKeySet();
  private final Set<LiveTailHandle> tails = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile boolean shutdown;

  /** Test seams. Not part of the stable API. */
  static final class Internals {
    /** Disable the {@code stream-} host prefix of StartLiveTail (mock servers on an IP). */
    boolean disableHostPrefix;

    /** Replaces the delay between Logs Insights polls. */
    Function<Duration, CompletableFuture<Void>> sleeper = CloudWatchLogsClient::delay;
  }

  private static ScheduledThreadPoolExecutor timers() {
    ScheduledThreadPoolExecutor t =
        new ScheduledThreadPoolExecutor(
            1,
            r -> {
              Thread th = new Thread(r, "grow-cloudwatch-logs-timer");
              th.setDaemon(true);
              return th;
            });
    t.setRemoveOnCancelPolicy(true);
    return t;
  }

  static CompletableFuture<Void> delay(Duration d) {
    CompletableFuture<Void> f = new CompletableFuture<>();
    ScheduledFuture<?> s =
        TIMERS.schedule(() -> f.complete(null), Math.max(0, d.toMillis()), TimeUnit.MILLISECONDS);
    f.whenComplete((v, t) -> s.cancel(false));
    return f;
  }

  private CloudWatchLogsClient(CloudWatchLogsConfig config, Internals internals) {
    this.config = config;
    this.internals = internals;
    Optional<CloudWatchLogsAsyncClient> supplied = config.sdkClient();
    if (supplied.isPresent()) {
      this.sdk = supplied.get();
      this.probe = supplied.get();
      this.ownsSdk = false;
      this.http = null;
      Region r = sdk.serviceClientConfiguration().region();
      this.region = r == null ? null : r.id();
      return;
    }
    this.region = resolveRegion(config);
    this.http = new GrowHttpClient(config.toTransportConfig());
    AwsCredentialsProvider credentials = credentials(config);
    ClientOverrideConfiguration.Builder override =
        ClientOverrideConfiguration.builder()
            .putAdvancedOption(
                SdkAdvancedClientOption.USER_AGENT_SUFFIX, config.toTransportConfig().userAgent());
    config.headers().forEach(override::putHeader);
    if (internals.disableHostPrefix) {
      override.putAdvancedOption(SdkAdvancedClientOption.DISABLE_HOST_PREFIX_INJECTION, true);
    }
    this.sdk = build(credentials, override.retryStrategy(retryStrategy(config)).build());
    this.probe = build(credentials, override.retryStrategy(AwsRetryStrategy.doNotRetry()).build());
    this.ownsSdk = true;
  }

  private CloudWatchLogsAsyncClient build(
      AwsCredentialsProvider credentials, ClientOverrideConfiguration override) {
    var b =
        CloudWatchLogsAsyncClient.builder()
            .httpClient(http)
            .region(Region.of(region))
            .credentialsProvider(credentials)
            .overrideConfiguration(override);
    config.endpoint().ifPresent(e -> b.endpointOverride(URI.create(e)));
    return b.build();
  }

  private static String resolveRegion(CloudWatchLogsConfig config) {
    if (config.region().isPresent()) {
      return config.region().get();
    }
    try {
      if (config.profile().isPresent()) {
        Region r =
            new AwsProfileRegionProvider(ProfileFile::defaultProfileFile, config.profile().get())
                .getRegion();
        if (r != null) {
          return r.id();
        }
      }
      return DefaultAwsRegionProviderChain.builder().build().getRegion().id();
    } catch (RuntimeException e) {
      throw new ConfigException(
          "config: AWS region is not configured (set GROW_AWS_CLOUDWATCH_LOGS_REGION or AWS_REGION)");
    }
  }

  private AwsCredentialsProvider credentials(CloudWatchLogsConfig cfg) {
    AwsCredentialsProvider base;
    if (cfg.accessKeyId().isPresent() && cfg.secretAccessKey().isPresent()) {
      String key = cfg.accessKeyId().get();
      String secret = cfg.secretAccessKey().get().reveal();
      base =
          StaticCredentialsProvider.create(
              cfg.sessionToken().isPresent()
                  ? AwsSessionCredentials.create(key, secret, cfg.sessionToken().get().reveal())
                  : AwsBasicCredentials.create(key, secret));
    } else if (cfg.credentialsProvider().isPresent()) {
      base = cfg.credentialsProvider().get();
    } else if (cfg.profile().isPresent()) {
      ProfileCredentialsProvider p = ProfileCredentialsProvider.create(cfg.profile().get());
      owned.add(p);
      base = p;
    } else {
      DefaultCredentialsProvider p = DefaultCredentialsProvider.builder().build();
      owned.add(p);
      base = p;
    }
    if (cfg.roleArn().isEmpty()) {
      return base;
    }
    StsClient sts =
        StsClient.builder()
            .httpClient(http)
            .region(Region.of(region))
            .credentialsProvider(base)
            .build();
    StsAssumeRoleCredentialsProvider assume =
        StsAssumeRoleCredentialsProvider.builder()
            .stsClient(sts)
            .refreshRequest(
                r -> {
                  r.roleArn(cfg.roleArn().get()).roleSessionName(cfg.roleSessionName());
                  cfg.externalId().ifPresent(r::externalId);
                })
            .asyncCredentialUpdateEnabled(false)
            .build();
    owned.add(assume);
    owned.add(sts);
    return assume;
  }

  private static RetryStrategy retryStrategy(CloudWatchLogsConfig config) {
    codes.grow.packages.cloudwatchlogs.core.RetryPolicy p = config.retry();
    BackoffStrategy backoff = attempt -> p.backoff(Math.max(0, attempt - 1));
    return AwsRetryStrategy.standardRetryStrategy().toBuilder()
        .maxAttempts(p.maxRetries() + 1)
        .backoffStrategy(backoff)
        .throttlingBackoffStrategy(backoff)
        .circuitBreakerEnabled(false)
        .build();
  }

  /**
   * Creates a client.
   *
   * @param config validated configuration
   * @return a new client
   * @throws ConfigException when the configuration is invalid or no region can be resolved
   */
  public static CloudWatchLogsClient create(CloudWatchLogsConfig config) {
    return create(config, new Internals());
  }

  static CloudWatchLogsClient create(CloudWatchLogsConfig config, Internals internals) {
    config.validate();
    return new CloudWatchLogsClient(config, internals);
  }

  /**
   * Creates a client from {@code GROW_AWS_CLOUDWATCH_LOGS_*} and the standard AWS variables.
   *
   * @return a new client
   */
  public static CloudWatchLogsClient fromEnv() {
    return create(CloudWatchLogsConfig.fromEnv());
  }

  /**
   * Starts a client builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the configuration.
   *
   * @return the configuration
   */
  public CloudWatchLogsConfig config() {
    return config;
  }

  /**
   * Returns the underlying AWS SDK client (for operations this package does not wrap).
   *
   * @return the SDK client
   */
  public CloudWatchLogsAsyncClient sdk() {
    return sdk;
  }

  /**
   * Returns the resolved AWS region.
   *
   * @return the region id, e.g. {@code us-east-1}
   */
  public String region() {
    return region;
  }

  /**
   * Returns whether {@link #close()} was called.
   *
   * @return true once closed
   */
  public boolean isClosed() {
    return closed.get();
  }

  // ------------------------------------------------------------------ plumbing

  private static <T> CompletableFuture<T> failed(Throwable t) {
    return CompletableFuture.failedFuture(t);
  }

  /** Runs {@code body}, turning a synchronous exception into a failed future. */
  private static <T> CompletableFuture<T> defer(Supplier<CompletableFuture<T>> body) {
    try {
      return body.get();
    } catch (RuntimeException e) {
      return failed(e);
    }
  }

  private <T> CompletableFuture<T> call(
      String op, Function<CloudWatchLogsAsyncClient, CompletableFuture<T>> fn) {
    return call(op, false, config.totalTimeout(), fn);
  }

  /** One SDK call with the total timeout, cancellation and error mapping. */
  private <T> CompletableFuture<T> call(
      String op,
      boolean useProbe,
      Duration timeout,
      Function<CloudWatchLogsAsyncClient, CompletableFuture<T>> fn) {
    if (shutdown) {
      return failed(new CancelledException(op + ": the client is closed"));
    }
    CompletableFuture<T> sdkFuture;
    try {
      sdkFuture = fn.apply(useProbe ? probe : sdk);
    } catch (RuntimeException e) {
      return failed(AwsErrors.map(op, e, config.knownSecrets()));
    }
    CompletableFuture<T> result = new CompletableFuture<>();
    ScheduledFuture<?> timer =
        timeout == null
            ? null
            : TIMERS.schedule(
                () -> {
                  if (result.completeExceptionally(
                      new TimeoutException(
                          op + ": timed out after " + timeout.toMillis() + " ms"))) {
                    sdkFuture.cancel(true);
                  }
                },
                timeout.toMillis(),
                TimeUnit.MILLISECONDS);
    sdkFuture.whenComplete(
        (v, t) -> {
          if (timer != null) {
            timer.cancel(false);
          }
          if (t == null) {
            result.complete(v);
          } else {
            result.completeExceptionally(AwsErrors.map(op, t, config.knownSecrets()));
          }
        });
    result.whenComplete(
        (v, t) -> {
          if (result.isCancelled()) {
            sdkFuture.cancel(true);
          }
        });
    return result;
  }

  private static <T> T await(CompletableFuture<T> f, String op) {
    return HttpTransport.await(f, op);
  }

  private String group(String group, String op) {
    String name = group != null ? group : config.logGroup().orElse(null);
    if (name == null) {
      throw new ConfigException(op + ": no log group given and config.logGroup is not set");
    }
    return name;
  }

  private static void checkGroupName(String name, String op) {
    if (!CloudWatchLogsConfig.isValidLogGroupName(name)) {
      throw new ValidationException(
          op + ": log group name must be 1-512 characters of [.-_/#A-Za-z0-9]");
    }
  }

  private static void checkStreamName(String name, String op) {
    if (!CloudWatchLogsConfig.isValidLogStreamName(name)) {
      throw new ValidationException(
          op + ": log stream name must be 1-512 characters without : or *");
    }
  }

  private static void checkRange(Integer value, long min, long max, String name, String op) {
    if (value != null && (value < min || value > max)) {
      throw new ValidationException(
          op + ": " + name + " must be an integer from " + min + " to " + max);
    }
  }

  private static void checkRetention(Integer days, String op) {
    if (days == null || days == 0) {
      return;
    }
    if (!RETENTION_DAYS.contains(days)) {
      throw new ValidationException(op + ": retentionDays must be one of " + RETENTION_DAYS);
    }
  }

  static String stripStar(String arn) {
    return arn.endsWith(":*") ? arn.substring(0, arn.length() - 2) : arn;
  }

  private static Optional<Instant> ms(Long value) {
    return value == null ? Optional.empty() : Optional.of(Instant.ofEpochMilli(value));
  }

  private static LogGroupInfo toGroup(LogGroup g) {
    String arn = g.logGroupArn() != null ? g.logGroupArn() : (g.arn() != null ? g.arn() : "");
    return new LogGroupInfo(
        g.logGroupName() == null ? "" : g.logGroupName(),
        stripStar(arn),
        ms(g.creationTime()),
        g.retentionInDays() == null
            ? java.util.OptionalInt.empty()
            : java.util.OptionalInt.of(g.retentionInDays()),
        g.storedBytes() == null
            ? java.util.OptionalLong.empty()
            : java.util.OptionalLong.of(g.storedBytes()),
        Optional.ofNullable(g.kmsKeyId()),
        g.metricFilterCount() == null
            ? java.util.OptionalInt.empty()
            : java.util.OptionalInt.of(g.metricFilterCount()),
        Optional.ofNullable(g.logGroupClassAsString()));
  }

  private static LogStreamInfo toStream(LogStream s) {
    return new LogStreamInfo(
        s.logStreamName() == null ? "" : s.logStreamName(),
        Optional.ofNullable(s.arn()),
        ms(s.creationTime()),
        ms(s.firstEventTimestamp()),
        ms(s.lastEventTimestamp()),
        ms(s.lastIngestionTime()));
  }

  /**
   * Converts a Logs Insights {@code @timestamp} ({@code YYYY-MM-DD HH:mm:ss.SSS} UTC on AWS, epoch
   * milliseconds on Moto) to an instant.
   *
   * @param value the field value
   * @return the instant, or empty when the text is neither form
   */
  public static Optional<Instant> parseInsightsTimestamp(String value) {
    if (value == null) {
      return Optional.empty();
    }
    String v = value.trim();
    if (v.matches("\\d{1,18}")) {
      return Optional.of(Instant.ofEpochMilli(Long.parseLong(v)));
    }
    Matcher m = INSIGHTS_TIME.matcher(v);
    if (!m.matches()) {
      return Optional.empty();
    }
    String frac = m.group(3) == null ? "" : m.group(3);
    try {
      return Optional.of(
          LocalDateTime.parse(m.group(1) + "T" + m.group(2) + frac).toInstant(ZoneOffset.UTC));
    } catch (DateTimeParseException e) {
      return Optional.empty();
    }
  }

  /**
   * Returns the partition of a region ({@code aws}, {@code aws-cn}, {@code aws-us-gov}, …).
   *
   * @param region region id
   * @return the partition
   */
  public static String partitionForRegion(String region) {
    if (region.startsWith("cn-")) {
      return "aws-cn";
    }
    if (region.startsWith("us-gov-")) {
      return "aws-us-gov";
    }
    if (region.startsWith("us-isob-")) {
      return "aws-iso-b";
    }
    if (region.startsWith("us-iso-")) {
      return "aws-iso";
    }
    if (region.startsWith("eusc-")) {
      return "aws-eusc";
    }
    return "aws";
  }

  /** One page of a listing: the items and the next token (null at the end). */
  private record Page<T>(List<T> items, String next) {}

  /** Fetches the page after {@code token} with at most {@code remaining} items (null = no cap). */
  private interface PageFetcher<T>
      extends BiFunction<String, Integer, CompletableFuture<Page<T>>> {}

  /** Lazy blocking iteration over pages. */
  private static final class PagedIterable<T> implements Iterable<T> {
    private final String op;
    private final PageFetcher<T> fetch;
    private final Integer limit;

    PagedIterable(String op, PageFetcher<T> fetch, Integer limit) {
      this.op = op;
      this.fetch = fetch;
      this.limit = limit;
    }

    @Override
    public Iterator<T> iterator() {
      return new Iterator<>() {
        private final Deque<T> buffer = new ArrayDeque<>();
        private String token;
        private boolean end;
        private int count;

        @Override
        public boolean hasNext() {
          if (limit != null && count >= limit) {
            return false;
          }
          while (buffer.isEmpty() && !end) {
            Page<T> page =
                await(fetch.apply(token, limit == null ? null : limit - count - buffer.size()), op);
            buffer.addAll(page.items());
            token = page.next();
            end = token == null;
          }
          return !buffer.isEmpty();
        }

        @Override
        public T next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          count++;
          return buffer.poll();
        }
      };
    }
  }

  private static <T> CompletableFuture<List<T>> collect(PageFetcher<T> fetch, Integer limit) {
    List<T> out = new ArrayList<>();
    return collectFrom(fetch, limit, null, out).thenApply(v -> out);
  }

  private static <T> CompletableFuture<Void> collectFrom(
      PageFetcher<T> fetch, Integer limit, String token, List<T> out) {
    return fetch
        .apply(token, limit == null ? null : limit - out.size())
        .thenCompose(
            page -> {
              for (T item : page.items()) {
                if (limit != null && out.size() >= limit) {
                  break;
                }
                out.add(item);
              }
              if (page.next() == null || (limit != null && out.size() >= limit)) {
                return CompletableFuture.completedFuture(null);
              }
              return collectFrom(fetch, limit, page.next(), out);
            });
  }

  /**
   * Wraps an iterable in a sequential stream.
   *
   * @param <T> item type
   * @param iterable the iterable
   * @return the stream
   */
  public static <T> Stream<T> stream(Iterable<T> iterable) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterable.iterator(), Spliterator.ORDERED), false);
  }

  // ------------------------------------------------------------------ core

  /**
   * Liveness via a signed {@code DescribeLogGroups {limit: 1}} without retries. Never throws for an
   * unhealthy or unreachable service. The service answering with an authentication or permission
   * error still counts as healthy ({@code details.auth = "failed"}); throttling reports {@code
   * degraded}.
   *
   * @return the health status
   * @throws CancelledException when interrupted or closed
   */
  public HealthStatus health() {
    return await(healthAsync(), "health");
  }

  /**
   * Asynchronous {@link #health()}.
   *
   * @return a future with the health status
   */
  public CompletableFuture<HealthStatus> healthAsync() {
    Duration timeout =
        config.requestTimeout().compareTo(Duration.ofSeconds(10)) < 0
            ? config.requestTimeout()
            : Duration.ofSeconds(10);
    DescribeLogGroupsRequest.Builder req = DescribeLogGroupsRequest.builder().limit(1);
    config.logGroup().ifPresent(req::logGroupNamePrefix);
    CompletableFuture<HealthStatus> result = new CompletableFuture<>();
    call("health", true, timeout, c -> c.describeLogGroups(req.build()))
        .whenComplete(
            (v, t) -> {
              if (t == null) {
                result.complete(HealthStatus.ok());
                return;
              }
              CloudWatchLogsException e = AwsErrors.map("health", t, config.knownSecrets());
              switch (e.code()) {
                case CANCELLED:
                case CONFIG:
                  result.completeExceptionally(e);
                  return;
                case AUTHENTICATION:
                case PERMISSION_DENIED:
                  result.complete(
                      new HealthStatus(
                          true, "ok", Map.of("auth", "failed", "code", e.code().name())));
                  return;
                case RATE_LIMITED:
                  result.complete(
                      new HealthStatus(true, "degraded", Map.of("code", e.code().name())));
                  return;
                default:
                  if (e.httpStatus().isPresent()) {
                    result.complete(
                        new HealthStatus(
                            false,
                            "unhealthy",
                            Map.of(
                                "httpStatus",
                                String.valueOf(e.httpStatus().getAsInt()),
                                "error",
                                e.code().name())));
                  } else {
                    result.complete(
                        new HealthStatus(false, "unreachable", Map.of("error", e.code().name())));
                  }
              }
            });
    return result;
  }

  /**
   * Authenticated round trip ({@code DescribeLogGroups {limit: 1}}).
   *
   * @return the latency
   * @throws codes.grow.packages.cloudwatchlogs.core.AuthenticationException on bad credentials
   * @throws codes.grow.packages.cloudwatchlogs.core.PermissionDeniedException without {@code
   *     logs:DescribeLogGroups}
   */
  public Duration ping() {
    return await(pingAsync(), "ping");
  }

  /**
   * Asynchronous {@link #ping()}.
   *
   * @return a future with the latency
   */
  public CompletableFuture<Duration> pingAsync() {
    long start = System.nanoTime();
    return call(
            "ping", c -> c.describeLogGroups(DescribeLogGroupsRequest.builder().limit(1).build()))
        .thenApply(r -> Duration.ofNanos(Math.max(1, System.nanoTime() - start)));
  }

  /**
   * Always throws {@link UnsupportedException}: CloudWatch Logs exposes no server version (see
   * {@link #API_VERSION}).
   *
   * @return never returns
   * @throws UnsupportedException always
   */
  public String serverVersion() {
    return await(serverVersionAsync(), "serverVersion");
  }

  /**
   * Asynchronous {@link #serverVersion()} (always fails with {@link UnsupportedException}).
   *
   * @return a failed future
   */
  public CompletableFuture<String> serverVersionAsync() {
    return failed(
        new UnsupportedException(
            "serverVersion: Amazon CloudWatch Logs does not expose a server version"));
  }

  /**
   * Flushes and closes every log client created by {@link #logClient}, stops live-tail sessions and
   * releases connections. Idempotent; never closes a caller-supplied SDK client. Later calls fail
   * with {@link CancelledException}.
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    for (BatchingLogClient c : List.copyOf(logClients)) {
      try {
        c.close();
      } catch (RuntimeException ignored) {
        // best effort
      }
    }
    for (LiveTailHandle t : List.copyOf(tails)) {
      t.close();
    }
    shutdown = true;
    if (ownsSdk) {
      sdk.close();
      probe.close();
      for (AutoCloseable c : owned) {
        try {
          c.close();
        } catch (Exception ignored) {
          // best effort
        }
      }
      http.close();
    }
  }

  // ------------------------------------------------------------------ bootstrap

  /**
   * Finds a log group by exact name ({@code DescribeLogGroups} with the name as prefix).
   *
   * @param name group name
   * @return the group, or empty
   */
  public Optional<LogGroupInfo> getLogGroup(String name) {
    return await(getLogGroupAsync(name), "getLogGroup");
  }

  /**
   * Asynchronous {@link #getLogGroup(String)}.
   *
   * @param name group name
   * @return a future with the group, or empty
   */
  public CompletableFuture<Optional<LogGroupInfo>> getLogGroupAsync(String name) {
    return defer(
        () -> {
          checkGroupName(name, "getLogGroup");
          return findGroup(name, null);
        });
  }

  private CompletableFuture<Optional<LogGroupInfo>> findGroup(String name, String token) {
    DescribeLogGroupsRequest.Builder req =
        DescribeLogGroupsRequest.builder().logGroupNamePrefix(name).limit(50);
    if (token != null) {
      req.nextToken(token);
    }
    return call("getLogGroup", c -> c.describeLogGroups(req.build()))
        .thenCompose(
            (DescribeLogGroupsResponse out) -> {
              for (LogGroup g : out.logGroups()) {
                if (name.equals(g.logGroupName())) {
                  return CompletableFuture.completedFuture(Optional.of(toGroup(g)));
                }
              }
              String next = out.nextToken();
              return next == null || next.isEmpty()
                  ? CompletableFuture.completedFuture(Optional.<LogGroupInfo>empty())
                  : findGroup(name, next);
            });
  }

  private CompletableFuture<LogGroupInfo> requireGroup(String name, String op) {
    return findGroup(name, null)
        .thenApply(g -> g.orElseThrow(() -> new NotFoundException(op + ": log group not found")));
  }

  /**
   * Creates the log group when it is missing ({@code CreateLogGroup}), then applies retention and
   * tags. For an existing group the requested tags are added (never removed) and the retention is
   * re-applied; a different KMS key raises {@link ConflictException}.
   *
   * @param name group name
   * @param options retention, KMS key, tags
   * @return whether the group was created, and its ARN
   */
  public EnsureLogGroupResult ensureLogGroup(String name, EnsureLogGroupOptions options) {
    return await(ensureLogGroupAsync(name, options), "ensureLogGroup");
  }

  /**
   * {@link #ensureLogGroup(String, EnsureLogGroupOptions)} with default options.
   *
   * @param name group name
   * @return whether the group was created, and its ARN
   */
  public EnsureLogGroupResult ensureLogGroup(String name) {
    return ensureLogGroup(name, EnsureLogGroupOptions.defaults());
  }

  /**
   * Asynchronous {@link #ensureLogGroup(String, EnsureLogGroupOptions)}.
   *
   * @param name group name
   * @param options retention, KMS key, tags
   * @return a future with the result
   */
  public CompletableFuture<EnsureLogGroupResult> ensureLogGroupAsync(
      String name, EnsureLogGroupOptions options) {
    String op = "ensureLogGroup";
    EnsureLogGroupOptions o = options == null ? EnsureLogGroupOptions.defaults() : options;
    return defer(
        () -> {
          checkGroupName(name, op);
          checkRetention(o.retentionDays(), op);
          CreateLogGroupRequest.Builder req = CreateLogGroupRequest.builder().logGroupName(name);
          if (o.kmsKeyId() != null) {
            req.kmsKeyId(o.kmsKeyId());
          }
          if (!o.tags().isEmpty()) {
            req.tags(o.tags());
          }
          return call(op, c -> c.createLogGroup(req.build()))
              .handle(
                  (v, t) -> {
                    if (t == null) {
                      return true;
                    }
                    if (isAlreadyExists(t)) {
                      return false;
                    }
                    throw AwsErrors.map(op, t, config.knownSecrets());
                  })
              .thenCompose(
                  created ->
                      requireGroup(name, op)
                          .thenCompose(
                              group -> {
                                CompletableFuture<Void> step =
                                    CompletableFuture.completedFuture(null);
                                if (!created) {
                                  String want = o.kmsKeyId();
                                  String have = group.kmsKeyId().orElse("");
                                  if (want != null
                                      && !want.equals(have)
                                      && !have.endsWith("/" + want)) {
                                    throw new ConflictException(
                                        op + ": log group exists with a different KMS key");
                                  }
                                  if (!o.tags().isEmpty()) {
                                    step = tagArn(op, group.arn(), o.tags());
                                  }
                                }
                                if (o.retentionDays() != null) {
                                  step =
                                      step.thenCompose(
                                          x -> putRetentionPolicyAsync(name, o.retentionDays()));
                                }
                                return step.thenApply(
                                    x -> new EnsureLogGroupResult(created, group.arn()));
                              }));
        });
  }

  private static boolean isAlreadyExists(Throwable t) {
    return AwsErrors.awsErrorCode(t).map("ResourceAlreadyExistsException"::equals).orElse(false);
  }

  /**
   * Creates the log stream when it is missing ({@code CreateLogStream}).
   *
   * @param group group name
   * @param stream stream name
   * @param createGroup create the log group first when it is missing
   * @return whether the stream was created
   * @throws NotFoundException when the group is missing and {@code createGroup} is false
   */
  public EnsureLogStreamResult ensureLogStream(String group, String stream, boolean createGroup) {
    return await(ensureLogStreamAsync(group, stream, createGroup), "ensureLogStream");
  }

  /**
   * {@link #ensureLogStream(String, String, boolean)} without creating the group.
   *
   * @param group group name
   * @param stream stream name
   * @return whether the stream was created
   */
  public EnsureLogStreamResult ensureLogStream(String group, String stream) {
    return ensureLogStream(group, stream, false);
  }

  /**
   * Asynchronous {@link #ensureLogStream(String, String, boolean)}.
   *
   * @param group group name
   * @param stream stream name
   * @param createGroup create the log group first when it is missing
   * @return a future with the result
   */
  public CompletableFuture<EnsureLogStreamResult> ensureLogStreamAsync(
      String group, String stream, boolean createGroup) {
    String op = "ensureLogStream";
    return defer(
        () -> {
          checkGroupName(group, op);
          checkStreamName(stream, op);
          CompletableFuture<?> first =
              createGroup
                  ? ensureLogGroupAsync(group, EnsureLogGroupOptions.defaults())
                  : CompletableFuture.completedFuture(null);
          return first.thenCompose(
              x ->
                  call(
                          op,
                          c ->
                              c.createLogStream(
                                  CreateLogStreamRequest.builder()
                                      .logGroupName(group)
                                      .logStreamName(stream)
                                      .build()))
                      .handle(
                          (v, t) -> {
                            if (t == null) {
                              return new EnsureLogStreamResult(true);
                            }
                            if (isAlreadyExists(t)) {
                              return new EnsureLogStreamResult(false);
                            }
                            throw AwsErrors.map(op, t, config.knownSecrets());
                          }));
        });
  }

  /**
   * Sets the retention ({@code PutRetentionPolicy}); {@code 0} removes it ({@code
   * DeleteRetentionPolicy}, never expire).
   *
   * @param group group name
   * @param days one of {@link #RETENTION_DAYS}, or 0
   */
  public void putRetentionPolicy(String group, int days) {
    await(putRetentionPolicyAsync(group, days), "putRetentionPolicy");
  }

  /**
   * Asynchronous {@link #putRetentionPolicy(String, int)}.
   *
   * @param group group name
   * @param days one of {@link #RETENTION_DAYS}, or 0
   * @return a future that completes when done
   */
  public CompletableFuture<Void> putRetentionPolicyAsync(String group, int days) {
    String op = "putRetentionPolicy";
    return defer(
        () -> {
          checkGroupName(group, op);
          checkRetention(days, op);
          if (days == 0) {
            return call(
                    op,
                    c ->
                        c.deleteRetentionPolicy(
                            DeleteRetentionPolicyRequest.builder().logGroupName(group).build()))
                .thenApply(r -> null);
          }
          return call(
                  op,
                  c ->
                      c.putRetentionPolicy(
                          PutRetentionPolicyRequest.builder()
                              .logGroupName(group)
                              .retentionInDays(days)
                              .build()))
              .thenApply(r -> null);
        });
  }

  /**
   * Deletes a log group with its streams and events.
   *
   * @param name group name
   * @param ignoreMissing return false instead of raising {@link NotFoundException}
   * @return true when deleted, false when missing and {@code ignoreMissing}
   */
  public boolean deleteLogGroup(String name, boolean ignoreMissing) {
    return await(deleteLogGroupAsync(name, ignoreMissing), "deleteLogGroup");
  }

  /**
   * {@link #deleteLogGroup(String, boolean)} that raises {@link NotFoundException} when missing.
   *
   * @param name group name
   * @return true
   */
  public boolean deleteLogGroup(String name) {
    return deleteLogGroup(name, false);
  }

  /**
   * Asynchronous {@link #deleteLogGroup(String, boolean)}.
   *
   * @param name group name
   * @param ignoreMissing return false instead of raising {@link NotFoundException}
   * @return a future with the result
   */
  public CompletableFuture<Boolean> deleteLogGroupAsync(String name, boolean ignoreMissing) {
    String op = "deleteLogGroup";
    return defer(
        () -> {
          checkGroupName(name, op);
          return ignoring(
              call(
                  op,
                  c ->
                      c.deleteLogGroup(DeleteLogGroupRequest.builder().logGroupName(name).build())),
              ignoreMissing);
        });
  }

  private static CompletableFuture<Boolean> ignoring(
      CompletableFuture<?> f, boolean ignoreMissing) {
    return f.handle(
        (v, t) -> {
          if (t == null) {
            return true;
          }
          CloudWatchLogsException e = HttpTransport.asSdkException(t, "delete");
          if (ignoreMissing && e instanceof NotFoundException) {
            return false;
          }
          throw e;
        });
  }

  /**
   * Deletes a log stream.
   *
   * @param group group name
   * @param stream stream name
   * @param ignoreMissing return false instead of raising {@link NotFoundException}
   * @return true when deleted, false when missing and {@code ignoreMissing}
   */
  public boolean deleteLogStream(String group, String stream, boolean ignoreMissing) {
    return await(deleteLogStreamAsync(group, stream, ignoreMissing), "deleteLogStream");
  }

  /**
   * {@link #deleteLogStream(String, String, boolean)} that raises {@link NotFoundException} when
   * missing.
   *
   * @param group group name
   * @param stream stream name
   * @return true
   */
  public boolean deleteLogStream(String group, String stream) {
    return deleteLogStream(group, stream, false);
  }

  /**
   * Asynchronous {@link #deleteLogStream(String, String, boolean)}.
   *
   * @param group group name
   * @param stream stream name
   * @param ignoreMissing return false instead of raising {@link NotFoundException}
   * @return a future with the result
   */
  public CompletableFuture<Boolean> deleteLogStreamAsync(
      String group, String stream, boolean ignoreMissing) {
    String op = "deleteLogStream";
    return defer(
        () -> {
          checkGroupName(group, op);
          checkStreamName(stream, op);
          return ignoring(
              call(
                  op,
                  c ->
                      c.deleteLogStream(
                          DeleteLogStreamRequest.builder()
                              .logGroupName(group)
                              .logStreamName(stream)
                              .build())),
              ignoreMissing);
        });
  }

  private PageFetcher<LogGroupInfo> groupPages(DescribeLogGroupsOptions o) {
    String op = "describeLogGroups";
    checkRange(o.pageSize(), 1, 50, "pageSize", op);
    checkRange(o.limit(), 1, Integer.MAX_VALUE, "limit", op);
    return (token, remaining) -> {
      DescribeLogGroupsRequest.Builder req =
          DescribeLogGroupsRequest.builder().limit(o.pageSize() != null ? o.pageSize() : 50);
      if (o.prefix() != null) {
        req.logGroupNamePrefix(o.prefix());
      }
      if (token != null) {
        req.nextToken(token);
      }
      return call(op, c -> c.describeLogGroups(req.build()))
          .thenApply(
              out -> {
                List<LogGroupInfo> items = new ArrayList<>();
                out.logGroups().forEach(g -> items.add(toGroup(g)));
                return new Page<>(items, blankToNull(out.nextToken()));
              });
    };
  }

  private static String blankToNull(String s) {
    return s == null || s.isEmpty() ? null : s;
  }

  /**
   * Iterates log groups (sorted by name) across pages, lazily.
   *
   * @param options prefix, page size, limit
   * @return the groups
   */
  public Iterable<LogGroupInfo> describeLogGroups(DescribeLogGroupsOptions options) {
    DescribeLogGroupsOptions o = options == null ? DescribeLogGroupsOptions.defaults() : options;
    return new PagedIterable<>("describeLogGroups", groupPages(o), o.limit());
  }

  /**
   * Asynchronous {@link #describeLogGroups(DescribeLogGroupsOptions)}: collects every group (up to
   * {@code limit}).
   *
   * @param options prefix, page size, limit
   * @return a future with the groups
   */
  public CompletableFuture<List<LogGroupInfo>> describeLogGroupsAsync(
      DescribeLogGroupsOptions options) {
    DescribeLogGroupsOptions o = options == null ? DescribeLogGroupsOptions.defaults() : options;
    return defer(() -> collect(groupPages(o), o.limit()));
  }

  private PageFetcher<LogStreamInfo> streamPages(String group, DescribeLogStreamsOptions o) {
    String op = "describeLogStreams";
    checkGroupName(group, op);
    checkRange(o.pageSize(), 1, 50, "pageSize", op);
    checkRange(o.limit(), 1, Integer.MAX_VALUE, "limit", op);
    if (o.orderBy() == LogStreamOrder.LAST_EVENT_TIME && o.prefix() != null) {
      throw new ValidationException(op + ": prefix cannot be combined with orderBy LastEventTime");
    }
    return (token, remaining) -> {
      DescribeLogStreamsRequest.Builder req =
          DescribeLogStreamsRequest.builder()
              .logGroupName(group)
              .limit(o.pageSize() != null ? o.pageSize() : 50);
      if (o.prefix() != null) {
        req.logStreamNamePrefix(o.prefix());
      }
      if (o.orderBy() != null) {
        req.orderBy(o.orderBy().wireValue());
      }
      if (o.descending() != null) {
        req.descending(o.descending());
      }
      if (token != null) {
        req.nextToken(token);
      }
      return call(op, c -> c.describeLogStreams(req.build()))
          .thenApply(
              out -> {
                List<LogStreamInfo> items = new ArrayList<>();
                out.logStreams().forEach(s -> items.add(toStream(s)));
                return new Page<>(items, blankToNull(out.nextToken()));
              });
    };
  }

  /**
   * Iterates the log streams of a group across pages, lazily.
   *
   * @param group group name
   * @param options prefix, order, page size, limit
   * @return the streams
   */
  public Iterable<LogStreamInfo> describeLogStreams(
      String group, DescribeLogStreamsOptions options) {
    DescribeLogStreamsOptions o = options == null ? DescribeLogStreamsOptions.defaults() : options;
    return new PagedIterable<>("describeLogStreams", streamPages(group, o), o.limit());
  }

  /**
   * {@link #describeLogStreams(String, DescribeLogStreamsOptions)} with default options.
   *
   * @param group group name
   * @return the streams
   */
  public Iterable<LogStreamInfo> describeLogStreams(String group) {
    return describeLogStreams(group, DescribeLogStreamsOptions.defaults());
  }

  /**
   * Asynchronous {@link #describeLogStreams(String, DescribeLogStreamsOptions)}: collects every
   * stream (up to {@code limit}).
   *
   * @param group group name
   * @param options prefix, order, page size, limit
   * @return a future with the streams
   */
  public CompletableFuture<List<LogStreamInfo>> describeLogStreamsAsync(
      String group, DescribeLogStreamsOptions options) {
    DescribeLogStreamsOptions o = options == null ? DescribeLogStreamsOptions.defaults() : options;
    return defer(() -> collect(streamPages(group, o), o.limit()));
  }

  private CompletableFuture<String> resolveArn(String op, String group) {
    if (group != null && group.startsWith("arn:")) {
      return CompletableFuture.completedFuture(stripStar(group));
    }
    checkGroupName(group, op);
    return requireGroup(group, op).thenApply(LogGroupInfo::arn);
  }

  private CompletableFuture<Void> tagArn(String op, String arn, Map<String, String> tags) {
    return call(
            op,
            c -> c.tagResource(TagResourceRequest.builder().resourceArn(arn).tags(tags).build()))
        .thenApply(r -> null);
  }

  /**
   * Adds or overwrites tags of a log group ({@code TagResource}).
   *
   * @param group group name or ARN
   * @param tags tags
   */
  public void tagLogGroup(String group, Map<String, String> tags) {
    await(tagLogGroupAsync(group, tags), "tagLogGroup");
  }

  /**
   * Asynchronous {@link #tagLogGroup(String, Map)}.
   *
   * @param group group name or ARN
   * @param tags tags
   * @return a future that completes when done
   */
  public CompletableFuture<Void> tagLogGroupAsync(String group, Map<String, String> tags) {
    String op = "tagLogGroup";
    return defer(() -> resolveArn(op, group).thenCompose(arn -> tagArn(op, arn, tags)));
  }

  /**
   * Removes tags from a log group ({@code UntagResource}).
   *
   * @param group group name or ARN
   * @param keys tag keys
   */
  public void untagLogGroup(String group, List<String> keys) {
    await(untagLogGroupAsync(group, keys), "untagLogGroup");
  }

  /**
   * Asynchronous {@link #untagLogGroup(String, List)}.
   *
   * @param group group name or ARN
   * @param keys tag keys
   * @return a future that completes when done
   */
  public CompletableFuture<Void> untagLogGroupAsync(String group, List<String> keys) {
    String op = "untagLogGroup";
    return defer(
        () ->
            resolveArn(op, group)
                .thenCompose(
                    arn ->
                        call(
                            op,
                            c ->
                                c.untagResource(
                                    UntagResourceRequest.builder()
                                        .resourceArn(arn)
                                        .tagKeys(keys)
                                        .build())))
                .thenApply(r -> null));
  }

  /**
   * Returns the tags of a log group ({@code ListTagsForResource}).
   *
   * @param group group name or ARN
   * @return the tags
   */
  public Map<String, String> listLogGroupTags(String group) {
    return await(listLogGroupTagsAsync(group), "listLogGroupTags");
  }

  /**
   * Asynchronous {@link #listLogGroupTags(String)}.
   *
   * @param group group name or ARN
   * @return a future with the tags
   */
  public CompletableFuture<Map<String, String>> listLogGroupTagsAsync(String group) {
    String op = "listLogGroupTags";
    return defer(
        () ->
            resolveArn(op, group)
                .thenCompose(
                    arn ->
                        call(
                            op,
                            c ->
                                c.listTagsForResource(
                                    ListTagsForResourceRequest.builder().resourceArn(arn).build())))
                .thenApply(r -> Map.copyOf(r.tags())));
  }

  // ------------------------------------------------------------------ ingest

  /**
   * Sends events with {@code PutLogEvents}. Events are sorted, oversized events are truncated /
   * dropped / rejected per {@code oversizePolicy}, and the events are split into batches that
   * respect every service limit (10 000 events, 1 MiB incl. 26 bytes per event, 24 h span). A
   * missing stream is created when {@code autoCreate} is on. Events the service rejects (too old,
   * too new, expired) are reported, never retried. Throttling and 5xx are retried by the AWS SDK (a
   * lost response may duplicate events).
   *
   * @param group group name
   * @param stream stream name
   * @param events events in any order
   * @param options auto-create and size policy
   * @return accepted, rejected, truncated and dropped counts
   */
  public PutLogEventsResult putLogEvents(
      String group, String stream, List<LogEvent> events, PutLogEventsOptions options) {
    return await(putLogEventsAsync(group, stream, events, options), "putLogEvents");
  }

  /**
   * {@link #putLogEvents(String, String, List, PutLogEventsOptions)} with default options.
   *
   * @param group group name
   * @param stream stream name
   * @param events events in any order
   * @return the result
   */
  public PutLogEventsResult putLogEvents(String group, String stream, List<LogEvent> events) {
    return putLogEvents(group, stream, events, PutLogEventsOptions.defaults());
  }

  /**
   * Asynchronous {@link #putLogEvents(String, String, List, PutLogEventsOptions)}.
   *
   * @param group group name
   * @param stream stream name
   * @param events events in any order
   * @param options auto-create and size policy
   * @return a future with the result
   */
  public CompletableFuture<PutLogEventsResult> putLogEventsAsync(
      String group, String stream, List<LogEvent> events, PutLogEventsOptions options) {
    String op = "putLogEvents";
    PutLogEventsOptions o = options == null ? PutLogEventsOptions.defaults() : options;
    return defer(
        () -> {
          checkGroupName(group, op);
          checkStreamName(stream, op);
          LogEventBatching.BatchPlan plan =
              LogEventBatching.plan(
                  events,
                  o.maxEventBytes() != null ? o.maxEventBytes() : config.maxEventBytes(),
                  o.oversizePolicy() != null ? o.oversizePolicy() : config.oversizePolicy());
          boolean autoCreate = o.autoCreate() != null ? o.autoCreate() : config.autoCreate();
          boolean createGroup = Boolean.TRUE.equals(o.createGroup());
          int[] counts = new int[3]; // accepted, rejected, batches
          List<RejectedLogEventsInfo> rejected = new ArrayList<>();
          CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
          for (int i = 0; i < plan.batches().size(); i++) {
            int index = i;
            List<LogEventBatching.WireEvent> batch = plan.batches().get(i);
            chain =
                chain.thenCompose(
                    x ->
                        sendBatch(op, group, stream, batch, autoCreate, createGroup)
                            .thenAccept(
                                out -> {
                                  counts[2]++;
                                  int r = rejectedCount(index, batch.size(), out, rejected);
                                  counts[1] += r;
                                  counts[0] += batch.size() - r;
                                }));
          }
          return chain.thenApply(
              x ->
                  new PutLogEventsResult(
                      counts[0], counts[1], plan.truncated(), plan.dropped(), counts[2], rejected));
        });
  }

  private CompletableFuture<PutLogEventsResponse> sendBatch(
      String op,
      String group,
      String stream,
      List<LogEventBatching.WireEvent> batch,
      boolean autoCreate,
      boolean createGroup) {
    List<InputLogEvent> wire = new ArrayList<>(batch.size());
    for (LogEventBatching.WireEvent e : batch) {
      wire.add(InputLogEvent.builder().timestamp(e.timestamp()).message(e.message()).build());
    }
    PutLogEventsRequest req =
        PutLogEventsRequest.builder()
            .logGroupName(group)
            .logStreamName(stream)
            .logEvents(wire)
            .build();
    return call(op, c -> c.putLogEvents(req))
        .handle(
            (out, t) -> {
              if (t == null) {
                return CompletableFuture.completedFuture(out);
              }
              CloudWatchLogsException e = HttpTransport.asSdkException(t, op);
              if (!(autoCreate && e instanceof NotFoundException)) {
                return CompletableFuture.<PutLogEventsResponse>failedFuture(e);
              }
              return ensureLogStreamAsync(group, stream, createGroup)
                  .handle(
                      (r, ct) -> {
                        if (ct == null) {
                          return call(op, c -> c.putLogEvents(req));
                        }
                        CloudWatchLogsException ce = HttpTransport.asSdkException(ct, op);
                        return CompletableFuture.<PutLogEventsResponse>failedFuture(
                            ce instanceof NotFoundException ? e : ce);
                      })
                  .thenCompose(f -> f);
            })
        .thenCompose(f -> f);
  }

  private static int rejectedCount(
      int index, int size, PutLogEventsResponse out, List<RejectedLogEventsInfo> into) {
    var info = out.rejectedLogEventsInfo();
    if (info == null) {
      return 0;
    }
    int oldEnd = -1;
    int newStart = size;
    if (info.tooOldLogEventEndIndex() != null) {
      oldEnd = Math.max(oldEnd, info.tooOldLogEventEndIndex());
    }
    if (info.expiredLogEventEndIndex() != null) {
      oldEnd = Math.max(oldEnd, info.expiredLogEventEndIndex());
    }
    if (info.tooNewLogEventStartIndex() != null) {
      newStart = Math.min(newStart, info.tooNewLogEventStartIndex());
    }
    into.add(
        new RejectedLogEventsInfo(
            index,
            optInt(info.tooOldLogEventEndIndex()),
            optInt(info.tooNewLogEventStartIndex()),
            optInt(info.expiredLogEventEndIndex())));
    // AWS documents the "end index" fields as inclusive.
    int old = Math.min(size, oldEnd + 1);
    int tooNew = Math.max(0, size - Math.max(newStart, old));
    return old + tooNew;
  }

  private static java.util.OptionalInt optInt(Integer v) {
    return v == null ? java.util.OptionalInt.empty() : java.util.OptionalInt.of(v);
  }

  /**
   * Builds an Embedded Metric Format document ({@link Emf#build}) and sends it as one log event to
   * {@code options.group}/{@code options.stream} (defaults {@code config.logGroup} / {@code
   * config.logStream}).
   *
   * @param metric metric name
   * @param value metric value
   * @param dimensions dimensions (at most 30)
   * @param options namespace, unit, timestamp, properties, target
   * @return the document sent
   */
  public Map<String, Object> emf(
      String metric, double value, Map<String, String> dimensions, Emf.Options options) {
    return await(emfAsync(metric, value, dimensions, options), "emf");
  }

  /**
   * Asynchronous {@link #emf(String, double, Map, Emf.Options)}.
   *
   * @param metric metric name
   * @param value metric value
   * @param dimensions dimensions (at most 30)
   * @param options namespace, unit, timestamp, properties, target
   * @return a future with the document sent
   */
  public CompletableFuture<Map<String, Object>> emfAsync(
      String metric, double value, Map<String, String> dimensions, Emf.Options options) {
    Emf.Options o = options == null ? Emf.Options.defaults() : options;
    return defer(
        () -> {
          String g = group(o.group(), "emf");
          Map<String, Object> doc = Emf.build(metric, value, dimensions, o);
          @SuppressWarnings("unchecked")
          long ts = (Long) ((Map<String, Object>) doc.get("_aws")).get("Timestamp");
          return putLogEventsAsync(
                  g,
                  o.stream() != null ? o.stream() : config.logStream(),
                  List.of(new LogEvent(Instant.ofEpochMilli(ts), Emf.toJson(doc))),
                  PutLogEventsOptions.builder().oversizePolicy(OversizePolicy.ERROR).build())
              .thenApply(
                  r -> {
                    if (r.rejected() > 0) {
                      throw new ValidationException("emf: the service rejected the event");
                    }
                    return doc;
                  });
        });
  }

  // ------------------------------------------------------------------ query

  private static final Comparator<FilteredLogEvent> ASCENDING =
      Comparator.comparing(FilteredLogEvent::timestamp).thenComparing(e -> e.eventId().orElse(""));

  /**
   * One {@code FilterLogEvents} request. Events are ordered by timestamp, then event id.
   *
   * @param options group, streams, pattern, range, page size
   * @param nextToken token of the previous page, or null
   * @return the page
   */
  public FilterLogEventsPage filterLogEventsPage(FilterLogEventsOptions options, String nextToken) {
    return await(filterLogEventsPageAsync(options, nextToken), "filterLogEvents");
  }

  /**
   * Asynchronous {@link #filterLogEventsPage(FilterLogEventsOptions, String)}.
   *
   * @param options group, streams, pattern, range, page size
   * @param nextToken token of the previous page, or null
   * @return a future with the page
   */
  public CompletableFuture<FilterLogEventsPage> filterLogEventsPageAsync(
      FilterLogEventsOptions options, String nextToken) {
    FilterLogEventsOptions o = options == null ? FilterLogEventsOptions.defaults() : options;
    return defer(() -> filterPage(o, nextToken, o.pageSize()));
  }

  private CompletableFuture<FilterLogEventsPage> filterPage(
      FilterLogEventsOptions o, String nextToken, Integer pageSize) {
    String op = "filterLogEvents";
    String group = group(o.group(), op);
    checkRange(pageSize, 1, 10_000, "pageSize", op);
    if (o.streams() != null && o.streamPrefix() != null) {
      throw new ValidationException(op + ": streams and streamPrefix are mutually exclusive");
    }
    if (o.streams() != null && (o.streams().isEmpty() || o.streams().size() > 100)) {
      throw new ValidationException(op + ": streams must hold 1-100 names");
    }
    FilterLogEventsRequest.Builder req = FilterLogEventsRequest.builder();
    if (group.startsWith("arn:")) {
      req.logGroupIdentifier(stripStar(group));
    } else {
      req.logGroupName(group);
    }
    if (o.streams() != null) {
      req.logStreamNames(o.streams());
    }
    if (o.streamPrefix() != null) {
      req.logStreamNamePrefix(o.streamPrefix());
    }
    if (o.pattern() != null && !o.pattern().isEmpty()) {
      req.filterPattern(o.pattern());
    }
    if (o.start() != null) {
      req.startTime(o.start().toEpochMilli());
    }
    if (o.end() != null) {
      req.endTime(o.end().toEpochMilli());
    }
    if (pageSize != null) {
      req.limit(pageSize);
    }
    if (nextToken != null) {
      req.nextToken(nextToken);
    }
    return call(op, c -> c.filterLogEvents(req.build()))
        .thenApply(
            out -> {
              List<FilteredLogEvent> events = new ArrayList<>();
              for (var ev : out.events()) {
                events.add(
                    new FilteredLogEvent(
                        Instant.ofEpochMilli(ev.timestamp() == null ? 0 : ev.timestamp()),
                        ev.message() == null ? "" : ev.message(),
                        ms(ev.ingestionTime()),
                        Optional.ofNullable(ev.logStreamName()),
                        Optional.ofNullable(ev.eventId())));
              }
              events.sort(ASCENDING);
              return new FilterLogEventsPage(
                  events, Optional.ofNullable(blankToNull(out.nextToken())));
            });
  }

  private PageFetcher<FilteredLogEvent> filterPages(FilterLogEventsOptions o) {
    checkRange(o.limit(), 1, Integer.MAX_VALUE, "limit", "filterLogEvents");
    return (token, remaining) -> {
      Integer size =
          remaining == null
              ? o.pageSize()
              : Integer.valueOf(Math.min(remaining, o.pageSize() != null ? o.pageSize() : 10_000));
      return filterPage(o, token, size)
          .thenApply(p -> new Page<>(p.events(), p.nextToken().orElse(null)));
    };
  }

  /**
   * Iterates events matching a filter pattern ({@code FilterLogEvents}) across pages, lazily, until
   * no token is left (or {@code limit}). Within a page events are ordered by timestamp, then event
   * id.
   *
   * @param options group, streams, pattern, range, page size, limit
   * @return the events
   */
  public Iterable<FilteredLogEvent> filterLogEvents(FilterLogEventsOptions options) {
    FilterLogEventsOptions o = options == null ? FilterLogEventsOptions.defaults() : options;
    return new PagedIterable<>("filterLogEvents", filterPages(o), o.limit());
  }

  /**
   * Asynchronous {@link #filterLogEvents(FilterLogEventsOptions)}: collects every event (up to
   * {@code limit}).
   *
   * @param options group, streams, pattern, range, page size, limit
   * @return a future with the events
   */
  public CompletableFuture<List<FilteredLogEvent>> filterLogEventsAsync(
      FilterLogEventsOptions options) {
    FilterLogEventsOptions o = options == null ? FilterLogEventsOptions.defaults() : options;
    return defer(() -> collect(filterPages(o), o.limit()));
  }

  private PageFetcher<StreamLogEvent> eventPages(
      String group, String stream, GetLogEventsOptions o) {
    String op = "getLogEvents";
    checkGroupName(group, op);
    checkStreamName(stream, op);
    checkRange(o.pageSize(), 1, 10_000, "pageSize", op);
    checkRange(o.limit(), 1, Integer.MAX_VALUE, "limit", op);
    boolean forward = Boolean.TRUE.equals(o.startFromHead());
    return (token, remaining) -> {
      GetLogEventsRequest.Builder req =
          GetLogEventsRequest.builder()
              .logGroupName(group)
              .logStreamName(stream)
              .startFromHead(forward);
      if (o.start() != null) {
        req.startTime(o.start().toEpochMilli());
      }
      if (o.end() != null) {
        req.endTime(o.end().toEpochMilli());
      }
      if (o.pageSize() != null) {
        req.limit(o.pageSize());
      }
      if (token != null) {
        req.nextToken(token);
      }
      return call(op, c -> c.getLogEvents(req.build()))
          .thenApply(
              out -> {
                List<StreamLogEvent> items = new ArrayList<>();
                for (var ev : out.events()) {
                  items.add(
                      new StreamLogEvent(
                          Instant.ofEpochMilli(ev.timestamp() == null ? 0 : ev.timestamp()),
                          ev.message() == null ? "" : ev.message(),
                          ms(ev.ingestionTime())));
                }
                String next =
                    blankToNull(forward ? out.nextForwardToken() : out.nextBackwardToken());
                // the service returns the token that was sent once the stream is exhausted
                if (next != null && (next.equals(token) || items.isEmpty())) {
                  next = null;
                }
                return new Page<>(items, next);
              });
    };
  }

  /**
   * Iterates the events of one stream ({@code GetLogEvents}), lazily. {@code startFromHead} reads
   * oldest to newest (forward tokens); the default reads the newest page first and then older pages
   * (backward tokens; events inside a page stay ascending). Iteration stops when the service
   * returns the token that was sent.
   *
   * @param group group name
   * @param stream stream name
   * @param options direction, range, page size, limit
   * @return the events
   */
  public Iterable<StreamLogEvent> getLogEvents(
      String group, String stream, GetLogEventsOptions options) {
    GetLogEventsOptions o = options == null ? GetLogEventsOptions.defaults() : options;
    return new PagedIterable<>("getLogEvents", eventPages(group, stream, o), o.limit());
  }

  /**
   * Asynchronous {@link #getLogEvents(String, String, GetLogEventsOptions)}: collects every event
   * (up to {@code limit}).
   *
   * @param group group name
   * @param stream stream name
   * @param options direction, range, page size, limit
   * @return a future with the events
   */
  public CompletableFuture<List<StreamLogEvent>> getLogEventsAsync(
      String group, String stream, GetLogEventsOptions options) {
    GetLogEventsOptions o = options == null ? GetLogEventsOptions.defaults() : options;
    return defer(() -> collect(eventPages(group, stream, o), o.limit()));
  }

  private StartQueryRequest queryRequest(String queryString, QueryOptions o, String op) {
    if (queryString == null || queryString.trim().isEmpty()) {
      throw new ValidationException(op + ": queryString must not be empty");
    }
    List<String> groups = o.groups() != null ? o.groups() : List.of(group(null, op));
    if (groups.isEmpty() || groups.size() > 50) {
      throw new ValidationException(op + ": groups must hold 1-50 log groups");
    }
    checkRange(o.limit(), 1, 10_000, "limit", op);
    long endMs = o.end() != null ? o.end().toEpochMilli() : System.currentTimeMillis();
    long startMs = o.start() != null ? o.start().toEpochMilli() : endMs - 3_600_000;
    if (startMs > endMs) {
      throw new ValidationException(op + ": start must not be after end");
    }
    StartQueryRequest.Builder req =
        StartQueryRequest.builder()
            .queryString(queryString)
            // StartQuery takes epoch SECONDS.
            .startTime(Math.floorDiv(startMs, 1000))
            .endTime(Math.floorDiv(endMs + 999, 1000));
    if (groups.stream().anyMatch(g -> g.startsWith("arn:"))) {
      List<String> ids = new ArrayList<>();
      groups.forEach(g -> ids.add(stripStar(g)));
      req.logGroupIdentifiers(ids);
    } else {
      req.logGroupNames(groups);
    }
    if (o.limit() != null) {
      req.limit(o.limit());
    }
    return req.build();
  }

  /**
   * Starts a Logs Insights query ({@code StartQuery}).
   *
   * @param queryString Logs Insights query
   * @param options groups, range, limit
   * @return the query id
   */
  public String startQuery(String queryString, QueryOptions options) {
    return await(startQueryAsync(queryString, options), "startQuery");
  }

  /**
   * Asynchronous {@link #startQuery(String, QueryOptions)}.
   *
   * @param queryString Logs Insights query
   * @param options groups, range, limit
   * @return a future with the query id
   */
  public CompletableFuture<String> startQueryAsync(String queryString, QueryOptions options) {
    QueryOptions o = options == null ? QueryOptions.defaults() : options;
    return defer(
        () -> {
          StartQueryRequest req = queryRequest(queryString, o, "startQuery");
          return call("startQuery", c -> c.startQuery(req))
              .thenApply(r -> queryId("startQuery", r.queryId()));
        });
  }

  private static String queryId(String op, String id) {
    if (id == null || id.isEmpty()) {
      throw new UnavailableException(op + ": no query id returned");
    }
    return id;
  }

  /**
   * Reads the status and rows of a Logs Insights query ({@code GetQueryResults}).
   *
   * @param queryId query id
   * @return status, rows and statistics
   */
  public QueryResults getQueryResults(String queryId) {
    return await(getQueryResultsAsync(queryId), "getQueryResults");
  }

  /**
   * Asynchronous {@link #getQueryResults(String)}.
   *
   * @param queryId query id
   * @return a future with the results
   */
  public CompletableFuture<QueryResults> getQueryResultsAsync(String queryId) {
    String op = "getQueryResults";
    return defer(
        () -> {
          if (queryId == null || queryId.isEmpty()) {
            throw new ValidationException(op + ": queryId must not be empty");
          }
          return call(
                  op,
                  c -> c.getQueryResults(GetQueryResultsRequest.builder().queryId(queryId).build()))
              .thenApply(
                  out -> {
                    List<Map<String, String>> rows = new ArrayList<>();
                    for (var row : out.results()) {
                      Map<String, String> r = new LinkedHashMap<>();
                      for (var f : row) {
                        if (f.field() != null) {
                          r.put(f.field(), f.value() == null ? "" : f.value());
                        }
                      }
                      rows.add(java.util.Collections.unmodifiableMap(r));
                    }
                    var st = out.statistics();
                    Optional<QueryStatistics> stats =
                        st == null
                            ? Optional.empty()
                            : Optional.of(
                                new QueryStatistics(
                                    st.recordsMatched() == null ? 0 : st.recordsMatched(),
                                    st.recordsScanned() == null ? 0 : st.recordsScanned(),
                                    st.bytesScanned() == null ? 0 : st.bytesScanned()));
                    return new QueryResults(
                        queryId, QueryStatus.parse(out.statusAsString()), rows, stats);
                  });
        });
  }

  /**
   * Stops a running query ({@code StopQuery}).
   *
   * @param queryId query id
   * @return false when it had already finished
   */
  public boolean stopQuery(String queryId) {
    return await(stopQueryAsync(queryId), "stopQuery");
  }

  /**
   * Asynchronous {@link #stopQuery(String)}.
   *
   * @param queryId query id
   * @return a future with the result
   */
  public CompletableFuture<Boolean> stopQueryAsync(String queryId) {
    String op = "stopQuery";
    return defer(
        () -> {
          if (queryId == null || queryId.isEmpty()) {
            throw new ValidationException(op + ": queryId must not be empty");
          }
          return call(op, c -> c.stopQuery(StopQueryRequest.builder().queryId(queryId).build()))
              .thenApply(r -> Boolean.TRUE.equals(r.success()));
        });
  }

  /**
   * Runs a Logs Insights query to completion: {@code StartQuery}, then polls {@code
   * GetQueryResults} until a terminal status. {@code Failed}/{@code Unknown} raise {@link
   * UnavailableException}, {@code Timeout} {@link TimeoutException}, {@code Cancelled} {@link
   * CancelledException} (the message includes the query id). When {@code timeout} passes or the
   * returned future is cancelled, the query is stopped best-effort.
   *
   * @param queryString Logs Insights query
   * @param options groups, range, limit, timeout, poll interval
   * @return the completed results
   */
  public QueryResults query(String queryString, QueryOptions options) {
    return await(queryAsync(queryString, options), "query");
  }

  /**
   * Asynchronous {@link #query(String, QueryOptions)}.
   *
   * @param queryString Logs Insights query
   * @param options groups, range, limit, timeout, poll interval
   * @return a future with the completed results
   */
  public CompletableFuture<QueryResults> queryAsync(String queryString, QueryOptions options) {
    String op = "query";
    QueryOptions o = options == null ? QueryOptions.defaults() : options;
    CompletableFuture<QueryResults> result = new CompletableFuture<>();
    AtomicReference<String> id = new AtomicReference<>();
    result.whenComplete(
        (v, t) -> {
          if (result.isCancelled() && id.get() != null) {
            stopQuietly(id.get());
          }
        });
    StartQueryRequest req;
    try {
      req = queryRequest(queryString, o, op);
    } catch (RuntimeException e) {
      return failed(e);
    }
    long deadline = System.nanoTime() + o.timeout().toNanos();
    int attempts = Math.max(1, o.retryOnQueryLimit());
    startWithRetry(op, req, 1, attempts, deadline)
        .thenCompose(
            qid -> {
              id.set(qid);
              return poll(op, qid, o.pollInterval(), deadline, o.timeout(), result);
            })
        .whenComplete(
            (v, t) -> {
              if (t == null) {
                result.complete(v);
              } else {
                result.completeExceptionally(HttpTransport.asSdkException(t, op));
              }
            });
    return result;
  }

  private CompletableFuture<String> startWithRetry(
      String op, StartQueryRequest req, int attempt, int attempts, long deadline) {
    return call(op, c -> c.startQuery(req))
        .thenApply(r -> queryId(op, r.queryId()))
        .handle(
            (qid, t) -> {
              if (t == null) {
                return CompletableFuture.completedFuture(qid);
              }
              boolean limit =
                  AwsErrors.awsErrorCode(t).map("LimitExceededException"::equals).orElse(false);
              if (!limit || attempt >= attempts || System.nanoTime() >= deadline) {
                return CompletableFuture.<String>failedFuture(t);
              }
              long wait = Math.min(1000L << (attempt - 1), 5000L);
              return internals
                  .sleeper
                  .apply(Duration.ofMillis(wait))
                  .thenCompose(x -> startWithRetry(op, req, attempt + 1, attempts, deadline));
            })
        .thenCompose(f -> f);
  }

  private CompletableFuture<QueryResults> poll(
      String op,
      String qid,
      Duration interval,
      long deadline,
      Duration timeout,
      CompletableFuture<?> outer) {
    if (outer.isDone()) {
      return failed(new CancelledException(op + ": cancelled"));
    }
    return getQueryResultsAsync(qid)
        .thenCompose(
            res -> {
              switch (res.status()) {
                case COMPLETE:
                  return CompletableFuture.completedFuture(res);
                case TIMEOUT:
                  return failed(
                      new TimeoutException(op + ": query " + qid + " timed out in the service"));
                case CANCELLED:
                  return failed(new CancelledException(op + ": query " + qid + " was cancelled"));
                case FAILED:
                case UNKNOWN:
                  return failed(
                      new UnavailableException(
                          op + ": query " + qid + " ended with status " + res.status().wireValue(),
                          null,
                          null,
                          null,
                          null));
                default:
                  long remaining = deadline - System.nanoTime();
                  if (remaining <= 0) {
                    stopQuietly(qid);
                    return failed(
                        new TimeoutException(
                            op
                                + ": query "
                                + qid
                                + " not complete within "
                                + timeout.toMillis()
                                + " ms"));
                  }
                  Duration wait =
                      interval.toNanos() < remaining ? interval : Duration.ofNanos(remaining);
                  Duration next =
                      interval.multipliedBy(2).compareTo(Duration.ofSeconds(5)) > 0
                          ? Duration.ofSeconds(5)
                          : interval.multipliedBy(2);
                  return internals
                      .sleeper
                      .apply(wait)
                      .thenCompose(x -> poll(op, qid, next, deadline, timeout, outer));
              }
            });
  }

  private void stopQuietly(String queryId) {
    stopQueryAsync(queryId).exceptionally(t -> false);
  }

  /**
   * Lists recent Logs Insights queries ({@code DescribeQueries}).
   *
   * @param options group, status, max results
   * @return the queries
   */
  public List<QueryInfo> describeQueries(DescribeQueriesOptions options) {
    return await(describeQueriesAsync(options), "describeQueries");
  }

  /**
   * Asynchronous {@link #describeQueries(DescribeQueriesOptions)}.
   *
   * @param options group, status, max results
   * @return a future with the queries
   */
  public CompletableFuture<List<QueryInfo>> describeQueriesAsync(DescribeQueriesOptions options) {
    String op = "describeQueries";
    DescribeQueriesOptions o = options == null ? DescribeQueriesOptions.defaults() : options;
    return defer(
        () -> {
          checkRange(o.maxResults(), 1, 1000, "maxResults", op);
          DescribeQueriesRequest.Builder req = DescribeQueriesRequest.builder();
          if (o.group() != null) {
            req.logGroupName(o.group());
          }
          if (o.status() != null) {
            req.status(o.status().wireValue());
          }
          if (o.maxResults() != null) {
            req.maxResults(o.maxResults());
          }
          return call(op, c -> c.describeQueries(req.build()))
              .thenApply(
                  out -> {
                    List<QueryInfo> list = new ArrayList<>();
                    for (var q : out.queries()) {
                      list.add(
                          new QueryInfo(
                              q.queryId() == null ? "" : q.queryId(),
                              q.queryString() == null ? "" : q.queryString(),
                              QueryStatus.parse(q.statusAsString()),
                              q.createTime() == null
                                  ? Optional.empty()
                                  : Optional.of(Instant.ofEpochMilli(q.createTime())),
                              Optional.ofNullable(q.logGroupName())));
                    }
                    return list;
                  });
        });
  }

  // ------------------------------------------------------------------ live tail

  /**
   * Returns the ARN of a log group (without {@code :*}): built from {@code config.accountId} and
   * the region when the account is configured, otherwise looked up with {@code DescribeLogGroups}.
   * An ARN is returned without {@code :*}.
   *
   * @param group group name or ARN
   * @return the ARN
   */
  public String logGroupArn(String group) {
    return await(logGroupArnAsync(group), "logGroupArn");
  }

  /**
   * Asynchronous {@link #logGroupArn(String)}.
   *
   * @param group group name or ARN
   * @return a future with the ARN
   */
  public CompletableFuture<String> logGroupArnAsync(String group) {
    String op = "logGroupArn";
    return defer(
        () -> {
          if (group != null && group.startsWith("arn:")) {
            return CompletableFuture.completedFuture(stripStar(group));
          }
          checkGroupName(group, op);
          if (config.accountId().isPresent() && region != null) {
            return CompletableFuture.completedFuture(
                "arn:"
                    + partitionForRegion(region)
                    + ":logs:"
                    + region
                    + ":"
                    + config.accountId().get()
                    + ":log-group:"
                    + group);
          }
          return requireGroup(group, op).thenApply(LogGroupInfo::arn);
        });
  }

  /**
   * Starts a Live Tail session ({@code StartLiveTail}) and calls {@code onEvent} for every event.
   * Returns once the session has started ({@code sessionStart}), with a handle to stop it. On AWS
   * the session streams new events for up to 3 hours; the Moto emulator replays matching existing
   * events once and ends. Callbacks run on SDK threads and must not block.
   *
   * @param request groups, streams, pattern
   * @param onEvent called for every event
   * @param options error and update callbacks
   * @return the session handle
   */
  public LiveTailHandle startLiveTail(
      LiveTailRequest request, Consumer<LiveTailEvent> onEvent, LiveTailOptions options) {
    return await(startLiveTailAsync(request, onEvent, options), "startLiveTail");
  }

  /**
   * {@link #startLiveTail(LiveTailRequest, Consumer, LiveTailOptions)} without callbacks.
   *
   * @param request groups, streams, pattern
   * @param onEvent called for every event
   * @return the session handle
   */
  public LiveTailHandle startLiveTail(LiveTailRequest request, Consumer<LiveTailEvent> onEvent) {
    return startLiveTail(request, onEvent, LiveTailOptions.defaults());
  }

  /**
   * Asynchronous {@link #startLiveTail(LiveTailRequest, Consumer, LiveTailOptions)}: completes once
   * the session has started.
   *
   * @param request groups, streams, pattern
   * @param onEvent called for every event
   * @param options error and update callbacks
   * @return a future with the session handle
   */
  public CompletableFuture<LiveTailHandle> startLiveTailAsync(
      LiveTailRequest request, Consumer<LiveTailEvent> onEvent, LiveTailOptions options) {
    String op = "startLiveTail";
    LiveTailOptions o = options == null ? LiveTailOptions.defaults() : options;
    return defer(
        () -> {
          List<String> groups = request == null ? null : request.groups();
          if (groups == null || groups.isEmpty() || groups.size() > 10) {
            throw new ValidationException(op + ": groups must hold 1-10 log groups");
          }
          if (request.streams() != null && request.streamPrefixes() != null) {
            throw new ValidationException(
                op + ": streams and streamPrefixes are mutually exclusive");
          }
          if ((request.streams() != null || request.streamPrefixes() != null)
              && groups.size() != 1) {
            throw new ValidationException(
                op + ": streams and streamPrefixes need exactly one log group");
          }
          CompletableFuture<List<String>> arns =
              CompletableFuture.completedFuture(new ArrayList<>());
          for (String g : groups) {
            arns =
                arns.thenCompose(
                    list ->
                        logGroupArnAsync(g)
                            .thenApply(
                                a -> {
                                  list.add(a);
                                  return list;
                                }));
          }
          AtomicReference<LiveTailHandle> pending = new AtomicReference<>();
          CompletableFuture<LiveTailHandle> inner =
              arns.thenCompose(list -> openTail(op, list, request, onEvent, o, pending::set));
          CompletableFuture<LiveTailHandle> out = new CompletableFuture<>();
          inner.whenComplete(
              (h, t) -> {
                if (t != null) {
                  out.completeExceptionally(t);
                } else if (!out.complete(h)) {
                  h.close();
                }
              });
          out.whenComplete(
              (h, t) -> {
                LiveTailHandle p = pending.get();
                if (out.isCancelled() && p != null) {
                  p.close();
                }
              });
          return out;
        });
  }

  private CompletableFuture<LiveTailHandle> openTail(
      String op,
      List<String> arns,
      LiveTailRequest request,
      Consumer<LiveTailEvent> onEvent,
      LiveTailOptions o,
      Consumer<LiveTailHandle> register) {
    StartLiveTailRequest.Builder req = StartLiveTailRequest.builder().logGroupIdentifiers(arns);
    if (request.streams() != null) {
      req.logStreamNames(request.streams());
    }
    if (request.streamPrefixes() != null) {
      req.logStreamNamePrefixes(request.streamPrefixes());
    }
    if (request.pattern() != null && !request.pattern().isEmpty()) {
      req.logEventFilterPattern(request.pattern());
    }
    String sessionKey = java.util.UUID.randomUUID().toString();
    if (http != null) {
      req.overrideConfiguration(c -> c.putHeader(GrowHttpClient.SESSION_HEADER, sessionKey));
    }
    CompletableFuture<LiveTailHandle> started = new CompletableFuture<>();
    AtomicReference<LiveTailHandle> ref = new AtomicReference<>();
    LiveTailHandle handle =
        new LiveTailHandle(
            () -> {
              tails.remove(ref.get());
              if (http != null) {
                http.abort(sessionKey);
              }
            });
    ref.set(handle);
    register.accept(handle);
    Consumer<CloudWatchLogsException> end =
        failure -> {
          if (failure != null
              && !handle.isClosed()
              && !handle.stoppedByCaller()
              && o.onError() != null) {
            safe(() -> o.onError().accept(failure));
          }
          handle.finish(failure);
        };
    Subscriber<StartLiveTailResponseStream> subscriber =
        new Subscriber<>() {
          private Subscription subscription;

          @Override
          public void onSubscribe(Subscription s) {
            subscription = s;
            handle.subscription(s);
            s.request(1);
          }

          @Override
          public void onNext(StartLiveTailResponseStream frame) {
            if (frame instanceof LiveTailSessionStart) {
              handle.sessionId(((LiveTailSessionStart) frame).sessionId());
              started.complete(handle);
            } else if (frame instanceof LiveTailSessionUpdate) {
              deliver((LiveTailSessionUpdate) frame, onEvent, o);
            }
            subscription.request(1);
          }

          @Override
          public void onError(Throwable t) {
            // reported through the call future
          }

          @Override
          public void onComplete() {
            // reported through the call future
          }
        };
    StartLiveTailResponseHandler responseHandler =
        StartLiveTailResponseHandler.builder()
            .onEventStream(p -> p.subscribe(subscriber))
            .onError(t -> {})
            .build();
    CompletableFuture<Void> callFuture =
        call(op, false, null, c -> c.startLiveTail(req.build(), responseHandler));
    handle.call(callFuture);
    tails.add(handle);
    ScheduledFuture<?> startTimer =
        TIMERS.schedule(
            () -> {
              if (started.completeExceptionally(
                  new TimeoutException(op + ": the session did not start in time"))) {
                handle.close();
              }
            },
            config.totalTimeout().toMillis(),
            TimeUnit.MILLISECONDS);
    callFuture.whenComplete(
        (v, t) -> {
          startTimer.cancel(false);
          CloudWatchLogsException failure =
              t == null || handle.stoppedByCaller()
                  ? null
                  : AwsErrors.map(op, t, config.knownSecrets());
          if (failure != null && started.completeExceptionally(failure)) {
            handle.finish(null);
            return;
          }
          // the stream ended (before or after sessionStart)
          started.complete(handle);
          end.accept(failure);
        });
    return started;
  }

  private static void deliver(
      LiveTailSessionUpdate update, Consumer<LiveTailEvent> onEvent, LiveTailOptions o) {
    List<LiveTailSessionLogEvent> results = update.sessionResults();
    boolean sampled =
        update.sessionMetadata() != null && Boolean.TRUE.equals(update.sessionMetadata().sampled());
    if (o.onUpdate() != null) {
      safe(() -> o.onUpdate().accept(new LiveTailUpdate(sampled, results.size())));
    }
    for (LiveTailSessionLogEvent r : results) {
      LiveTailEvent ev =
          new LiveTailEvent(
              Instant.ofEpochMilli(r.timestamp() == null ? 0 : r.timestamp()),
              ms(r.ingestionTime()),
              r.message() == null ? "" : r.message(),
              Optional.ofNullable(r.logStreamName()),
              Optional.ofNullable(r.logGroupIdentifier()));
      safe(() -> onEvent.accept(ev));
    }
  }

  private static void safe(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException ignored) {
      // callbacks must never break the session
    }
  }

  // ------------------------------------------------------------------ logging category

  /**
   * Returns a {@link BatchingLogClient} that exports to the group/stream of {@code options}
   * (defaults {@code config.logGroup} / {@code config.logStream}; the stream is created on first
   * use). Records are sent as normalized LogRecord JSON. {@link #close()} flushes it too.
   *
   * @param options target, batching, resource, redaction
   * @return the log client
   */
  public BatchingLogClient logClient(CloudWatchLogClientOptions options) {
    CloudWatchLogClientOptions o =
        options == null ? CloudWatchLogClientOptions.defaults() : options;
    CloudWatchLogsExporter exporter = exporter(o.group(), o.stream(), o.createGroup());
    BatchingLogClient.Builder b =
        BatchingLogClient.builder(exporter).batchOptions(o.batchOptions()).resource(o.resource());
    if (o.redaction() != null) {
      b.redaction(o.redaction());
    }
    BatchingLogClient client = b.build();
    logClients.add(client);
    return client;
  }

  /**
   * {@link #logClient(CloudWatchLogClientOptions)} with default options.
   *
   * @return the log client
   */
  public BatchingLogClient logClient() {
    return logClient(CloudWatchLogClientOptions.defaults());
  }

  /**
   * Returns an exporter that writes records with {@code PutLogEvents}.
   *
   * @param group log group (null: {@code config.logGroup})
   * @param stream log stream (null: {@code config.logStream})
   * @param createGroup create a missing log group on the first export
   * @return the exporter
   */
  public CloudWatchLogsExporter exporter(String group, String stream, boolean createGroup) {
    String g = group(group, "logClient");
    checkGroupName(g, "logClient");
    String s = stream != null ? stream : config.logStream();
    checkStreamName(s, "logClient");
    return new CloudWatchLogsExporter(this, g, s, createGroup);
  }

  /**
   * Returns a query client over {@code group}.
   *
   * @param group log group (null: {@code config.logGroup})
   * @return the query client
   */
  public CloudWatchLogsQueryClient logQueryClient(String group) {
    String g = group(group, "logQueryClient");
    checkGroupName(g, "logQueryClient");
    return new CloudWatchLogsQueryClient(this, g);
  }

  /**
   * Returns a query client over {@code config.logGroup}.
   *
   * @return the query client
   */
  public CloudWatchLogsQueryClient logQueryClient() {
    return logQueryClient(null);
  }

  @Override
  public String toString() {
    return "CloudWatchLogsClient{" + config + "}";
  }

  /** Builder that configures and creates a client in one chain. */
  public static final class Builder {
    private final CloudWatchLogsConfig.Builder config = CloudWatchLogsConfig.builder();

    private Builder() {}

    /**
     * Fills unset fields from an environment reader.
     *
     * @param env reader, e.g. {@link EnvReader#system()}
     * @return this builder
     */
    public Builder fromEnv(EnvReader env) {
      config.fromEnv(env);
      return this;
    }

    /**
     * Changes the configuration builder in place (every {@link CloudWatchLogsConfig.Builder} setter
     * is available).
     *
     * @param change the change
     * @return this builder
     */
    public Builder config(Consumer<CloudWatchLogsConfig.Builder> change) {
      change.accept(config);
      return this;
    }

    /**
     * Sets the endpoint override.
     *
     * @param url URL
     * @return this builder
     */
    public Builder url(String url) {
      config.url(url);
      return this;
    }

    /**
     * Sets the AWS region.
     *
     * @param region region
     * @return this builder
     */
    public Builder region(String region) {
      config.region(region);
      return this;
    }

    /**
     * Sets static credentials.
     *
     * @param accessKeyId access key id
     * @param secretAccessKey secret access key
     * @return this builder
     */
    public Builder credentials(String accessKeyId, String secretAccessKey) {
      config.accessKeyId(accessKeyId).secretAccessKey(secretAccessKey);
      return this;
    }

    /**
     * Sets the default log group.
     *
     * @param group group name
     * @return this builder
     */
    public Builder logGroup(String group) {
      config.logGroup(group);
      return this;
    }

    /**
     * Sets the log stream of the log client.
     *
     * @param stream stream name
     * @return this builder
     */
    public Builder logStream(String stream) {
      config.logStream(stream);
      return this;
    }

    /**
     * Sets the connect timeout.
     *
     * @param d value
     * @return this builder
     */
    public Builder connectTimeout(Duration d) {
      config.connectTimeout(d);
      return this;
    }

    /**
     * Sets the per-attempt timeout.
     *
     * @param d value
     * @return this builder
     */
    public Builder requestTimeout(Duration d) {
      config.requestTimeout(d);
      return this;
    }

    /**
     * Sets the total timeout per call.
     *
     * @param d value
     * @return this builder
     */
    public Builder totalTimeout(Duration d) {
      config.totalTimeout(d);
      return this;
    }

    /**
     * Sets the retry policy.
     *
     * @param retry value
     * @return this builder
     */
    public Builder retry(codes.grow.packages.cloudwatchlogs.core.RetryPolicy retry) {
      config.retry(retry);
      return this;
    }

    /**
     * Sets the TLS options.
     *
     * @param tls value
     * @return this builder
     */
    public Builder tls(codes.grow.packages.cloudwatchlogs.core.TlsOptions tls) {
      config.tls(tls);
      return this;
    }

    /**
     * Sets the proxy URL.
     *
     * @param proxy value
     * @return this builder
     */
    public Builder proxy(String proxy) {
      config.proxy(proxy);
      return this;
    }

    /**
     * Sets the User-Agent suffix.
     *
     * @param suffix value
     * @return this builder
     */
    public Builder userAgentSuffix(String suffix) {
      config.userAgentSuffix(suffix);
      return this;
    }

    /**
     * Sets the request hooks.
     *
     * @param hooks value
     * @return this builder
     */
    public Builder hooks(codes.grow.packages.cloudwatchlogs.core.Hooks hooks) {
      config.hooks(hooks);
      return this;
    }

    /**
     * Sets the logger.
     *
     * @param logger value
     * @return this builder
     */
    public Builder logger(codes.grow.packages.cloudwatchlogs.core.Logger logger) {
      config.logger(logger);
      return this;
    }

    /**
     * Adds a header sent with every request.
     *
     * @param name header name
     * @param value header value
     * @return this builder
     */
    public Builder header(String name, String value) {
      config.header(name, value);
      return this;
    }

    /**
     * Builds the configuration and the client.
     *
     * @return a new client
     * @throws ConfigException when the configuration is invalid
     */
    public CloudWatchLogsClient build() {
      return create(config.build());
    }
  }
}
