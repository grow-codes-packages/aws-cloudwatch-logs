package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.ConfigException;
import codes.grow.packages.cloudwatchlogs.core.EnvReader;
import codes.grow.packages.cloudwatchlogs.core.Hooks;
import codes.grow.packages.cloudwatchlogs.core.Logger;
import codes.grow.packages.cloudwatchlogs.core.Redaction;
import codes.grow.packages.cloudwatchlogs.core.RetryPolicy;
import codes.grow.packages.cloudwatchlogs.core.Secret;
import codes.grow.packages.cloudwatchlogs.core.TlsOptions;
import codes.grow.packages.cloudwatchlogs.core.TransportConfig;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;

/**
 * Immutable configuration of a {@link CloudWatchLogsClient} (SPEC section 2.2, API.md section 5).
 *
 * <p>Build one with {@link #builder()} or read it from the environment with {@link #fromEnv()}.
 * Precedence: explicit builder value &gt; {@code GROW_AWS_CLOUDWATCH_LOGS_<NAME>} &gt; standard AWS
 * / CloudWatch variables ({@code AWS_ENDPOINT_URL_CLOUDWATCH_LOGS}, {@code AWS_REGION}, {@code
 * AWS_ACCESS_KEY_ID}, {@code CLOUDWATCH_LOG_GROUP}, …) &gt; default. Credentials left unset use the
 * AWS SDK default chain. {@link #toString()} never prints credentials.
 */
public final class CloudWatchLogsConfig {
  /** Default per-event message limit in UTF-8 bytes (256 KiB). */
  public static final int DEFAULT_MAX_EVENT_BYTES = 262_144;

  /** Largest message PutLogEvents accepts: 1 MiB minus the 26 bytes AWS counts per event. */
  public static final int MAX_EVENT_BYTES_HARD_CAP = 1_048_550;

  /** Default STS role session name used with {@code roleArn}. */
  public static final String DEFAULT_ROLE_SESSION_NAME = "grow-sdk";

  /** CloudWatch Logs API version. */
  public static final String API_VERSION = "2014-03-28";

  /** Vendor variables read for {@code url} after {@code GROW_AWS_CLOUDWATCH_LOGS_URL}, in order. */
  static final String[] URL_VENDOR_VARS = {"AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "AWS_ENDPOINT_URL"};

  /** Vendor variables read for {@code region}, in order. */
  static final String[] REGION_VENDOR_VARS = {"AWS_REGION", "AWS_DEFAULT_REGION"};

  /** Vendor variables read for the log group. */
  static final String[] LOG_GROUP_VENDOR_VARS = {"CLOUDWATCH_LOG_GROUP"};

  /** Vendor variables read for the log stream. */
  static final String[] LOG_STREAM_VENDOR_VARS = {"CLOUDWATCH_LOG_STREAM"};

  private static final Pattern GROUP_NAME = Pattern.compile("[.\\-_/#A-Za-z0-9]{1,512}");
  private static final Pattern REGION = Pattern.compile("[a-z0-9-]+");
  private static final Pattern ACCOUNT = Pattern.compile("\\d{12}");
  private static final SecureRandom RANDOM = new SecureRandom();

  private final String endpoint;
  private final Duration connectTimeout;
  private final Duration requestTimeout;
  private final Duration totalTimeout;
  private final RetryPolicy retry;
  private final TlsOptions tls;
  private final String proxy;
  private final Map<String, String> headers;
  private final String userAgentSuffix;
  private final Hooks hooks;
  private final Logger logger;
  private final String region;
  private final String accessKeyId;
  private final Secret secretAccessKey;
  private final Secret sessionToken;
  private final String profile;
  private final AwsCredentialsProvider credentialsProvider;
  private final String roleArn;
  private final String roleSessionName;
  private final String externalId;
  private final String logGroup;
  private final String logStream;
  private final String accountId;
  private final int maxEventBytes;
  private final OversizePolicy oversizePolicy;
  private final boolean autoCreate;
  private final CloudWatchLogsAsyncClient sdkClient;

  private CloudWatchLogsConfig(Builder b) {
    this.endpoint = blank(b.url);
    this.connectTimeout = b.connectTimeout != null ? b.connectTimeout : Duration.ofSeconds(10);
    this.requestTimeout = b.requestTimeout != null ? b.requestTimeout : Duration.ofSeconds(30);
    this.totalTimeout = b.totalTimeout != null ? b.totalTimeout : Duration.ofSeconds(120);
    this.retry = b.retry != null ? b.retry : RetryPolicy.defaults();
    this.tls = b.tls != null ? b.tls : TlsOptions.defaults();
    this.proxy = blank(b.proxy);
    this.headers = Map.copyOf(b.headers);
    this.userAgentSuffix = blank(b.userAgentSuffix);
    this.hooks = b.hooks != null ? b.hooks : Hooks.NONE;
    this.logger = b.logger != null ? b.logger : Logger.NOOP;
    this.region = blank(b.region);
    this.accessKeyId = blank(b.accessKeyId);
    this.secretAccessKey = b.secretAccessKey;
    this.sessionToken = b.sessionToken;
    this.profile = blank(b.profile);
    this.credentialsProvider = b.credentialsProvider;
    this.roleArn = blank(b.roleArn);
    String session = blank(b.roleSessionName);
    this.roleSessionName = session != null ? session : DEFAULT_ROLE_SESSION_NAME;
    this.externalId = blank(b.externalId);
    this.logGroup = blank(b.logGroup);
    String stream = blank(b.logStream);
    this.logStream = stream != null ? stream : defaultLogStreamName();
    this.accountId = blank(b.accountId);
    this.maxEventBytes = b.maxEventBytes != null ? b.maxEventBytes : DEFAULT_MAX_EVENT_BYTES;
    this.oversizePolicy = b.oversizePolicy != null ? b.oversizePolicy : OversizePolicy.TRUNCATE;
    this.autoCreate = b.autoCreate == null || b.autoCreate;
    this.sdkClient = b.sdkClient;
  }

  private static String blank(String value) {
    return value == null || value.trim().isEmpty() ? null : value;
  }

  /**
   * Returns whether {@code name} is a valid log group name (1–512 of {@code [.-_/#A-Za-z0-9]}).
   *
   * @param name candidate name
   * @return true when valid
   */
  public static boolean isValidLogGroupName(String name) {
    return name != null && GROUP_NAME.matcher(name).matches();
  }

  /**
   * Returns whether {@code name} is a valid log stream name (1–512 characters, no {@code :} or
   * {@code *}).
   *
   * @param name candidate name
   * @return true when valid
   */
  public static boolean isValidLogStreamName(String name) {
    return name != null
        && !name.isEmpty()
        && name.length() <= 512
        && name.indexOf(':') < 0
        && name.indexOf('*') < 0;
  }

  /**
   * Default stream name {@code <hostname>-<pid>-<random6>} (characters outside the allowed set
   * become {@code -}).
   *
   * @return a new stream name
   */
  public static String defaultLogStreamName() {
    String host;
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      host = "host";
    }
    host = host.replaceAll("[:*\\s]", "-");
    if (host.isEmpty()) {
      host = "host";
    }
    byte[] r = new byte[3];
    RANDOM.nextBytes(r);
    return String.format(
        Locale.ROOT, "%s-%d-%02x%02x%02x", host, ProcessHandle.current().pid(), r[0], r[1], r[2]);
  }

  /**
   * Starts an empty builder (every field at its default).
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reads the configuration from the process environment.
   *
   * @return the validated configuration
   * @throws ConfigException when a variable holds an invalid value (the message names the variable,
   *     never the value)
   */
  public static CloudWatchLogsConfig fromEnv() {
    return builder().fromEnv(EnvReader.system()).build();
  }

  /**
   * Reads the configuration from an explicit variable map (tests).
   *
   * @param env variable map
   * @return the validated configuration
   */
  public static CloudWatchLogsConfig fromEnv(Map<String, String> env) {
    return builder().fromEnv(EnvReader.of(env)).build();
  }

  /**
   * Returns a builder holding every value of this configuration.
   *
   * @return a new builder
   */
  public Builder toBuilder() {
    Builder b = new Builder();
    b.url = endpoint;
    b.connectTimeout = connectTimeout;
    b.requestTimeout = requestTimeout;
    b.totalTimeout = totalTimeout;
    b.retry = retry;
    b.tls = tls;
    b.proxy = proxy;
    b.headers.putAll(headers);
    b.userAgentSuffix = userAgentSuffix;
    b.hooks = hooks;
    b.logger = logger;
    b.region = region;
    b.accessKeyId = accessKeyId;
    b.secretAccessKey = secretAccessKey;
    b.sessionToken = sessionToken;
    b.profile = profile;
    b.credentialsProvider = credentialsProvider;
    b.roleArn = roleArn;
    b.roleSessionName = roleSessionName;
    b.externalId = externalId;
    b.logGroup = logGroup;
    b.logStream = logStream;
    b.accountId = accountId;
    b.maxEventBytes = maxEventBytes;
    b.oversizePolicy = oversizePolicy;
    b.autoCreate = autoCreate;
    b.sdkClient = sdkClient;
    return b;
  }

  /**
   * Checks every rule (URL, timeouts, TLS, credential pairs, names, limits).
   *
   * @throws ConfigException when a rule is violated (all problems in one message)
   */
  public void validate() {
    List<String> problems = new ArrayList<>();
    if (endpoint != null) {
      checkUrl("url", endpoint, problems);
    }
    if (proxy != null) {
      checkUrl("proxy", proxy, problems);
    }
    positive("connectTimeout", connectTimeout, problems);
    positive("requestTimeout", requestTimeout, problems);
    positive("totalTimeout", totalTimeout, problems);
    try {
      tls.validate();
    } catch (ConfigException e) {
      problems.add(e.getMessage());
    }
    if (region != null && !REGION.matcher(region).matches()) {
      problems.add("region must look like us-east-1");
    }
    if (accessKeyId != null && secretAccessKey == null) {
      problems.add("accessKeyId requires secretAccessKey");
    }
    if (accessKeyId == null && secretAccessKey != null) {
      problems.add("secretAccessKey requires accessKeyId");
    }
    if (sessionToken != null && accessKeyId == null) {
      problems.add("sessionToken requires accessKeyId and secretAccessKey");
    }
    if (credentialsProvider != null && accessKeyId != null) {
      problems.add("credentialsProvider and accessKeyId/secretAccessKey are mutually exclusive");
    }
    if (sdkClient != null) {
      List<String> clash = new ArrayList<>();
      addIf(clash, "accessKeyId", accessKeyId);
      addIf(clash, "credentialsProvider", credentialsProvider);
      addIf(clash, "profile", profile);
      addIf(clash, "roleArn", roleArn);
      addIf(clash, "region", region);
      addIf(clash, "url", endpoint);
      if (!clash.isEmpty()) {
        problems.add("sdkClient is mutually exclusive with " + String.join(", ", clash));
      }
    }
    if (roleArn != null && !roleArn.startsWith("arn:")) {
      problems.add("roleArn must be an ARN");
    }
    if (logGroup != null && !isValidLogGroupName(logGroup)) {
      problems.add("logGroup must be 1-512 characters of [.-_/#A-Za-z0-9]");
    }
    if (!isValidLogStreamName(logStream)) {
      problems.add("logStream must be 1-512 characters without ':' or '*'");
    }
    if (accountId != null && !ACCOUNT.matcher(accountId).matches()) {
      problems.add("accountId must be 12 digits");
    }
    if (maxEventBytes < 64 || maxEventBytes > MAX_EVENT_BYTES_HARD_CAP) {
      problems.add("maxEventBytes must be an integer from 64 to " + MAX_EVENT_BYTES_HARD_CAP);
    }
    if (!problems.isEmpty()) {
      throw new ConfigException("invalid configuration: " + String.join("; ", problems));
    }
  }

  private static void addIf(List<String> out, String name, Object value) {
    if (value != null) {
      out.add(name);
    }
  }

  private static void checkUrl(String name, String value, List<String> problems) {
    try {
      URI u = new URI(value);
      String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase(Locale.ROOT);
      if (!(scheme.equals("http") || scheme.equals("https")) || u.getHost() == null) {
        problems.add(name + " must use http or https and include a host");
      }
    } catch (URISyntaxException e) {
      problems.add(name + " is not a valid URL");
    }
  }

  private static void positive(String name, Duration d, List<String> problems) {
    if (d.isZero() || d.isNegative()) {
      problems.add(name + " must be positive");
    }
  }

  /**
   * Translates the common fields into transport settings (used by the Grow HTTP client of the AWS
   * SDK).
   *
   * @return the transport configuration
   */
  public TransportConfig toTransportConfig() {
    return TransportConfig.builder(url())
        .connectTimeout(connectTimeout)
        .requestTimeout(requestTimeout)
        .totalTimeout(totalTimeout)
        .retry(retry)
        .tls(tls)
        .proxy(proxy)
        .headers(headers)
        .userAgentSuffix(userAgentSuffix)
        .hooks(hooks)
        .logger(logger)
        .knownSecrets(this::knownSecrets)
        .build();
  }

  /**
   * Credential values to redact from error messages and logs.
   *
   * @return the live secret values
   */
  public List<String> knownSecrets() {
    List<String> out = new ArrayList<>();
    for (Secret s : new Secret[] {secretAccessKey, sessionToken}) {
      if (s != null && !s.isCleared() && !s.isEmpty()) {
        out.add(s.reveal());
      }
    }
    return out;
  }

  /**
   * Effective endpoint: {@link #endpoint()} when set, otherwise the public AWS endpoint of {@link
   * #region()} ({@code us-east-1} when the region is left to the SDK). Informational.
   *
   * @return the URL
   */
  public String url() {
    return endpoint != null
        ? endpoint
        : "https://logs." + (region != null ? region : "us-east-1") + ".amazonaws.com";
  }

  /**
   * Returns the explicit endpoint override passed to the AWS SDK.
   *
   * @return the endpoint, or empty when the SDK resolves it
   */
  public Optional<String> endpoint() {
    return Optional.ofNullable(endpoint);
  }

  /**
   * Returns the connect timeout (default 10 s).
   *
   * @return the timeout
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  /**
   * Returns the per-attempt timeout (default 30 s).
   *
   * @return the timeout
   */
  public Duration requestTimeout() {
    return requestTimeout;
  }

  /**
   * Returns the total timeout per call including retries (default 120 s).
   *
   * @return the timeout
   */
  public Duration totalTimeout() {
    return totalTimeout;
  }

  /**
   * Returns the retry policy ({@code maxAttempts = maxRetries + 1} in the SDK standard mode).
   *
   * @return the policy
   */
  public RetryPolicy retry() {
    return retry;
  }

  /**
   * Returns the TLS options.
   *
   * @return the options
   */
  public TlsOptions tls() {
    return tls;
  }

  /**
   * Returns the explicit proxy URL.
   *
   * @return the proxy, or empty
   */
  public Optional<String> proxy() {
    return Optional.ofNullable(proxy);
  }

  /**
   * Returns the extra request headers.
   *
   * @return immutable map
   */
  public Map<String, String> headers() {
    return headers;
  }

  /**
   * Returns the user agent suffix.
   *
   * @return the suffix, or empty
   */
  public Optional<String> userAgentSuffix() {
    return Optional.ofNullable(userAgentSuffix);
  }

  /**
   * Returns the hooks.
   *
   * @return the hooks
   */
  public Hooks hooks() {
    return hooks;
  }

  /**
   * Returns the logger.
   *
   * @return the logger
   */
  public Logger logger() {
    return logger;
  }

  /**
   * Returns the AWS region.
   *
   * @return the region, or empty (SDK chain: environment, profile)
   */
  public Optional<String> region() {
    return Optional.ofNullable(region);
  }

  /**
   * Returns the access key id.
   *
   * @return the key id, or empty
   */
  public Optional<String> accessKeyId() {
    return Optional.ofNullable(accessKeyId);
  }

  /**
   * Returns the secret access key.
   *
   * @return the secret, or empty
   */
  public Optional<Secret> secretAccessKey() {
    return Optional.ofNullable(secretAccessKey);
  }

  /**
   * Returns the session token.
   *
   * @return the token, or empty
   */
  public Optional<Secret> sessionToken() {
    return Optional.ofNullable(sessionToken);
  }

  /**
   * Returns the shared config profile.
   *
   * @return the profile, or empty
   */
  public Optional<String> profile() {
    return Optional.ofNullable(profile);
  }

  /**
   * Returns the explicit AWS credentials provider.
   *
   * @return the provider, or empty
   */
  public Optional<AwsCredentialsProvider> credentialsProvider() {
    return Optional.ofNullable(credentialsProvider);
  }

  /**
   * Returns the role assumed with STS.
   *
   * @return the role ARN, or empty
   */
  public Optional<String> roleArn() {
    return Optional.ofNullable(roleArn);
  }

  /**
   * Returns the STS session name (default {@code grow-sdk}).
   *
   * @return the session name
   */
  public String roleSessionName() {
    return roleSessionName;
  }

  /**
   * Returns the STS external id.
   *
   * @return the external id, or empty
   */
  public Optional<String> externalId() {
    return Optional.ofNullable(externalId);
  }

  /**
   * Returns the default log group of the log client and the query client.
   *
   * @return the group, or empty
   */
  public Optional<String> logGroup() {
    return Optional.ofNullable(logGroup);
  }

  /**
   * Returns the log stream of the log client (default {@code <hostname>-<pid>-<random6>}).
   *
   * @return the stream name
   */
  public String logStream() {
    return logStream;
  }

  /**
   * Returns the AWS account id used to build log group ARNs for live tail.
   *
   * @return the account id, or empty (looked up with {@code DescribeLogGroups})
   */
  public Optional<String> accountId() {
    return Optional.ofNullable(accountId);
  }

  /**
   * Returns the per-event message limit in UTF-8 bytes (default 262 144).
   *
   * @return the limit
   */
  public int maxEventBytes() {
    return maxEventBytes;
  }

  /**
   * Returns the policy for oversized events (default {@link OversizePolicy#TRUNCATE}).
   *
   * @return the policy
   */
  public OversizePolicy oversizePolicy() {
    return oversizePolicy;
  }

  /**
   * Returns whether {@code putLogEvents} creates a missing log stream (default true).
   *
   * @return the flag
   */
  public boolean autoCreate() {
    return autoCreate;
  }

  /**
   * Returns the caller-supplied AWS SDK client.
   *
   * @return the client, or empty
   */
  public Optional<CloudWatchLogsAsyncClient> sdkClient() {
    return Optional.ofNullable(sdkClient);
  }

  @Override
  public String toString() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("url", Redaction.redactUrl(url()));
    m.put("endpoint", endpoint == null ? null : Redaction.redactUrl(endpoint));
    m.put("connectTimeout", connectTimeout);
    m.put("requestTimeout", requestTimeout);
    m.put("totalTimeout", totalTimeout);
    m.put("retry", retry);
    m.put("tls", tls);
    m.put("proxy", proxy == null ? null : Redaction.redactUrl(proxy));
    m.put("headers", Redaction.redactHeaders(headers));
    m.put("userAgentSuffix", userAgentSuffix);
    m.put("region", region);
    m.put("accessKeyId", accessKeyId);
    m.put("secretAccessKey", secretAccessKey == null ? null : Redaction.REDACTED);
    m.put("sessionToken", sessionToken == null ? null : Redaction.REDACTED);
    m.put("profile", profile);
    m.put("credentialsProvider", credentialsProvider == null ? null : Redaction.REDACTED);
    m.put("roleArn", roleArn);
    m.put("roleSessionName", roleSessionName);
    m.put("externalId", externalId == null ? null : Redaction.REDACTED);
    m.put("logGroup", logGroup);
    m.put("logStream", logStream);
    m.put("accountId", accountId);
    m.put("maxEventBytes", maxEventBytes);
    m.put("oversizePolicy", oversizePolicy.wireValue());
    m.put("autoCreate", autoCreate);
    m.put("sdkClient", sdkClient == null ? null : "CloudWatchLogsAsyncClient");
    return "CloudWatchLogsConfig" + m;
  }

  /**
   * Builder of the configuration.
   *
   * <p>Unset fields take the environment (when {@link #fromEnv(EnvReader)} is called) or the
   * default.
   */
  public static final class Builder {
    private String url;
    private Duration connectTimeout;
    private Duration requestTimeout;
    private Duration totalTimeout;
    private RetryPolicy retry;
    private TlsOptions tls;
    private String proxy;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private String userAgentSuffix;
    private Hooks hooks;
    private Logger logger;
    private String region;
    private String accessKeyId;
    private Secret secretAccessKey;
    private Secret sessionToken;
    private String profile;
    private AwsCredentialsProvider credentialsProvider;
    private String roleArn;
    private String roleSessionName;
    private String externalId;
    private String logGroup;
    private String logStream;
    private String accountId;
    private Integer maxEventBytes;
    private OversizePolicy oversizePolicy;
    private Boolean autoCreate;
    private CloudWatchLogsAsyncClient sdkClient;

    private Builder() {}

    /** Reads only a vendor variable (the empty SDK name is never set). */
    private static String vendor(EnvReader env, String variable) {
      return env.string("", variable).orElse(null);
    }

    /**
     * Fills every field that was not set explicitly from the environment. An explicit {@code
     * sdkClient} or {@code credentialsProvider} replaces the credentials of the environment (and an
     * explicit {@code sdkClient} also its region, endpoint, profile and role). Credentials come
     * from one layer: a {@code GROW_…_ACCESS_KEY_ID} never pairs with an {@code
     * AWS_SECRET_ACCESS_KEY}.
     *
     * @param env environment reader
     * @return this builder
     * @throws ConfigException when a variable holds an invalid value
     */
    public Builder fromEnv(EnvReader env) {
      boolean explicitClient = sdkClient != null;
      boolean explicitCredentials = explicitClient || credentialsProvider != null;
      if (!explicitClient) {
        if (url == null) {
          url = env.string("URL", URL_VENDOR_VARS).orElse(null);
        }
        if (region == null) {
          region = env.string("REGION", REGION_VENDOR_VARS).orElse(null);
        }
        if (profile == null) {
          profile = env.string("PROFILE", "AWS_PROFILE").orElse(null);
        }
        if (roleArn == null) {
          roleArn = env.string("ROLE_ARN").orElse(null);
        }
      }
      if (!explicitCredentials && accessKeyId == null && secretAccessKey == null) {
        Optional<String> growKey = env.string("ACCESS_KEY_ID");
        if (growKey.isPresent()) {
          accessKeyId = growKey.get();
          secretAccessKey = env.string("SECRET_ACCESS_KEY").map(Secret::of).orElse(null);
          sessionToken = env.string("SESSION_TOKEN").map(Secret::of).orElse(null);
        } else {
          accessKeyId = vendor(env, "AWS_ACCESS_KEY_ID");
          String secret = vendor(env, "AWS_SECRET_ACCESS_KEY");
          secretAccessKey = secret == null ? null : Secret.of(secret);
          String token = vendor(env, "AWS_SESSION_TOKEN");
          sessionToken = token == null ? null : Secret.of(token);
        }
      }
      if (roleSessionName == null) {
        roleSessionName = env.string("ROLE_SESSION_NAME").orElse(null);
      }
      if (externalId == null) {
        externalId = env.string("EXTERNAL_ID").orElse(null);
      }
      if (logGroup == null) {
        logGroup = env.string("LOG_GROUP", LOG_GROUP_VENDOR_VARS).orElse(null);
      }
      if (logStream == null) {
        logStream = env.string("LOG_STREAM", LOG_STREAM_VENDOR_VARS).orElse(null);
      }
      if (accountId == null) {
        accountId = env.string("ACCOUNT_ID").orElse(null);
      }
      if (maxEventBytes == null && env.integer("MAX_EVENT_BYTES").isPresent()) {
        maxEventBytes = env.integer("MAX_EVENT_BYTES").getAsInt();
      }
      if (oversizePolicy == null) {
        Optional<String> p = env.string("OVERSIZE_POLICY");
        if (p.isPresent()) {
          try {
            oversizePolicy = OversizePolicy.parse(p.get());
          } catch (ConfigException e) {
            throw new ConfigException(
                "config: invalid value in "
                    + EnvReader.DEFAULT_PREFIX
                    + "OVERSIZE_POLICY (expected truncate, drop or error)");
          }
        }
      }
      if (autoCreate == null) {
        autoCreate = env.bool("AUTO_CREATE").orElse(null);
      }
      if (connectTimeout == null) {
        connectTimeout = env.duration("CONNECT_TIMEOUT").orElse(null);
      }
      if (requestTimeout == null) {
        requestTimeout = env.duration("REQUEST_TIMEOUT").orElse(null);
      }
      if (totalTimeout == null) {
        totalTimeout = env.duration("TOTAL_TIMEOUT").orElse(null);
      }
      if (retry == null && env.integer("MAX_RETRIES").isPresent()) {
        int n = env.integer("MAX_RETRIES").getAsInt();
        if (n < 0) {
          String variable = EnvReader.DEFAULT_PREFIX + "MAX_RETRIES";
          throw new ConfigException("config: invalid value in " + variable + " (expected >= 0)");
        }
        retry = RetryPolicy.builder().maxRetries(n).build();
      }
      if (tls == null) {
        TlsOptions.Builder t = TlsOptions.builder();
        env.string("CA_CERT", "AWS_CA_BUNDLE").ifPresent(p -> t.caCertPath(Paths.get(p)));
        env.string("CLIENT_CERT").ifPresent(p -> t.clientCertPath(Paths.get(p)));
        env.string("CLIENT_KEY").ifPresent(p -> t.clientKeyPath(Paths.get(p)));
        env.bool("INSECURE_SKIP_VERIFY").ifPresent(t::insecureSkipVerify);
        tls = t.build();
      }
      if (proxy == null) {
        proxy = env.string("PROXY").orElse(null);
      }
      return this;
    }

    /**
     * Sets the endpoint override ({@code http://aws-cloudwatch-logs:5000} in Docker). Unset: the
     * AWS SDK resolves {@code https://logs.<region>.amazonaws.com}.
     *
     * @param url {@code http} or {@code https} URL
     * @return this builder
     */
    public Builder url(String url) {
      this.url = url;
      return this;
    }

    /**
     * Sets the connect timeout (default 10 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder connectTimeout(Duration d) {
      this.connectTimeout = d;
      return this;
    }

    /**
     * Sets the per-attempt timeout (default 30 s; not applied to live-tail sessions).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder requestTimeout(Duration d) {
      this.requestTimeout = d;
      return this;
    }

    /**
     * Sets the total timeout per call including retries (default 120 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder totalTimeout(Duration d) {
      this.totalTimeout = d;
      return this;
    }

    /**
     * Sets the retry policy (default {@link RetryPolicy#defaults()}).
     *
     * @param retry policy
     * @return this builder
     */
    public Builder retry(RetryPolicy retry) {
      this.retry = retry;
      return this;
    }

    /**
     * Sets the TLS options; environment TLS variables are then ignored.
     *
     * @param tls options
     * @return this builder
     */
    public Builder tls(TlsOptions tls) {
      this.tls = tls;
      return this;
    }

    /**
     * Sets an explicit proxy URL; environment proxies ({@code HTTPS_PROXY}) are never used.
     *
     * @param proxy proxy URL
     * @return this builder
     */
    public Builder proxy(String proxy) {
      this.proxy = proxy;
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
      this.headers.put(name, value);
      return this;
    }

    /**
     * Sets the text appended to the {@code User-Agent}.
     *
     * @param suffix suffix
     * @return this builder
     */
    public Builder userAgentSuffix(String suffix) {
      this.userAgentSuffix = suffix;
      return this;
    }

    /**
     * Sets request hooks (fired once per HTTP attempt).
     *
     * @param hooks hooks
     * @return this builder
     */
    public Builder hooks(Hooks hooks) {
      this.hooks = hooks;
      return this;
    }

    /**
     * Sets the logger (default silent).
     *
     * @param logger logger
     * @return this builder
     */
    public Builder logger(Logger logger) {
      this.logger = logger;
      return this;
    }

    /**
     * Sets the AWS region.
     *
     * @param region region such as {@code us-east-1}
     * @return this builder
     */
    public Builder region(String region) {
      this.region = region;
      return this;
    }

    /**
     * Sets the access key id.
     *
     * @param accessKeyId key id
     * @return this builder
     */
    public Builder accessKeyId(String accessKeyId) {
      this.accessKeyId = accessKeyId;
      return this;
    }

    /**
     * Sets the secret access key.
     *
     * @param secret secret
     * @return this builder
     */
    public Builder secretAccessKey(Secret secret) {
      this.secretAccessKey = secret;
      return this;
    }

    /**
     * Sets the secret access key.
     *
     * @param secret secret
     * @return this builder
     */
    public Builder secretAccessKey(String secret) {
      this.secretAccessKey = secret == null ? null : Secret.of(secret);
      return this;
    }

    /**
     * Sets the session token.
     *
     * @param token token
     * @return this builder
     */
    public Builder sessionToken(Secret token) {
      this.sessionToken = token;
      return this;
    }

    /**
     * Sets the session token.
     *
     * @param token token
     * @return this builder
     */
    public Builder sessionToken(String token) {
      this.sessionToken = token == null ? null : Secret.of(token);
      return this;
    }

    /**
     * Sets the shared config profile.
     *
     * @param profile profile name
     * @return this builder
     */
    public Builder profile(String profile) {
      this.profile = profile;
      return this;
    }

    /**
     * Sets an AWS SDK credentials provider (instead of the key fields or the default chain).
     *
     * @param provider provider
     * @return this builder
     */
    public Builder credentialsProvider(AwsCredentialsProvider provider) {
      this.credentialsProvider = provider;
      return this;
    }

    /**
     * Sets a role assumed with STS on top of the base credentials.
     *
     * @param roleArn role ARN
     * @return this builder
     */
    public Builder roleArn(String roleArn) {
      this.roleArn = roleArn;
      return this;
    }

    /**
     * Sets the STS session name (default {@code grow-sdk}).
     *
     * @param name session name
     * @return this builder
     */
    public Builder roleSessionName(String name) {
      this.roleSessionName = name;
      return this;
    }

    /**
     * Sets the STS external id.
     *
     * @param externalId external id
     * @return this builder
     */
    public Builder externalId(String externalId) {
      this.externalId = externalId;
      return this;
    }

    /**
     * Sets the default log group.
     *
     * @param logGroup group name
     * @return this builder
     */
    public Builder logGroup(String logGroup) {
      this.logGroup = logGroup;
      return this;
    }

    /**
     * Sets the log stream of the log client.
     *
     * @param logStream stream name
     * @return this builder
     */
    public Builder logStream(String logStream) {
      this.logStream = logStream;
      return this;
    }

    /**
     * Sets the AWS account id used for live-tail ARNs.
     *
     * @param accountId 12-digit account id
     * @return this builder
     */
    public Builder accountId(String accountId) {
      this.accountId = accountId;
      return this;
    }

    /**
     * Sets the per-event message limit (64 to 1 048 550 bytes; default 262 144).
     *
     * @param bytes limit in UTF-8 bytes
     * @return this builder
     */
    public Builder maxEventBytes(int bytes) {
      this.maxEventBytes = bytes;
      return this;
    }

    /**
     * Sets the policy for oversized events.
     *
     * @param policy policy
     * @return this builder
     */
    public Builder oversizePolicy(OversizePolicy policy) {
      this.oversizePolicy = policy;
      return this;
    }

    /**
     * Sets whether {@code putLogEvents} creates a missing log stream and retries once.
     *
     * @param autoCreate flag (default true)
     * @return this builder
     */
    public Builder autoCreate(boolean autoCreate) {
      this.autoCreate = autoCreate;
      return this;
    }

    /**
     * Uses an existing AWS SDK async client instead of building one. Mutually exclusive with the
     * credential, region and endpoint fields; {@code close()} never closes it and the Grow hooks,
     * TLS, proxy, timeout and User-Agent settings do not apply to it.
     *
     * @param client the SDK client
     * @return this builder
     */
    public Builder sdkClient(CloudWatchLogsAsyncClient client) {
      this.sdkClient = client;
      return this;
    }

    /**
     * Builds and validates the configuration.
     *
     * @return the configuration
     * @throws ConfigException when a rule is violated
     */
    public CloudWatchLogsConfig build() {
      CloudWatchLogsConfig c = new CloudWatchLogsConfig(this);
      c.validate();
      return c;
    }
  }
}
