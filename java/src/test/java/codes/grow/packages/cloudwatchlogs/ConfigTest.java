package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.cloudwatchlogs.core.ConfigException;
import codes.grow.packages.cloudwatchlogs.core.EnvReader;
import codes.grow.packages.cloudwatchlogs.core.Hooks;
import codes.grow.packages.cloudwatchlogs.core.Logger;
import codes.grow.packages.cloudwatchlogs.core.RetryPolicy;
import codes.grow.packages.cloudwatchlogs.core.Secret;
import codes.grow.packages.cloudwatchlogs.core.TlsOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;

class ConfigTest {
  static CloudWatchLogsAsyncClient sdk() {
    return CloudWatchLogsAsyncClient.builder()
        .region(software.amazon.awssdk.regions.Region.US_EAST_1)
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .httpClient(new GrowHttpClient(CloudWatchLogsConfig.builder().build().toTransportConfig()))
        .build();
  }

  @Test
  void defaults() {
    CloudWatchLogsConfig c = CloudWatchLogsConfig.builder().build();
    assertThat(c.endpoint()).isEmpty();
    assertThat(c.url()).isEqualTo("https://logs.us-east-1.amazonaws.com");
    assertThat(c.connectTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(c.requestTimeout()).isEqualTo(Duration.ofSeconds(30));
    assertThat(c.totalTimeout()).isEqualTo(Duration.ofSeconds(120));
    assertThat(c.retry().maxRetries()).isEqualTo(RetryPolicy.DEFAULT_MAX_RETRIES);
    assertThat(c.tls()).isNotNull();
    assertThat(c.proxy()).isEmpty();
    assertThat(c.headers()).isEmpty();
    assertThat(c.userAgentSuffix()).isEmpty();
    assertThat(c.hooks()).isSameAs(Hooks.NONE);
    assertThat(c.logger()).isSameAs(Logger.NOOP);
    assertThat(c.region()).isEmpty();
    assertThat(c.accessKeyId()).isEmpty();
    assertThat(c.secretAccessKey()).isEmpty();
    assertThat(c.sessionToken()).isEmpty();
    assertThat(c.profile()).isEmpty();
    assertThat(c.credentialsProvider()).isEmpty();
    assertThat(c.roleArn()).isEmpty();
    assertThat(c.roleSessionName()).isEqualTo("grow-sdk");
    assertThat(c.externalId()).isEmpty();
    assertThat(c.logGroup()).isEmpty();
    assertThat(c.logStream()).matches(".+-\\d+-[0-9a-f]{6}");
    assertThat(c.accountId()).isEmpty();
    assertThat(c.maxEventBytes()).isEqualTo(262_144);
    assertThat(c.oversizePolicy()).isEqualTo(OversizePolicy.TRUNCATE);
    assertThat(c.autoCreate()).isTrue();
    assertThat(c.sdkClient()).isEmpty();
    assertThat(c.knownSecrets()).isEmpty();
    assertThat(CloudWatchLogsConfig.API_VERSION).isEqualTo("2014-03-28");
  }

  @Test
  void explicitValuesAndRedaction() {
    Hooks hooks = new Hooks() {};
    Logger logger = new Logger() {};
    CloudWatchLogsConfig c =
        CloudWatchLogsConfig.builder()
            .url("http://user:pw@localhost:5000")
            .connectTimeout(Duration.ofSeconds(1))
            .requestTimeout(Duration.ofSeconds(2))
            .totalTimeout(Duration.ofSeconds(3))
            .retry(RetryPolicy.none())
            .tls(TlsOptions.defaults())
            .proxy("http://proxy:3128")
            .header("X-Extra", "1")
            .userAgentSuffix("app/1")
            .hooks(hooks)
            .logger(logger)
            .region("eu-west-1")
            .accessKeyId("AKID")
            .secretAccessKey(Secret.of("secret-value-1"))
            .sessionToken(Secret.of("session-value-1"))
            .roleArn("arn:aws:iam::1:role/r")
            .roleSessionName("sess")
            .externalId("ext-secret")
            .logGroup("app/api")
            .logStream("web-1")
            .accountId("123456789012")
            .maxEventBytes(1000)
            .oversizePolicy(OversizePolicy.DROP)
            .autoCreate(false)
            .build();
    assertThat(c.url()).isEqualTo("http://user:pw@localhost:5000");
    assertThat(c.endpoint()).isPresent();
    assertThat(c.knownSecrets()).containsExactly("secret-value-1", "session-value-1");
    String text = c.toString();
    assertThat(text)
        .doesNotContain("secret-value-1")
        .doesNotContain("session-value-1")
        .doesNotContain("ext-secret")
        .doesNotContain(":pw@")
        .contains("drop");
    CloudWatchLogsConfig copy = c.toBuilder().build();
    assertThat(copy.toString()).isEqualTo(text);
    assertThat(copy.hooks()).isSameAs(hooks);
    assertThat(copy.toTransportConfig().userAgent()).contains("app/1");
    assertThat(copy.toTransportConfig().knownSecrets()).contains("secret-value-1");
    c.secretAccessKey().get().clear();
    assertThat(c.knownSecrets()).containsExactly("session-value-1");
    CloudWatchLogsConfig withProvider =
        CloudWatchLogsConfig.builder()
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .profile("p")
            .secretAccessKey((String) null)
            .sessionToken((String) null)
            .build();
    assertThat(withProvider.toString()).contains("credentialsProvider=[REDACTED]");
  }

  @Test
  void validationCollectsProblems() {
    assertThatThrownBy(
            () ->
                CloudWatchLogsConfig.builder()
                    .url("ftp://x")
                    .proxy("::bad")
                    .connectTimeout(Duration.ZERO)
                    .requestTimeout(Duration.ofSeconds(-1))
                    .totalTimeout(Duration.ZERO)
                    .region("EU WEST")
                    .accessKeyId("AKID")
                    .roleArn("role")
                    .logGroup("bad name")
                    .logStream("a:b")
                    .accountId("12")
                    .maxEventBytes(10)
                    .build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("url must use http")
        .hasMessageContaining("proxy is not a valid URL")
        .hasMessageContaining("connectTimeout")
        .hasMessageContaining("region")
        .hasMessageContaining("accessKeyId requires secretAccessKey")
        .hasMessageContaining("roleArn")
        .hasMessageContaining("logGroup")
        .hasMessageContaining("logStream")
        .hasMessageContaining("accountId")
        .hasMessageContaining("maxEventBytes");
    assertThatThrownBy(
            () -> CloudWatchLogsConfig.builder().secretAccessKey("s").sessionToken("t").build())
        .hasMessageContaining("secretAccessKey requires accessKeyId")
        .hasMessageContaining("sessionToken requires");
    assertThatThrownBy(
            () ->
                CloudWatchLogsConfig.builder()
                    .accessKeyId("a")
                    .secretAccessKey("s")
                    .credentialsProvider(AnonymousCredentialsProvider.create())
                    .build())
        .hasMessageContaining("mutually exclusive");
    CloudWatchLogsAsyncClient sdk = sdk();
    try {
      assertThatThrownBy(
              () ->
                  CloudWatchLogsConfig.builder()
                      .sdkClient(sdk)
                      .accessKeyId("a")
                      .secretAccessKey("s")
                      .profile("p")
                      .roleArn("arn:x")
                      .region("us-east-1")
                      .url("http://x")
                      .credentialsProvider(AnonymousCredentialsProvider.create())
                      .build())
          .hasMessageContaining(
              "sdkClient is mutually exclusive with accessKeyId, credentialsProvider, profile,"
                  + " roleArn, region, url");
      CloudWatchLogsConfig ok = CloudWatchLogsConfig.builder().sdkClient(sdk).build();
      assertThat(ok.toString()).contains("CloudWatchLogsAsyncClient");
    } finally {
      sdk.close();
    }
    assertThatThrownBy(() -> CloudWatchLogsConfig.builder().url("http://[bad").build())
        .hasMessageContaining("url is not a valid URL");
    assertThatThrownBy(() -> CloudWatchLogsConfig.builder().url("http:///path").build())
        .hasMessageContaining("include a host");
    assertThatThrownBy(
            () ->
                CloudWatchLogsConfig.builder()
                    .tls(TlsOptions.builder().clientCertPath(Path.of("/nope.pem")).build())
                    .build())
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void names() {
    assertThat(CloudWatchLogsConfig.isValidLogGroupName("a/b-c_d.e#f")).isTrue();
    assertThat(CloudWatchLogsConfig.isValidLogGroupName("a b")).isFalse();
    assertThat(CloudWatchLogsConfig.isValidLogGroupName(null)).isFalse();
    assertThat(CloudWatchLogsConfig.isValidLogGroupName("x".repeat(513))).isFalse();
    assertThat(CloudWatchLogsConfig.isValidLogStreamName("a b/c")).isTrue();
    assertThat(CloudWatchLogsConfig.isValidLogStreamName("a*b")).isFalse();
    assertThat(CloudWatchLogsConfig.isValidLogStreamName("")).isFalse();
    assertThat(CloudWatchLogsConfig.isValidLogStreamName(null)).isFalse();
    assertThat(CloudWatchLogsConfig.isValidLogStreamName("x".repeat(513))).isFalse();
    assertThat(CloudWatchLogsConfig.defaultLogStreamName())
        .isNotEqualTo(CloudWatchLogsConfig.defaultLogStreamName());
  }

  @Test
  void fromEnvPrecedence() {
    Map<String, String> env = new HashMap<>();
    env.put("AWS_ENDPOINT_URL", "http://generic:1");
    env.put("AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "http://service:2");
    env.put("AWS_DEFAULT_REGION", "eu-central-1");
    env.put("AWS_REGION", "eu-west-1");
    env.put("AWS_ACCESS_KEY_ID", "AKIAVENDOR");
    env.put("AWS_SECRET_ACCESS_KEY", "vendor-secret");
    env.put("AWS_SESSION_TOKEN", "vendor-token");
    env.put("AWS_PROFILE", "vendor-profile");
    env.put("CLOUDWATCH_LOG_GROUP", "vendor/group");
    env.put("CLOUDWATCH_LOG_STREAM", "vendor-stream");
    CloudWatchLogsConfig v = CloudWatchLogsConfig.fromEnv(env);
    assertThat(v.url()).isEqualTo("http://service:2");
    assertThat(v.region()).contains("eu-west-1");
    assertThat(v.accessKeyId()).contains("AKIAVENDOR");
    assertThat(v.sessionToken().map(Secret::reveal)).contains("vendor-token");
    assertThat(v.profile()).contains("vendor-profile");
    assertThat(v.logGroup()).contains("vendor/group");
    assertThat(v.logStream()).isEqualTo("vendor-stream");

    String p = EnvReader.DEFAULT_PREFIX;
    env.put(p + "URL", "http://grow:3");
    env.put(p + "REGION", "us-west-2");
    env.put(p + "ACCESS_KEY_ID", "AKIAGROW");
    env.put(p + "SECRET_ACCESS_KEY", "grow-secret");
    env.put(p + "PROFILE", "grow-profile");
    env.put(p + "ROLE_ARN", "arn:aws:iam::1:role/r");
    env.put(p + "ROLE_SESSION_NAME", "s");
    env.put(p + "EXTERNAL_ID", "e");
    env.put(p + "LOG_GROUP", "grow/group");
    env.put(p + "LOG_STREAM", "grow-stream");
    env.put(p + "ACCOUNT_ID", "123456789012");
    env.put(p + "MAX_EVENT_BYTES", "4096");
    env.put(p + "OVERSIZE_POLICY", "Error");
    env.put(p + "AUTO_CREATE", "false");
    env.put(p + "CONNECT_TIMEOUT", "1s");
    env.put(p + "REQUEST_TIMEOUT", "2s");
    env.put(p + "TOTAL_TIMEOUT", "3s");
    env.put(p + "MAX_RETRIES", "5");
    env.put(p + "PROXY", "http://proxy:1");
    env.put(p + "INSECURE_SKIP_VERIFY", "true");
    CloudWatchLogsConfig g = CloudWatchLogsConfig.fromEnv(env);
    assertThat(g.url()).isEqualTo("http://grow:3");
    assertThat(g.region()).contains("us-west-2");
    assertThat(g.accessKeyId()).contains("AKIAGROW");
    assertThat(g.secretAccessKey().map(Secret::reveal)).contains("grow-secret");
    // credentials come from one layer: no AWS_SESSION_TOKEN with a GROW key
    assertThat(g.sessionToken()).isEmpty();
    assertThat(g.profile()).contains("grow-profile");
    assertThat(g.roleArn()).contains("arn:aws:iam::1:role/r");
    assertThat(g.roleSessionName()).isEqualTo("s");
    assertThat(g.externalId()).contains("e");
    assertThat(g.logGroup()).contains("grow/group");
    assertThat(g.logStream()).isEqualTo("grow-stream");
    assertThat(g.accountId()).contains("123456789012");
    assertThat(g.maxEventBytes()).isEqualTo(4096);
    assertThat(g.oversizePolicy()).isEqualTo(OversizePolicy.ERROR);
    assertThat(g.autoCreate()).isFalse();
    assertThat(g.connectTimeout()).isEqualTo(Duration.ofSeconds(1));
    assertThat(g.requestTimeout()).isEqualTo(Duration.ofSeconds(2));
    assertThat(g.totalTimeout()).isEqualTo(Duration.ofSeconds(3));
    assertThat(g.retry().maxRetries()).isEqualTo(5);
    assertThat(g.proxy()).contains("http://proxy:1");
    assertThat(g.tls().insecureSkipVerify()).isTrue();

    // explicit builder values win over the environment
    CloudWatchLogsConfig explicit =
        CloudWatchLogsConfig.builder()
            .url("http://explicit:4")
            .region("ap-south-1")
            .logGroup("explicit/group")
            .retry(RetryPolicy.none())
            .tls(TlsOptions.defaults())
            .oversizePolicy(OversizePolicy.TRUNCATE)
            .fromEnv(EnvReader.of(env))
            .build();
    assertThat(explicit.url()).isEqualTo("http://explicit:4");
    assertThat(explicit.region()).contains("ap-south-1");
    assertThat(explicit.logGroup()).contains("explicit/group");
    assertThat(explicit.oversizePolicy()).isEqualTo(OversizePolicy.TRUNCATE);
  }

  @Test
  void fromEnvExplicitClientOrProviderReplacesEnvironmentCredentials() {
    Map<String, String> env =
        Map.of(
            "AWS_ACCESS_KEY_ID", "AKIA",
            "AWS_SECRET_ACCESS_KEY", "s",
            "AWS_REGION", "eu-west-1",
            "AWS_PROFILE", "p");
    CloudWatchLogsConfig withProvider =
        CloudWatchLogsConfig.builder()
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .fromEnv(EnvReader.of(env))
            .build();
    assertThat(withProvider.accessKeyId()).isEmpty();
    assertThat(withProvider.region()).contains("eu-west-1");
    CloudWatchLogsAsyncClient sdk = sdk();
    try {
      CloudWatchLogsConfig withClient =
          CloudWatchLogsConfig.builder().sdkClient(sdk).fromEnv(EnvReader.of(env)).build();
      assertThat(withClient.region()).isEmpty();
      assertThat(withClient.profile()).isEmpty();
      assertThat(withClient.accessKeyId()).isEmpty();
    } finally {
      sdk.close();
    }
  }

  @Test
  void fromEnvErrorsNameTheVariable() {
    String p = EnvReader.DEFAULT_PREFIX;
    assertThatThrownBy(() -> CloudWatchLogsConfig.fromEnv(Map.of(p + "OVERSIZE_POLICY", "x")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(p + "OVERSIZE_POLICY");
    assertThatThrownBy(() -> CloudWatchLogsConfig.fromEnv(Map.of(p + "MAX_RETRIES", "-1")))
        .hasMessageContaining(p + "MAX_RETRIES");
    assertThatThrownBy(() -> CloudWatchLogsConfig.fromEnv(Map.of(p + "MAX_EVENT_BYTES", "x")))
        .hasMessageContaining(p + "MAX_EVENT_BYTES");
    CloudWatchLogsConfig ca =
        CloudWatchLogsConfig.fromEnv(Map.of("AWS_CA_BUNDLE", "/etc/ssl/certs/ca-certificates.crt"));
    assertThat(ca.tls().caCertPath()).isPresent();
    CloudWatchLogsConfig mtls =
        CloudWatchLogsConfig.fromEnv(
            Map.of(p + "CLIENT_CERT", "/nope/cert.pem", p + "CLIENT_KEY", "/nope/key.pem"));
    assertThat(mtls.tls().clientCertPath()).isPresent();
  }

  @Test
  void oversizePolicy() {
    assertThat(OversizePolicy.parse(" drop ")).isEqualTo(OversizePolicy.DROP);
    assertThat(OversizePolicy.ERROR.wireValue()).isEqualTo("error");
    assertThatThrownBy(() -> OversizePolicy.parse(null)).isInstanceOf(ConfigException.class);
  }
}
