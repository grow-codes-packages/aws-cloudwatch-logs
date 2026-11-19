// Service-owned: config and core client tests (health, ping, serverVersion, close, hooks, UA).

import { inspect } from "node:util";
import { CloudWatchLogsClient as AwsCloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import { afterEach, describe, expect, it } from "vitest";
import {
  API_VERSION,
  CloudWatchLogsClient,
  CloudWatchLogsConfig,
  CloudWatchLogsError,
  ConfigError,
  DEFAULT_MAX_EVENT_BYTES,
  GROW_USER_AGENT,
  REDACTED,
  RetryPolicy,
  Secret,
  VENDOR_ENV,
  defaultLogStreamName,
  isValidLogGroupName,
  isValidLogStreamName,
  type RequestInfo,
  type ResponseInfo,
  type ErrorInfo,
} from "../src/index.js";
import {
  type FakeCloudWatch,
  TEST_CREDENTIALS,
  awsError,
  fixtureResponse,
  loopbackDispatcher,
  ok,
  startFakeCloudWatch,
  testClient,
} from "./helpers/aws.js";
import { closedPort } from "./helpers/server.js";

const fakes: FakeCloudWatch[] = [];
const clients: CloudWatchLogsClient[] = [];
afterEach(async () => {
  await Promise.all(clients.splice(0).map((c) => c.close()));
  await Promise.all(fakes.splice(0).map((f) => f.close()));
});

async function fake(handler: Parameters<typeof startFakeCloudWatch>[0]): Promise<FakeCloudWatch> {
  const f = await startFakeCloudWatch(handler);
  fakes.push(f);
  return f;
}

function client(
  f: FakeCloudWatch,
  options: ConstructorParameters<typeof CloudWatchLogsConfig>[0] = {},
) {
  const c = testClient(f, options);
  clients.push(c);
  return c;
}

describe("CloudWatchLogsConfig", () => {
  it("uses defaults and freezes", () => {
    const c = new CloudWatchLogsConfig();
    expect(c.url).toBe("https://logs.us-east-1.amazonaws.com");
    expect(c.endpoint).toBeUndefined();
    expect(c.requestTimeoutMs).toBe(30_000);
    expect(c.retry).toBeInstanceOf(RetryPolicy);
    expect(c.maxEventBytes).toBe(DEFAULT_MAX_EVENT_BYTES);
    expect(c.oversizePolicy).toBe("truncate");
    expect(c.autoCreate).toBe(true);
    expect(c.roleSessionName).toBe("grow-sdk");
    expect(c.logStream).toMatch(/^.+-\d+-[0-9a-f]{6}$/);
    expect(Object.isFrozen(c)).toBe(true);
    expect(CloudWatchLogsConfig.from(c)).toBe(c);
    expect(CloudWatchLogsConfig.ENV_PREFIX).toBe("GROW_AWS_CLOUDWATCH_LOGS_");
    expect(API_VERSION).toBe("2014-03-28");
    expect(new CloudWatchLogsConfig({ region: "eu-west-1" }).url).toBe(
      "https://logs.eu-west-1.amazonaws.com",
    );
  });

  it("keeps every option and redacts credentials", () => {
    const hooks = {};
    const logger = { debug() {}, info() {}, warn() {}, error() {} };
    const c = new CloudWatchLogsConfig({
      url: "http://moto:5000",
      region: "us-east-1",
      accessKeyId: "AKID",
      secretAccessKey: "super-secret-value",
      sessionToken: new Secret("session-token-value"),
      profile: "dev",
      roleArn: "arn:aws:iam::123456789012:role/x",
      roleSessionName: "s",
      externalId: "ext-id-value",
      logGroup: "app/logs",
      logStream: "s1",
      accountId: "123456789012",
      maxEventBytes: 1000,
      oversizePolicy: "drop",
      autoCreate: false,
      proxy: "http://p:1",
      userAgentSuffix: "suffix",
      hooks,
      logger,
    });
    expect(c.endpoint).toBe("http://moto:5000");
    expect(c.url).toBe("http://moto:5000");
    expect(c.secretAccessKey?.reveal()).toBe("super-secret-value");
    expect(c.knownSecrets()).toEqual(["super-secret-value", "session-token-value"]);
    expect(c.hooks).toBe(hooks);
    expect(c.logger).toBe(logger);
    expect(c.proxy).toBe("http://p:1");
    expect(c.userAgentSuffix).toBe("suffix");
    const text = `${c.toString()} ${inspect(c)} ${JSON.stringify(c)}`;
    for (const secret of ["super-secret-value", "session-token-value", "ext-id-value"]) {
      expect(text).not.toContain(secret);
    }
    expect(c.toJSON()).toMatchObject({
      secretAccessKey: REDACTED,
      sessionToken: REDACTED,
      externalId: REDACTED,
      logGroup: "app/logs",
      logStream: "s1",
      oversizePolicy: "drop",
      autoCreate: false,
      sdkClient: null,
      credentials: null,
    });
    const withCreds = new CloudWatchLogsConfig({
      credentials: { accessKeyId: "a", secretAccessKey: "b" },
    });
    expect(withCreds.toJSON().credentials).toBe(REDACTED);
    const sdk = new AwsCloudWatchLogsClient({ region: "us-east-1" });
    expect(new CloudWatchLogsConfig({ sdkClient: sdk }).toJSON().sdkClient).toBe(
      "CloudWatchLogsClient",
    );
    sdk.destroy();
  });

  it("validates", () => {
    const bad: ConstructorParameters<typeof CloudWatchLogsConfig>[0][] = [
      { url: "ftp://x" },
      { region: "US EAST" },
      { accessKeyId: "a" },
      { secretAccessKey: "b" },
      { sessionToken: "t" },
      {
        accessKeyId: "a",
        secretAccessKey: "b",
        credentials: { accessKeyId: "a", secretAccessKey: "b" },
      },
      { roleArn: "role" },
      { logGroup: "bad name" },
      { logStream: "a:b" },
      { accountId: "12" },
      { maxEventBytes: 10 },
      { maxEventBytes: 2_000_000 },
      { oversizePolicy: "explode" as never },
      { connectTimeoutMs: 0 },
    ];
    for (const options of bad) expect(() => new CloudWatchLogsConfig(options)).toThrow(ConfigError);
    const sdk = new AwsCloudWatchLogsClient({ region: "us-east-1" });
    expect(() => new CloudWatchLogsConfig({ sdkClient: sdk, region: "us-east-1" })).toThrow(
      /sdkClient is mutually exclusive with region/,
    );
    sdk.destroy();
  });

  it("names and default stream", () => {
    expect(isValidLogGroupName("a/b-c_d.e#f")).toBe(true);
    expect(isValidLogGroupName("")).toBe(false);
    expect(isValidLogGroupName("x".repeat(513))).toBe(false);
    expect(isValidLogStreamName("s")).toBe(true);
    expect(isValidLogStreamName("a*b")).toBe(false);
    expect(defaultLogStreamName()).toMatch(new RegExp(`-${process.pid}-[0-9a-f]{6}$`));
  });

  it("fromEnv: GROW_* > AWS/vendor variables > defaults", () => {
    const env = {
      GROW_AWS_CLOUDWATCH_LOGS_URL: "http://grow:1",
      AWS_ENDPOINT_URL_CLOUDWATCH_LOGS: "http://vendor:2",
      AWS_REGION: "eu-west-1",
      AWS_DEFAULT_REGION: "us-west-2",
      AWS_ACCESS_KEY_ID: "AKIDENV",
      AWS_SECRET_ACCESS_KEY: "env-secret",
      AWS_SESSION_TOKEN: "env-session",
      AWS_PROFILE: "p1",
      CLOUDWATCH_LOG_GROUP: "vendor/group",
      GROW_AWS_CLOUDWATCH_LOGS_LOG_STREAM: "grow-stream",
      CLOUDWATCH_LOG_STREAM: "vendor-stream",
      GROW_AWS_CLOUDWATCH_LOGS_ROLE_ARN: "arn:aws:iam::1:role/r",
      GROW_AWS_CLOUDWATCH_LOGS_ROLE_SESSION_NAME: "sess",
      GROW_AWS_CLOUDWATCH_LOGS_EXTERNAL_ID: "ext",
      GROW_AWS_CLOUDWATCH_LOGS_ACCOUNT_ID: "123456789012",
      GROW_AWS_CLOUDWATCH_LOGS_MAX_EVENT_BYTES: "4096",
      GROW_AWS_CLOUDWATCH_LOGS_OVERSIZE_POLICY: "ERROR",
      GROW_AWS_CLOUDWATCH_LOGS_AUTO_CREATE: "no",
      GROW_AWS_CLOUDWATCH_LOGS_MAX_RETRIES: "5",
      GROW_AWS_CLOUDWATCH_LOGS_REQUEST_TIMEOUT: "2s",
    };
    const c = CloudWatchLogsConfig.fromEnv({}, env);
    expect(c.endpoint).toBe("http://grow:1");
    expect(c.region).toBe("eu-west-1");
    expect(c.accessKeyId).toBe("AKIDENV");
    expect(c.secretAccessKey?.reveal()).toBe("env-secret");
    expect(c.sessionToken?.reveal()).toBe("env-session");
    expect(c.profile).toBe("p1");
    expect(c.logGroup).toBe("vendor/group");
    expect(c.logStream).toBe("grow-stream");
    expect(c.roleArn).toBe("arn:aws:iam::1:role/r");
    expect(c.roleSessionName).toBe("sess");
    expect(c.externalId).toBe("ext");
    expect(c.accountId).toBe("123456789012");
    expect(c.maxEventBytes).toBe(4096);
    expect(c.oversizePolicy).toBe("error");
    expect(c.autoCreate).toBe(false);
    expect(c.retry.maxRetries).toBe(5);
    expect(c.requestTimeoutMs).toBe(2000);

    // vendor fallbacks, explicit overrides win
    const v = CloudWatchLogsConfig.fromEnv(
      { logGroup: "explicit" },
      { AWS_ENDPOINT_URL: "http://generic:3", AWS_DEFAULT_REGION: "us-west-2" },
    );
    expect(v.endpoint).toBe("http://generic:3");
    expect(v.region).toBe("us-west-2");
    expect(v.logGroup).toBe("explicit");
    expect(v.accessKeyId).toBeUndefined();
    expect(VENDOR_ENV.url).toEqual(["AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "AWS_ENDPOINT_URL"]);
  });

  it("fromEnv: GROW_* credentials come from one layer only", () => {
    const c = CloudWatchLogsConfig.fromEnv(
      {},
      {
        GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID: "AKIDGROW",
        GROW_AWS_CLOUDWATCH_LOGS_SECRET_ACCESS_KEY: "grow-secret",
        GROW_AWS_CLOUDWATCH_LOGS_SESSION_TOKEN: "grow-session",
        AWS_SECRET_ACCESS_KEY: "aws-secret",
      },
    );
    expect(c.accessKeyId).toBe("AKIDGROW");
    expect(c.secretAccessKey?.reveal()).toBe("grow-secret");
    expect(c.sessionToken?.reveal()).toBe("grow-session");
    expect(() =>
      CloudWatchLogsConfig.fromEnv({}, { GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID: "AKID" }),
    ).toThrow(/accessKeyId requires secretAccessKey/);
  });

  it("fromEnv: an explicit sdkClient or credentials replaces the environment", () => {
    const env = {
      AWS_ACCESS_KEY_ID: "AKIDENV",
      AWS_SECRET_ACCESS_KEY: "env-secret",
      AWS_REGION: "eu-west-1",
      AWS_ENDPOINT_URL: "http://x:1",
      AWS_PROFILE: "p",
      GROW_AWS_CLOUDWATCH_LOGS_ROLE_ARN: "arn:aws:iam::1:role/r",
    };
    const sdk = new AwsCloudWatchLogsClient({ region: "us-east-1" });
    const c = CloudWatchLogsConfig.fromEnv({ sdkClient: sdk }, env);
    expect(c.sdkClient).toBe(sdk);
    expect(c.accessKeyId).toBeUndefined();
    expect(c.region).toBeUndefined();
    sdk.destroy();
    const d = CloudWatchLogsConfig.fromEnv(
      { credentials: { accessKeyId: "x", secretAccessKey: "y" } },
      env,
    );
    expect(d.accessKeyId).toBeUndefined();
    expect(d.region).toBe("eu-west-1");
  });

  it("fromEnv: invalid values name the variable, never the value", () => {
    expect(() =>
      CloudWatchLogsConfig.fromEnv({}, { GROW_AWS_CLOUDWATCH_LOGS_OVERSIZE_POLICY: "secretish" }),
    ).toThrow(/GROW_AWS_CLOUDWATCH_LOGS_OVERSIZE_POLICY/);
    try {
      CloudWatchLogsConfig.fromEnv({}, { GROW_AWS_CLOUDWATCH_LOGS_MAX_EVENT_BYTES: "lots-xyz" });
    } catch (err) {
      expect(String(err)).not.toContain("lots-xyz");
    }
  });
});

describe("CloudWatchLogsClient core", () => {
  it("health is ok on a signed DescribeLogGroups and uses the log group prefix", async () => {
    const f = await fake(() => fixtureResponse("ping.ok"));
    const c = client(f, { logGroup: "grow-bootstrap/seed" });
    expect(await c.health()).toEqual({ healthy: true, status: "ok", details: {} });
    expect(f.ops()).toEqual(["DescribeLogGroups"]);
    expect(f.requests[0]?.body).toEqual({ limit: 1, logGroupNamePrefix: "grow-bootstrap/seed" });
    expect(String(f.requests[0]?.headers.authorization)).toMatch(/^AWS4-HMAC-SHA256 /);
  });

  it("health maps auth failures, throttling, 5xx and network errors without throwing", async () => {
    const auth = client(await fake(() => fixtureResponse("ping.auth-unknown-key")));
    expect(await auth.health()).toEqual({
      healthy: true,
      status: "ok",
      details: { auth: "failed", code: "AUTHENTICATION" },
    });
    const denied = client(await fake(() => fixtureResponse("deleteLogGroup.access-denied")));
    expect((await denied.health()).details.code).toBe("PERMISSION_DENIED");
    const throttled = client(await fake(() => awsError("ThrottlingException")));
    expect(await throttled.health()).toMatchObject({ healthy: true, status: "degraded" });
    const broken = client(await fake(() => fixtureResponse("stopQuery.moto-500")), {
      retry: { maxRetries: 3 },
    });
    expect(await broken.health()).toEqual({
      healthy: false,
      status: "unhealthy",
      details: { httpStatus: "500", error: "UNAVAILABLE" },
    });
    // the probe never retries
    expect(fakes.at(-1)?.requests).toHaveLength(1);
    const port = await closedPort();
    const down = new CloudWatchLogsClient(
      { ...TEST_CREDENTIALS, url: `http://127.0.0.1:${port}` },
      { dispatcher: loopbackDispatcher() },
    );
    clients.push(down);
    expect(await down.health()).toEqual({
      healthy: false,
      status: "unreachable",
      details: { error: "UNAVAILABLE" },
    });
  });

  it("health rethrows cancellation", async () => {
    const c = client(await fake(() => fixtureResponse("ping.ok")));
    const ac = new AbortController();
    ac.abort();
    await expect(c.health({ signal: ac.signal })).rejects.toMatchObject({ code: "CANCELLED" });
  });

  it("ping returns latency and maps auth errors from Moto's XML bodies", async () => {
    const c = client(await fake(() => fixtureResponse("ping.ok")));
    expect(await c.ping()).toBeGreaterThanOrEqual(0);
    for (const [name, code] of [
      ["ping.auth-unknown-key", "AUTHENTICATION"],
      ["ping.auth-bad-signature", "AUTHENTICATION"],
      ["deleteLogGroup.access-denied", "PERMISSION_DENIED"],
    ] as const) {
      const bad = client(await fake(() => fixtureResponse(name)));
      const err = await bad.ping().catch((e: unknown) => e as CloudWatchLogsError);
      expect(err).toBeInstanceOf(CloudWatchLogsError);
      expect(err).toMatchObject({ code, httpStatus: 403 });
      expect((err as CloudWatchLogsError).requestId).toBe(
        fixtureResponse(name).headers?.["x-amzn-requestid"],
      );
      expect((err as Error).message).not.toContain(TEST_CREDENTIALS.secretAccessKey);
    }
  });

  it("serverVersion is unsupported", async () => {
    const c = client(await fake(() => ok({})));
    await expect(c.serverVersion()).rejects.toMatchObject({ code: "UNSUPPORTED" });
  });

  it("close is idempotent and later calls fail with CANCELLED", async () => {
    const c = client(await fake(() => fixtureResponse("ping.ok")));
    await c.close();
    await c.close();
    await expect(c.ping()).rejects.toMatchObject({ code: "CANCELLED" });
    await expect(c.ensureLogGroup("app/web")).rejects.toMatchObject({ code: "CANCELLED" });
  });

  it("never destroys a caller-supplied SDK client", async () => {
    const f = await fake(() => fixtureResponse("ping.ok"));
    const sdk = new AwsCloudWatchLogsClient({
      region: "us-east-1",
      endpoint: f.url,
      credentials: { accessKeyId: "a", secretAccessKey: "b" },
    });
    const c = new CloudWatchLogsClient({ sdkClient: sdk });
    expect(c.sdk).toBe(sdk);
    expect(await c.ping()).toBeGreaterThanOrEqual(0);
    expect((await c.health()).healthy).toBe(true);
    await c.close();
    expect(await sdk.config.region()).toBe("us-east-1");
    sdk.destroy();
  });

  it("sends the Grow User-Agent and extra headers, fires hooks, retries per maxRetries", async () => {
    const f = await fake((_req, i) =>
      i === 0 ? { status: 503, body: "" } : fixtureResponse("ping.ok"),
    );
    const requests: RequestInfo[] = [];
    const responses: ResponseInfo[] = [];
    const errors: ErrorInfo[] = [];
    const c = client(f, {
      retry: { maxRetries: 2 },
      headers: { "x-extra": "1" },
      userAgentSuffix: "my-app/2",
      hooks: {
        onRequest: (r) => requests.push(r),
        onResponse: (r) => responses.push(r),
        onError: (e) => errors.push(e),
      },
    });
    await c.ping();
    expect(f.requests).toHaveLength(2);
    expect(String(f.requests[0]?.headers["user-agent"])).toContain(`${GROW_USER_AGENT} my-app/2`);
    expect(f.requests[0]?.headers["x-extra"]).toBe("1");
    expect(requests.map((r) => r.attempt)).toEqual([1, 2]);
    expect(requests[0]?.headers.authorization).toBe(REDACTED);
    expect(responses.map((r) => r.status)).toEqual([503, 200]);
    expect(responses[1]?.requestId).toBeDefined();
    expect(errors).toEqual([]);
    expect(JSON.stringify(requests)).not.toContain(TEST_CREDENTIALS.secretAccessKey);
  });

  it("network errors fire onError and map to UNAVAILABLE", async () => {
    const port = await closedPort();
    const errors: ErrorInfo[] = [];
    const c = new CloudWatchLogsClient(
      {
        ...TEST_CREDENTIALS,
        url: `http://127.0.0.1:${port}`,
        retry: { maxRetries: 1 },
        hooks: { onError: (e) => errors.push(e) },
      },
      { dispatcher: loopbackDispatcher() },
    );
    clients.push(c);
    const err = await c.ping().catch((e: unknown) => e as CloudWatchLogsError);
    expect(err).toMatchObject({ code: "UNAVAILABLE", retryable: true });
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0]?.willRetry).toBe(true);
  });

  it("per-attempt and total timeouts map to TIMEOUT; cancellation to CANCELLED", async () => {
    const slow = await fake(() => ({ ...fixtureResponse("ping.ok"), delayMs: 400 }));
    const attempt = client(slow, { requestTimeoutMs: 50 });
    await expect(attempt.ping()).rejects.toMatchObject({ code: "TIMEOUT" });
    const total = client(slow, { totalTimeoutMs: 60, retry: { maxRetries: 0 } });
    await expect(total.ping()).rejects.toMatchObject({
      code: "TIMEOUT",
      message: "ping: timed out after 60 ms",
    });
    const cancel = client(slow);
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 30);
    await expect(cancel.ping({ signal: ac.signal })).rejects.toMatchObject({ code: "CANCELLED" });
  });

  it("missing region and missing credentials map to CONFIG / AUTHENTICATION", async () => {
    const f = await fake(() => fixtureResponse("ping.ok"));
    const saved = { ...process.env };
    for (const k of [
      "AWS_REGION",
      "AWS_DEFAULT_REGION",
      "AWS_PROFILE",
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_SESSION_TOKEN",
    ]) {
      delete process.env[k];
    }
    process.env.AWS_CONFIG_FILE = "/nonexistent/config";
    process.env.AWS_SHARED_CREDENTIALS_FILE = "/nonexistent/credentials";
    process.env.AWS_EC2_METADATA_DISABLED = "true";
    try {
      const noRegion = new CloudWatchLogsClient(
        { url: f.url, accessKeyId: "a", secretAccessKey: "b" },
        { dispatcher: loopbackDispatcher() },
      );
      clients.push(noRegion);
      await expect(noRegion.ping()).rejects.toMatchObject({ code: "CONFIG" });
      const noCreds = new CloudWatchLogsClient(
        { url: f.url, region: "us-east-1" },
        { dispatcher: loopbackDispatcher() },
      );
      clients.push(noCreds);
      await expect(noCreds.ping()).rejects.toMatchObject({ code: "AUTHENTICATION" });
    } finally {
      process.env = saved;
    }
  });

  it("builds credentials from a session token, a provider or an assumed role", async () => {
    const f = await fake((req) =>
      req.target === "" ? { status: 500 } : fixtureResponse("ping.ok"),
    );
    const withToken = client(f, { sessionToken: "session-token-xyz" });
    await withToken.ping();
    expect(f.requests.at(-1)?.headers["x-amz-security-token"]).toBe("session-token-xyz");
    const provider = new CloudWatchLogsClient(
      {
        region: "us-east-1",
        url: f.url,
        credentials: async () => ({ accessKeyId: "AKIDPROV", secretAccessKey: "prov" }),
      },
      { dispatcher: loopbackDispatcher() },
    );
    clients.push(provider);
    await provider.ping();
    expect(String(f.requests.at(-1)?.headers.authorization)).toContain("AKIDPROV");
    const role = client(f, { roleArn: "arn:aws:iam::123456789012:role/r", externalId: "e" });
    expect(role.config.roleArn).toBeDefined();
  });

  it("fromEnv builds a client", () => {
    const saved = { ...process.env };
    process.env.GROW_AWS_CLOUDWATCH_LOGS_REGION = "us-east-1";
    try {
      const c = CloudWatchLogsClient.fromEnv({ logGroup: "x" });
      clients.push(c);
      expect(c.config.logGroup).toBe("x");
      expect(c.config.region).toBe("us-east-1");
    } finally {
      process.env = saved;
    }
  });
});
