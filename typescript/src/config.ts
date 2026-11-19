// Service-owned: configuration of the Amazon CloudWatch Logs client.

import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import type { CloudWatchLogsClient as AwsCloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import type { AwsCredentialIdentity, AwsCredentialIdentityProvider } from "@smithy/types";
import {
  type ClientLogger,
  type CommonConfig,
  type CommonConfigOptions,
  type CommonVendorEnv,
  ConfigError,
  type EnvMap,
  EnvReader,
  type Hooks,
  REDACTED,
  type RetryPolicy,
  Secret,
  type TlsOptions,
  describeCommonConfig,
  mergeOptions,
  readCommonEnv,
  resolveCommonConfig,
  secretValues,
  validateCommonConfig,
} from "./core/index.js";

const INSPECT = Symbol.for("nodejs.util.inspect.custom");

/** What happens to a log event whose message is larger than `maxEventBytes`. */
export type OversizePolicy = "truncate" | "drop" | "error";

/** Default per-event size limit in UTF-8 bytes (256 KiB). */
export const DEFAULT_MAX_EVENT_BYTES = 262_144;

/** Largest message PutLogEvents accepts: 1 MiB minus the 26 bytes AWS counts per event. */
export const MAX_EVENT_BYTES_HARD_CAP = 1_048_550;

/** Default STS role session name used with `roleArn`. */
export const DEFAULT_ROLE_SESSION_NAME = "grow-sdk";

/** CloudWatch Logs API version (`2014-03-28`). */
export const API_VERSION = "2014-03-28";

/** Options accepted by {@link CloudWatchLogsConfig} (all optional). */
export interface CloudWatchLogsConfigOptions extends CommonConfigOptions {
  /**
   * Endpoint override (env `GROW_AWS_CLOUDWATCH_LOGS_URL` > `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS` >
   * `AWS_ENDPOINT_URL`). Unset: the AWS SDK resolves `https://logs.<region>.amazonaws.com`.
   */
  url?: string;
  /** AWS region (env `GROW_AWS_CLOUDWATCH_LOGS_REGION` > `AWS_REGION` > `AWS_DEFAULT_REGION`). */
  region?: string;
  /** Access key id (env `GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID` > `AWS_ACCESS_KEY_ID`). */
  accessKeyId?: string;
  /** Secret access key (env `GROW_AWS_CLOUDWATCH_LOGS_SECRET_ACCESS_KEY` > `AWS_SECRET_ACCESS_KEY`). */
  secretAccessKey?: string | Secret;
  /** Session token (env `GROW_AWS_CLOUDWATCH_LOGS_SESSION_TOKEN` > `AWS_SESSION_TOKEN`). */
  sessionToken?: string | Secret;
  /** Shared config profile (env `GROW_AWS_CLOUDWATCH_LOGS_PROFILE` > `AWS_PROFILE`). */
  profile?: string;
  /** An AWS SDK credentials object or provider (instead of the key fields or the default chain). */
  credentials?: AwsCredentialIdentity | AwsCredentialIdentityProvider;
  /** Role assumed with STS on top of the base credentials (env `GROW_AWS_CLOUDWATCH_LOGS_ROLE_ARN`). */
  roleArn?: string;
  /** STS session name (default `grow-sdk`, env `GROW_AWS_CLOUDWATCH_LOGS_ROLE_SESSION_NAME`). */
  roleSessionName?: string;
  /** STS external id (env `GROW_AWS_CLOUDWATCH_LOGS_EXTERNAL_ID`). */
  externalId?: string;
  /** Default log group of the `LogClient` and the query client (env `…_LOG_GROUP` > `CLOUDWATCH_LOG_GROUP`). */
  logGroup?: string;
  /**
   * Log stream of the `LogClient` (env `…_LOG_STREAM` > `CLOUDWATCH_LOG_STREAM`).
   * Default `<hostname>-<pid>-<random6>`, created on first use.
   */
  logStream?: string;
  /** AWS account id used to build log group ARNs for live tail (env `…_ACCOUNT_ID`); else looked up. */
  accountId?: string;
  /** Per-event message limit in UTF-8 bytes (default 262 144, max 1 048 550; env `…_MAX_EVENT_BYTES`). */
  maxEventBytes?: number;
  /** Oversized events: `truncate` (default), `drop` or `error` (env `…_OVERSIZE_POLICY`). */
  oversizePolicy?: OversizePolicy;
  /** `putLogEvents` creates a missing log stream and retries once (default true; env `…_AUTO_CREATE`). */
  autoCreate?: boolean;
  /**
   * An existing AWS SDK `CloudWatchLogsClient` to use instead of building one. Mutually exclusive
   * with the credential, region and endpoint fields; `close()` never destroys it and the Grow
   * hooks, TLS, proxy, timeout and User-Agent settings do not apply to it.
   */
  sdkClient?: AwsCloudWatchLogsClient;
}

/** Standard AWS variables read after `GROW_AWS_CLOUDWATCH_LOGS_*`, in order. */
export const VENDOR_ENV: CommonVendorEnv & {
  region: readonly string[];
  accessKeyId: readonly string[];
  secretAccessKey: readonly string[];
  sessionToken: readonly string[];
  profile: readonly string[];
  logGroup: readonly string[];
  logStream: readonly string[];
} = {
  url: ["AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "AWS_ENDPOINT_URL"],
  region: ["AWS_REGION", "AWS_DEFAULT_REGION"],
  accessKeyId: ["AWS_ACCESS_KEY_ID"],
  secretAccessKey: ["AWS_SECRET_ACCESS_KEY"],
  sessionToken: ["AWS_SESSION_TOKEN"],
  profile: ["AWS_PROFILE"],
  logGroup: ["CLOUDWATCH_LOG_GROUP"],
  logStream: ["CLOUDWATCH_LOG_STREAM"],
};

/** `true` when `name` is a valid CloudWatch Logs group name (1–512 of `[.\-_/#A-Za-z0-9]`). */
export function isValidLogGroupName(name: string): boolean {
  return /^[.\-_/#A-Za-z0-9]{1,512}$/.test(name);
}

/** `true` when `name` is a valid log stream name (1–512 characters, no `:` or `*`). */
export function isValidLogStreamName(name: string): boolean {
  return name.length >= 1 && name.length <= 512 && !/[:*]/.test(name);
}

/** Default stream name: `<hostname>-<pid>-<random6>` (characters outside the allowed set become `-`). */
export function defaultLogStreamName(): string {
  const host = hostname().replace(/[:*\s]/g, "-") || "host";
  return `${host}-${process.pid}-${randomBytes(3).toString("hex")}`;
}

function blankToUndefined(value: string | undefined): string | undefined {
  return value === undefined || value.trim() === "" ? undefined : value;
}

/** Immutable configuration of {@link CloudWatchLogsClient}. */
export class CloudWatchLogsConfig implements CommonConfig {
  /** Prefix of every environment variable. */
  static readonly ENV_PREFIX = "GROW_AWS_CLOUDWATCH_LOGS_";

  /**
   * Effective endpoint: {@link endpoint} when set, otherwise the public AWS endpoint of
   * {@link region} (`us-east-1` when the region is left to the SDK). Informational.
   */
  readonly url: string;
  /** Explicit endpoint override passed to the AWS SDK (`undefined` = the SDK resolves it). */
  readonly endpoint?: string;
  readonly connectTimeoutMs: number;
  readonly requestTimeoutMs: number;
  readonly totalTimeoutMs: number;
  readonly retry: RetryPolicy;
  readonly tls: Readonly<TlsOptions>;
  readonly proxy?: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly userAgentSuffix?: string;
  readonly hooks?: Hooks;
  readonly logger?: ClientLogger;
  /** AWS region (`undefined` = SDK chain: env, shared config). */
  readonly region?: string;
  /** Access key id. */
  readonly accessKeyId?: string;
  /** Secret access key. */
  readonly secretAccessKey?: Secret;
  /** Session token. */
  readonly sessionToken?: Secret;
  /** Shared config profile. */
  readonly profile?: string;
  /** Explicit SDK credentials. */
  readonly credentials?: AwsCredentialIdentity | AwsCredentialIdentityProvider;
  /** Role assumed with STS. */
  readonly roleArn?: string;
  /** STS session name. */
  readonly roleSessionName: string;
  /** STS external id. */
  readonly externalId?: string;
  /** Default log group. */
  readonly logGroup?: string;
  /** Log stream of the `LogClient`. */
  readonly logStream: string;
  /** Account id for live-tail ARNs. */
  readonly accountId?: string;
  /** Per-event message limit in UTF-8 bytes. */
  readonly maxEventBytes: number;
  /** Policy for oversized events. */
  readonly oversizePolicy: OversizePolicy;
  /** Create missing log streams in `putLogEvents`. */
  readonly autoCreate: boolean;
  /** Caller-supplied AWS SDK client. */
  readonly sdkClient?: AwsCloudWatchLogsClient;

  /** Applies defaults, validates (throws {@link ConfigError}) and freezes. */
  constructor(options: CloudWatchLogsConfigOptions = {}) {
    const region = blankToUndefined(options.region);
    const fallbackUrl = `https://logs.${region ?? "us-east-1"}.amazonaws.com`;
    const endpoint = blankToUndefined(options.url);
    const common = resolveCommonConfig({ ...options, url: endpoint }, fallbackUrl);
    this.url = common.url;
    if (endpoint !== undefined) this.endpoint = endpoint;
    this.connectTimeoutMs = common.connectTimeoutMs;
    this.requestTimeoutMs = common.requestTimeoutMs;
    this.totalTimeoutMs = common.totalTimeoutMs;
    this.retry = common.retry;
    this.tls = common.tls;
    this.headers = common.headers;
    if (common.proxy !== undefined) this.proxy = common.proxy;
    if (common.userAgentSuffix !== undefined) this.userAgentSuffix = common.userAgentSuffix;
    if (common.hooks !== undefined) this.hooks = common.hooks;
    if (common.logger !== undefined) this.logger = common.logger;
    if (region !== undefined) this.region = region;
    const accessKeyId = blankToUndefined(options.accessKeyId);
    if (accessKeyId !== undefined) this.accessKeyId = accessKeyId;
    const secretAccessKey = Secret.from(options.secretAccessKey);
    if (secretAccessKey !== undefined) this.secretAccessKey = secretAccessKey;
    const sessionToken = Secret.from(options.sessionToken);
    if (sessionToken !== undefined) this.sessionToken = sessionToken;
    const profile = blankToUndefined(options.profile);
    if (profile !== undefined) this.profile = profile;
    if (options.credentials !== undefined) this.credentials = options.credentials;
    const roleArn = blankToUndefined(options.roleArn);
    if (roleArn !== undefined) this.roleArn = roleArn;
    this.roleSessionName = blankToUndefined(options.roleSessionName) ?? DEFAULT_ROLE_SESSION_NAME;
    const externalId = blankToUndefined(options.externalId);
    if (externalId !== undefined) this.externalId = externalId;
    const logGroup = blankToUndefined(options.logGroup);
    if (logGroup !== undefined) this.logGroup = logGroup;
    this.logStream = blankToUndefined(options.logStream) ?? defaultLogStreamName();
    const accountId = blankToUndefined(options.accountId);
    if (accountId !== undefined) this.accountId = accountId;
    this.maxEventBytes = options.maxEventBytes ?? DEFAULT_MAX_EVENT_BYTES;
    this.oversizePolicy = options.oversizePolicy ?? "truncate";
    this.autoCreate = options.autoCreate ?? true;
    if (options.sdkClient !== undefined) this.sdkClient = options.sdkClient;
    this.validate();
    Object.freeze(this);
  }

  /** Returns `options` when it already is a config, otherwise builds one. */
  static from(options: CloudWatchLogsConfigOptions = {}): CloudWatchLogsConfig {
    if (options instanceof CloudWatchLogsConfig) return options;
    return new CloudWatchLogsConfig(options);
  }

  /**
   * Builds a config from the environment. Precedence: explicit `overrides` >
   * `GROW_AWS_CLOUDWATCH_LOGS_<NAME>` > standard AWS / CloudWatch variables ({@link VENDOR_ENV}) >
   * defaults. Credentials left unset use the AWS SDK default chain.
   */
  static fromEnv(
    overrides: CloudWatchLogsConfigOptions = {},
    env: EnvMap = process.env,
  ): CloudWatchLogsConfig {
    const reader = new EnvReader(CloudWatchLogsConfig.ENV_PREFIX, env);
    const fromEnv: CloudWatchLogsConfigOptions = readCommonEnv(reader, VENDOR_ENV);
    const set = <K extends keyof CloudWatchLogsConfigOptions>(
      key: K,
      value: CloudWatchLogsConfigOptions[K] | undefined,
    ): void => {
      if (value !== undefined) fromEnv[key] = value;
    };
    set("region", reader.string("REGION", VENDOR_ENV.region));
    // Credentials come from a single layer: a GROW_* key id never pairs with an AWS_* secret.
    const growKey = reader.string("ACCESS_KEY_ID");
    if (growKey !== undefined) {
      set("accessKeyId", growKey);
      set("secretAccessKey", reader.secret("SECRET_ACCESS_KEY"));
      set("sessionToken", reader.secret("SESSION_TOKEN"));
    } else {
      const noPrefix = new EnvReader("", env);
      set("accessKeyId", noPrefix.string(VENDOR_ENV.accessKeyId[0] as string));
      set("secretAccessKey", noPrefix.secret(VENDOR_ENV.secretAccessKey[0] as string));
      set("sessionToken", noPrefix.secret(VENDOR_ENV.sessionToken[0] as string));
    }
    set("profile", reader.string("PROFILE", VENDOR_ENV.profile));
    set("roleArn", reader.string("ROLE_ARN"));
    set("roleSessionName", reader.string("ROLE_SESSION_NAME"));
    set("externalId", reader.string("EXTERNAL_ID"));
    set("logGroup", reader.string("LOG_GROUP", VENDOR_ENV.logGroup));
    set("logStream", reader.string("LOG_STREAM", VENDOR_ENV.logStream));
    set("accountId", reader.string("ACCOUNT_ID"));
    set("maxEventBytes", reader.int("MAX_EVENT_BYTES"));
    const policy = reader.lookup("OVERSIZE_POLICY");
    if (policy !== undefined) {
      const value = policy.value.trim().toLowerCase();
      if (value !== "truncate" && value !== "drop" && value !== "error") {
        throw new ConfigError(
          `invalid value in environment variable ${policy.name}: expected truncate, drop or error`,
        );
      }
      fromEnv.oversizePolicy = value;
    }
    set("autoCreate", reader.bool("AUTO_CREATE"));
    const merged = mergeOptions<CloudWatchLogsConfigOptions>(fromEnv, overrides);
    if (overrides.sdkClient !== undefined || overrides.credentials !== undefined) {
      // An explicit client or credentials object replaces the credentials found in the environment.
      delete merged.accessKeyId;
      delete merged.secretAccessKey;
      delete merged.sessionToken;
      if (overrides.sdkClient !== undefined) {
        if (overrides.region === undefined) delete merged.region;
        if (overrides.url === undefined) delete merged.url;
        if (overrides.profile === undefined) delete merged.profile;
        if (overrides.roleArn === undefined) delete merged.roleArn;
      }
    }
    return new CloudWatchLogsConfig(merged);
  }

  /** Throws {@link ConfigError} when a setting is invalid. */
  validate(): void {
    validateCommonConfig(this);
    const problems: string[] = [];
    if (this.region !== undefined && !/^[a-z0-9-]+$/.test(this.region)) {
      problems.push("region must look like us-east-1");
    }
    if (this.accessKeyId !== undefined && this.secretAccessKey === undefined) {
      problems.push("accessKeyId requires secretAccessKey");
    }
    if (this.accessKeyId === undefined && this.secretAccessKey !== undefined) {
      problems.push("secretAccessKey requires accessKeyId");
    }
    if (this.sessionToken !== undefined && this.accessKeyId === undefined) {
      problems.push("sessionToken requires accessKeyId and secretAccessKey");
    }
    if (this.credentials !== undefined && this.accessKeyId !== undefined) {
      problems.push("credentials and accessKeyId/secretAccessKey are mutually exclusive");
    }
    if (this.sdkClient !== undefined) {
      const clash = (
        [
          ["accessKeyId", this.accessKeyId],
          ["credentials", this.credentials],
          ["profile", this.profile],
          ["roleArn", this.roleArn],
          ["region", this.region],
          ["url", this.endpoint],
        ] as const
      ).filter(([, v]) => v !== undefined);
      if (clash.length > 0) {
        problems.push(`sdkClient is mutually exclusive with ${clash.map(([k]) => k).join(", ")}`);
      }
    }
    if (this.roleArn !== undefined && !this.roleArn.startsWith("arn:")) {
      problems.push("roleArn must be an ARN");
    }
    if (this.logGroup !== undefined && !isValidLogGroupName(this.logGroup)) {
      problems.push("logGroup must be 1-512 characters of [.-_/#A-Za-z0-9]");
    }
    if (!isValidLogStreamName(this.logStream)) {
      problems.push("logStream must be 1-512 characters without ':' or '*'");
    }
    if (this.accountId !== undefined && !/^\d{12}$/.test(this.accountId)) {
      problems.push("accountId must be 12 digits");
    }
    if (
      !Number.isInteger(this.maxEventBytes) ||
      this.maxEventBytes < 64 ||
      this.maxEventBytes > MAX_EVENT_BYTES_HARD_CAP
    ) {
      problems.push(`maxEventBytes must be an integer from 64 to ${MAX_EVENT_BYTES_HARD_CAP}`);
    }
    if (!["truncate", "drop", "error"].includes(this.oversizePolicy)) {
      problems.push("oversizePolicy must be truncate, drop or error");
    }
    if (problems.length > 0) {
      throw new ConfigError(`invalid configuration: ${problems.join("; ")}`);
    }
  }

  /** Credential values to redact from error messages and logs. */
  knownSecrets(): string[] {
    return secretValues(this.secretAccessKey, this.sessionToken);
  }

  /** Debug form with every credential redacted. */
  toJSON(): Record<string, unknown> {
    return {
      ...describeCommonConfig(this),
      endpoint: this.endpoint ?? null,
      region: this.region ?? null,
      accessKeyId: this.accessKeyId ?? null,
      secretAccessKey: this.secretAccessKey === undefined ? null : REDACTED,
      sessionToken: this.sessionToken === undefined ? null : REDACTED,
      profile: this.profile ?? null,
      credentials: this.credentials === undefined ? null : REDACTED,
      roleArn: this.roleArn ?? null,
      roleSessionName: this.roleSessionName,
      externalId: this.externalId === undefined ? null : REDACTED,
      logGroup: this.logGroup ?? null,
      logStream: this.logStream,
      accountId: this.accountId ?? null,
      maxEventBytes: this.maxEventBytes,
      oversizePolicy: this.oversizePolicy,
      autoCreate: this.autoCreate,
      sdkClient: this.sdkClient === undefined ? null : "CloudWatchLogsClient",
    };
  }

  toString(): string {
    return `CloudWatchLogsConfig(${JSON.stringify(this.toJSON())})`;
  }

  [INSPECT](): string {
    return this.toString();
  }
}
