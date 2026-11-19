// Service-owned: the Amazon CloudWatch Logs client, built on the official AWS SDK v3
// (`@aws-sdk/client-cloudwatch-logs`, SPEC §2.3 "Cloud SDK services").

import {
  CloudWatchLogsClient as AwsCloudWatchLogsClient,
  type CloudWatchLogsClientConfig,
  CreateLogGroupCommand,
  CreateLogStreamCommand,
  DeleteLogGroupCommand,
  DeleteLogStreamCommand,
  DeleteRetentionPolicyCommand,
  DescribeLogGroupsCommand,
  type DescribeLogGroupsCommandOutput,
  DescribeLogStreamsCommand,
  DescribeQueriesCommand,
  FilterLogEventsCommand,
  GetLogEventsCommand,
  GetQueryResultsCommand,
  ListTagsForResourceCommand,
  type LogGroup,
  PutLogEventsCommand,
  PutRetentionPolicyCommand,
  StartLiveTailCommand,
  type StartLiveTailCommandInput,
  type StartLiveTailResponseStream,
  StartQueryCommand,
  type StartQueryCommandInput,
  StopQueryCommand,
  TagResourceCommand,
  UntagResourceCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import { fromTemporaryCredentials } from "@aws-sdk/credential-providers";
import {
  CancelledError,
  CloudWatchLogsError,
  ConfigError,
  ConflictError,
  ErrorCode,
  type HealthStatus,
  NotFoundError,
  TimeoutError,
  UnavailableError,
  UnsupportedError,
  ValidationError,
  healthStatus,
  sleep,
  throwIfAborted,
} from "./core/index.js";
import { awsErrorCode, mapAwsError } from "./aws-errors.js";
import { type InputLogEvent, planLogEventBatches } from "./batching.js";
import {
  CloudWatchLogsConfig,
  type CloudWatchLogsConfigOptions,
  type OversizePolicy,
  isValidLogGroupName,
  isValidLogStreamName,
} from "./config.js";
import { type EmfOptions, buildEmf } from "./emf.js";
import { CloudWatchLogsExporter } from "./exporter.js";
import { GrowHttpHandler, type GrowHttpHandlerOptions } from "./http-handler.js";
import { BatchingLogClient, type BatchingLogClientOptions } from "./logging/index.js";
import { CloudWatchLogsQueryClient } from "./query-client.js";
import type {
  CallOptions,
  FilteredLogEvent,
  LiveTailEvent,
  LiveTailHandle,
  LogGroupInfo,
  LogStreamInfo,
  PutLogEventsResult,
  QueryInfo,
  QueryResults,
  QueryStatistics,
  QueryStatus,
  RejectedLogEventsInfo,
  StreamLogEvent,
  TimeInput,
} from "./types.js";

/** Test seams. Not part of the stable API. */
export type ClientInternals = Pick<GrowHttpHandlerOptions, "dispatcher"> & {
  /** Replaces the sleep used between Logs Insights polls. */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
};

/** Allowed `retentionDays` values (CloudWatch Logs). */
export const RETENTION_DAYS: readonly number[] = Object.freeze([
  1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288,
  3653,
]);

/** Options of {@link CloudWatchLogsClient.ensureLogGroup}. */
export interface EnsureLogGroupOptions extends CallOptions {
  /** Retention in days (one of {@link RETENTION_DAYS}); `null` = never expire; unset = leave. */
  retentionDays?: number | null;
  /** KMS key for a new group. An existing group with a different key raises `ConflictError`. */
  kmsKeyId?: string;
  /** Tags to add (existing tags are kept; values of the same keys are overwritten). */
  tags?: Record<string, string>;
}

/** Result of {@link CloudWatchLogsClient.ensureLogGroup}. */
export interface EnsureLogGroupResult {
  /** Whether this call created the group. */
  created: boolean;
  /** Group ARN without `:*`. */
  arn: string;
}

/** Options of {@link CloudWatchLogsClient.ensureLogStream}. */
export interface EnsureLogStreamOptions extends CallOptions {
  /** Create the log group first when it is missing (default false). */
  createGroup?: boolean;
}

/** Options of the delete functions. */
export interface DeleteOptions extends CallOptions {
  /** Return `false` instead of raising `NotFoundError` when it does not exist (default false). */
  ignoreMissing?: boolean;
}

/** Options of {@link CloudWatchLogsClient.describeLogGroups}. */
export interface DescribeLogGroupsOptions extends CallOptions {
  /** Name prefix. */
  prefix?: string;
  /** Groups per request, 1–50 (default 50). */
  pageSize?: number;
  /** Stop after this many groups. */
  limit?: number;
}

/** Options of {@link CloudWatchLogsClient.describeLogStreams}. */
export interface DescribeLogStreamsOptions extends CallOptions {
  /** Name prefix (not allowed with `orderBy: "LastEventTime"`). */
  prefix?: string;
  /** Sort key (default `LogStreamName`). */
  orderBy?: "LogStreamName" | "LastEventTime";
  /** Descending order. */
  descending?: boolean;
  /** Streams per request, 1–50 (default 50). */
  pageSize?: number;
  /** Stop after this many streams. */
  limit?: number;
}

/** Options of {@link CloudWatchLogsClient.putLogEvents}. */
export interface PutLogEventsOptions extends CallOptions {
  /** Create a missing log stream and retry once (default: `config.autoCreate`, true). */
  autoCreate?: boolean;
  /** With `autoCreate`, also create a missing log group (default false). */
  createGroup?: boolean;
  /** Per-event limit (default `config.maxEventBytes`). */
  maxEventBytes?: number;
  /** Oversize policy (default `config.oversizePolicy`). */
  oversizePolicy?: OversizePolicy;
}

/** Options of {@link CloudWatchLogsClient.emf}. */
export interface EmfSendOptions extends EmfOptions, CallOptions {
  /** Log group (default `config.logGroup`). */
  group?: string;
  /** Log stream (default `config.logStream`). */
  stream?: string;
}

/** Options of {@link CloudWatchLogsClient.filterLogEvents}. */
export interface FilterLogEventsOptions extends CallOptions {
  /** Log group name or ARN (default `config.logGroup`). */
  group?: string;
  /** Only these streams (max 100; not with `streamPrefix`). */
  streams?: readonly string[];
  /** Only streams with this prefix (not with `streams`). */
  streamPrefix?: string;
  /** CloudWatch filter pattern. */
  pattern?: string;
  /** Inclusive start. */
  start?: TimeInput;
  /** Inclusive end. */
  end?: TimeInput;
  /** Events per request, 1–10 000. */
  pageSize?: number;
  /** Stop after this many events. */
  limit?: number;
}

/** Options of {@link CloudWatchLogsClient.getLogEvents}. */
export interface GetLogEventsOptions extends CallOptions {
  /** `true`: oldest first; `false` (default): newest page first (ascending inside a page). */
  startFromHead?: boolean;
  /** Inclusive start. */
  start?: TimeInput;
  /** Exclusive end. */
  end?: TimeInput;
  /** Events per request, 1–10 000. */
  pageSize?: number;
  /** Stop after this many events. */
  limit?: number;
}

/** Options of {@link CloudWatchLogsClient.startQuery}. */
export interface StartQueryOptions extends CallOptions {
  /** Log group names or ARNs (max 50; default `[config.logGroup]`). */
  groups?: readonly string[];
  /** Start of the range (default: one hour before `end`). */
  start?: TimeInput;
  /** End of the range (default now). */
  end?: TimeInput;
  /** Maximum rows (1–10 000). */
  limit?: number;
}

/** Options of {@link CloudWatchLogsClient.query}. */
export interface QueryOptions extends StartQueryOptions {
  /** Give up after this many ms (default 60 000); the query is stopped best-effort. */
  timeoutMs?: number;
  /** First poll interval (default 1000 ms), doubled up to 5000 ms. */
  pollIntervalMs?: number;
  /** StartQuery attempts on `LimitExceededException` (concurrent-query quota; default 3). */
  retryOnQueryLimit?: number;
}

/** Options of {@link CloudWatchLogsClient.describeQueries}. */
export interface DescribeQueriesOptions extends CallOptions {
  /** Only queries of this log group. */
  group?: string;
  /** Only queries with this status. */
  status?: QueryStatus;
  /** Maximum queries (1–1000). */
  maxResults?: number;
}

/** What {@link CloudWatchLogsClient.startLiveTail} tails. */
export interface LiveTailRequest {
  /** Log group names or ARNs (1–10). Names are converted to ARNs. */
  groups: readonly string[];
  /** Only these streams (only with one group; not with `streamPrefixes`). */
  streams?: readonly string[];
  /** Only streams with these prefixes (only with one group). */
  streamPrefixes?: readonly string[];
  /** CloudWatch filter pattern. */
  pattern?: string;
}

/** Callbacks and options of {@link CloudWatchLogsClient.startLiveTail}. */
export interface LiveTailOptions extends CallOptions {
  /** Called with the error that ends the session (mapped to the SDK error classes). */
  onError?: (error: CloudWatchLogsError) => void;
  /** Called for every `sessionUpdate` frame (also empty keep-alive updates). */
  onUpdate?: (update: { sampled: boolean; count: number }) => void;
}

/** Options of {@link CloudWatchLogsClient.logClient}. */
export interface CloudWatchLogClientOptions extends BatchingLogClientOptions {
  /** Log group (default `config.logGroup`, required). */
  group?: string;
  /** Log stream (default `config.logStream`). */
  stream?: string;
  /** Create a missing log group on the first export (default false). */
  createGroup?: boolean;
}

const QUERY_TERMINAL: ReadonlySet<string> = new Set([
  "Complete",
  "Failed",
  "Cancelled",
  "Timeout",
  "Unknown",
]);

function toMs(value: TimeInput, name: string, operation: string): number {
  const ms = value instanceof Date ? value.getTime() : value;
  if (typeof ms !== "number" || !Number.isFinite(ms)) {
    throw new ValidationError(`${operation}: ${name} is not a valid time`);
  }
  return Math.trunc(ms);
}

function optDate(ms: number | undefined | null): Date | undefined {
  return typeof ms === "number" ? new Date(ms) : undefined;
}

function stripStar(arn: string): string {
  return arn.endsWith(":*") ? arn.slice(0, -2) : arn;
}

function checkRange(
  value: number | undefined,
  min: number,
  max: number,
  name: string,
  operation: string,
): void {
  if (value !== undefined && (!Number.isInteger(value) || value < min || value > max)) {
    throw new ValidationError(`${operation}: ${name} must be an integer from ${min} to ${max}`);
  }
}

function checkGroupName(name: string, operation: string): void {
  if (typeof name !== "string" || !isValidLogGroupName(name)) {
    throw new ValidationError(
      `${operation}: log group name must be 1-512 characters of [.-_/#A-Za-z0-9]`,
    );
  }
}

function checkStreamName(name: string, operation: string): void {
  if (typeof name !== "string" || !isValidLogStreamName(name)) {
    throw new ValidationError(
      `${operation}: log stream name must be 1-512 characters without : or *`,
    );
  }
}

function checkRetention(days: number | null | undefined, operation: string): void {
  if (days === undefined || days === null || days === 0) return;
  if (!RETENTION_DAYS.includes(days)) {
    throw new ValidationError(
      `${operation}: retentionDays must be one of ${RETENTION_DAYS.join(", ")}`,
    );
  }
}

function toGroup(g: LogGroup): LogGroupInfo {
  const name = g.logGroupName ?? "";
  const info: LogGroupInfo = { name, arn: stripStar(g.logGroupArn ?? g.arn ?? "") };
  const created = optDate(g.creationTime);
  if (created !== undefined) info.creationTime = created;
  if (typeof g.retentionInDays === "number") info.retentionDays = g.retentionInDays;
  if (typeof g.storedBytes === "number") info.storedBytes = g.storedBytes;
  if (typeof g.kmsKeyId === "string") info.kmsKeyId = g.kmsKeyId;
  if (typeof g.metricFilterCount === "number") info.metricFilterCount = g.metricFilterCount;
  if (typeof g.logGroupClass === "string") info.logGroupClass = g.logGroupClass;
  return info;
}

/** Converts a Logs Insights `@timestamp` (`YYYY-MM-DD HH:mm:ss.SSS` UTC, or epoch ms) to a Date. */
export function parseInsightsTimestamp(value: string): Date | undefined {
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) return new Date(Number(trimmed));
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?)Z?$/.exec(trimmed);
  if (m === null) return undefined;
  const d = new Date(`${m[1] as string}T${(m[2] as string).slice(0, 12)}Z`);
  return Number.isNaN(d.getTime()) ? undefined : d;
}

/** Partition of a region (`aws`, `aws-cn`, `aws-us-gov`). */
export function partitionForRegion(region: string): string {
  if (region.startsWith("cn-")) return "aws-cn";
  if (region.startsWith("us-gov-")) return "aws-us-gov";
  return "aws";
}

/** Client for Amazon CloudWatch Logs. */
export class CloudWatchLogsClient {
  /** The immutable configuration. */
  readonly config: CloudWatchLogsConfig;
  readonly #sdk: AwsCloudWatchLogsClient;
  readonly #probe: AwsCloudWatchLogsClient;
  readonly #ownsSdk: boolean;
  readonly #sleep: (ms: number, signal?: AbortSignal) => Promise<void>;
  readonly #logClients = new Set<BatchingLogClient>();
  readonly #tails = new Set<LiveTailHandle>();
  #closing: Promise<void> | null = null;
  #closed = false;

  /** Accepts a {@link CloudWatchLogsConfig} or plain options. */
  constructor(config: CloudWatchLogsConfigOptions = {}, internals: ClientInternals = {}) {
    const cfg = CloudWatchLogsConfig.from(config);
    this.config = cfg;
    this.#sleep = internals.sleep ?? sleep;
    if (cfg.sdkClient !== undefined) {
      this.#sdk = cfg.sdkClient;
      this.#probe = cfg.sdkClient;
      this.#ownsSdk = false;
      return;
    }
    const handler = new GrowHttpHandler({
      config: cfg,
      ...(internals.dispatcher === undefined ? {} : { dispatcher: internals.dispatcher }),
    });
    const base: CloudWatchLogsClientConfig = {
      requestHandler: handler,
      retryMode: "standard",
    };
    if (cfg.region !== undefined) base.region = cfg.region;
    if (cfg.endpoint !== undefined) base.endpoint = cfg.endpoint;
    if (cfg.profile !== undefined) base.profile = cfg.profile;
    let credentials: CloudWatchLogsClientConfig["credentials"];
    if (cfg.accessKeyId !== undefined && cfg.secretAccessKey !== undefined) {
      credentials = {
        accessKeyId: cfg.accessKeyId,
        secretAccessKey: cfg.secretAccessKey.reveal(),
        ...(cfg.sessionToken === undefined ? {} : { sessionToken: cfg.sessionToken.reveal() }),
      };
    } else if (cfg.credentials !== undefined) {
      credentials = cfg.credentials;
    }
    if (cfg.roleArn !== undefined) {
      credentials = fromTemporaryCredentials({
        params: {
          RoleArn: cfg.roleArn,
          RoleSessionName: cfg.roleSessionName,
          ...(cfg.externalId === undefined ? {} : { ExternalId: cfg.externalId }),
        },
        ...(credentials === undefined ? {} : { masterCredentials: credentials }),
        clientConfig: {
          ...(cfg.region === undefined ? {} : { region: cfg.region }),
          requestHandler: handler,
        },
      });
    }
    if (credentials !== undefined) base.credentials = credentials;
    this.#sdk = new AwsCloudWatchLogsClient({ ...base, maxAttempts: cfg.retry.maxRetries + 1 });
    this.#probe = new AwsCloudWatchLogsClient({ ...base, maxAttempts: 1 });
    this.#ownsSdk = true;
  }

  /** Builds a client from `GROW_AWS_CLOUDWATCH_LOGS_*` and the standard AWS environment variables. */
  static fromEnv(overrides: CloudWatchLogsConfigOptions = {}): CloudWatchLogsClient {
    return new CloudWatchLogsClient(CloudWatchLogsConfig.fromEnv(overrides));
  }

  /** The underlying AWS SDK client (for operations this package does not wrap). */
  get sdk(): AwsCloudWatchLogsClient {
    return this.#sdk;
  }

  // ---------------------------------------------------------------- plumbing

  /** Runs one SDK call with the total timeout, cancellation and error mapping. */
  async #call<T>(
    operation: string,
    fn: (sdk: AwsCloudWatchLogsClient, signal: AbortSignal) => Promise<T>,
    signal: AbortSignal | undefined,
    options: { probe?: boolean; timeoutMs?: number } = {},
  ): Promise<T> {
    if (this.#closed) throw new CancelledError(`${operation}: the client is closed`);
    throwIfAborted(signal, operation);
    const controller = new AbortController();
    let reason: "cancel" | "timeout" | undefined;
    const onAbort = (): void => {
      reason ??= "cancel";
      controller.abort();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    const timeoutMs = options.timeoutMs ?? this.config.totalTimeoutMs;
    const timer = setTimeout(() => {
      reason ??= "timeout";
      controller.abort();
    }, timeoutMs);
    timer.unref();
    const aborted = new Promise<never>((_, reject) => {
      controller.signal.addEventListener(
        "abort",
        () => {
          reject(new Error("aborted"));
        },
        { once: true },
      );
    });
    aborted.catch(() => undefined);
    try {
      const sdk = options.probe === true ? this.#probe : this.#sdk;
      return await Promise.race([fn(sdk, controller.signal), aborted]);
    } catch (err) {
      if (reason === "cancel") throw new CancelledError(`${operation}: cancelled`);
      if (reason === "timeout") {
        throw new TimeoutError(`${operation}: timed out after ${timeoutMs} ms`);
      }
      throw mapAwsError(operation, err, this.config.knownSecrets());
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    }
  }

  #group(group: string | undefined, operation: string): string {
    const name = group ?? this.config.logGroup;
    if (name === undefined) {
      throw new ConfigError(`${operation}: no log group given and config.logGroup is not set`);
    }
    return name;
  }

  // ---------------------------------------------------------------- core

  /**
   * Liveness via a signed `DescribeLogGroups {limit: 1}` without retries. Never throws for an
   * unhealthy or unreachable service (only `CANCELLED` and `CONFIG`). The service answering with an
   * authentication or permission error still counts as healthy (`details.auth = "failed"`);
   * throttling reports `degraded`.
   */
  async health(options: CallOptions = {}): Promise<HealthStatus> {
    try {
      await this.#call(
        "health",
        (sdk, s) =>
          sdk.send(
            new DescribeLogGroupsCommand({
              limit: 1,
              ...(this.config.logGroup === undefined
                ? {}
                : { logGroupNamePrefix: this.config.logGroup }),
            }),
            { abortSignal: s },
          ),
        options.signal,
        { probe: true, timeoutMs: Math.min(this.config.requestTimeoutMs, 10_000) },
      );
      return healthStatus("ok", {}, true);
    } catch (err) {
      const e = err as CloudWatchLogsError;
      switch (e.code) {
        case ErrorCode.Cancelled:
        case ErrorCode.Config:
          throw e;
        case ErrorCode.Authentication:
        case ErrorCode.PermissionDenied:
          return healthStatus("ok", { auth: "failed", code: e.code }, true);
        case ErrorCode.RateLimited:
          return healthStatus("degraded", { code: e.code }, true);
        default:
          if (e.httpStatus !== undefined) {
            return healthStatus("unhealthy", { httpStatus: e.httpStatus, error: e.code }, false);
          }
          return healthStatus("unreachable", { error: e.code }, false);
      }
    }
  }

  /**
   * Authenticated round trip (`DescribeLogGroups {limit: 1}`). Returns the latency in ms;
   * `AuthenticationError` on bad credentials, `PermissionDeniedError` without
   * `logs:DescribeLogGroups`.
   */
  async ping(options: CallOptions = {}): Promise<number> {
    const start = performance.now();
    await this.#call(
      "ping",
      (sdk, s) => sdk.send(new DescribeLogGroupsCommand({ limit: 1 }), { abortSignal: s }),
      options.signal,
    );
    return performance.now() - start;
  }

  /** Always `UnsupportedError`: CloudWatch Logs exposes no server version (see `API_VERSION`). */
  serverVersion(_options: CallOptions = {}): Promise<string> {
    return Promise.reject(
      new UnsupportedError(
        "serverVersion: Amazon CloudWatch Logs does not expose a server version",
      ),
    );
  }

  /**
   * Flushes and closes every log client created by {@link logClient}, stops live-tail sessions and
   * releases connections. Idempotent; never destroys a caller-supplied SDK client.
   */
  close(): Promise<void> {
    this.#closing ??= this.#shutdown();
    return this.#closing;
  }

  async #shutdown(): Promise<void> {
    await Promise.allSettled([...this.#logClients].map((c) => c.close()));
    await Promise.allSettled([...this.#tails].map((t) => t.close()));
    this.#closed = true;
    if (this.#ownsSdk) {
      this.#sdk.destroy();
      this.#probe.destroy();
    }
  }

  // ---------------------------------------------------------------- bootstrap

  /** Finds a log group by exact name (`DescribeLogGroups` with the name as prefix). */
  async getLogGroup(name: string, options: CallOptions = {}): Promise<LogGroupInfo | undefined> {
    checkGroupName(name, "getLogGroup");
    let token: string | undefined;
    do {
      const out: DescribeLogGroupsCommandOutput = await this.#call(
        "getLogGroup",
        (sdk, s) =>
          sdk.send(
            new DescribeLogGroupsCommand({
              logGroupNamePrefix: name,
              limit: 50,
              ...(token === undefined ? {} : { nextToken: token }),
            }),
            { abortSignal: s },
          ),
        options.signal,
      );
      const hit = (out.logGroups ?? []).find((g) => g.logGroupName === name);
      if (hit !== undefined) return toGroup(hit);
      token = out.nextToken ?? undefined;
    } while (token !== undefined);
    return undefined;
  }

  async #requireGroup(
    name: string,
    operation: string,
    signal?: AbortSignal,
  ): Promise<LogGroupInfo> {
    const group = await this.getLogGroup(name, signal === undefined ? {} : { signal });
    if (group === undefined) throw new NotFoundError(`${operation}: log group not found`);
    return group;
  }

  /**
   * Creates the log group when it is missing (`CreateLogGroup`), then applies retention and tags.
   * For an existing group the requested tags are added (never removed) and the retention is
   * re-applied; a different KMS key raises `ConflictError`.
   */
  async ensureLogGroup(
    name: string,
    options: EnsureLogGroupOptions = {},
  ): Promise<EnsureLogGroupResult> {
    const op = "ensureLogGroup";
    checkGroupName(name, op);
    checkRetention(options.retentionDays, op);
    const tags = options.tags ?? {};
    let created = true;
    try {
      await this.#call(
        op,
        (sdk, s) =>
          sdk.send(
            new CreateLogGroupCommand({
              logGroupName: name,
              ...(options.kmsKeyId === undefined ? {} : { kmsKeyId: options.kmsKeyId }),
              ...(Object.keys(tags).length === 0 ? {} : { tags }),
            }),
            { abortSignal: s },
          ),
        options.signal,
      );
    } catch (err) {
      if (awsErrorCode((err as Error).cause) !== "ResourceAlreadyExistsException") throw err;
      created = false;
    }
    const group = await this.#requireGroup(name, op, options.signal);
    if (!created) {
      const want = options.kmsKeyId;
      if (
        want !== undefined &&
        group.kmsKeyId !== want &&
        !(group.kmsKeyId ?? "").endsWith(`/${want}`)
      ) {
        throw new ConflictError(`${op}: log group exists with a different KMS key`);
      }
      if (Object.keys(tags).length > 0) await this.tagLogGroup(name, tags, options);
    }
    if (options.retentionDays !== undefined) {
      await this.putRetentionPolicy(name, options.retentionDays, options);
    }
    return { created, arn: group.arn };
  }

  /**
   * Creates the log stream when it is missing. A missing group raises `NotFoundError` unless
   * `createGroup` is set.
   */
  async ensureLogStream(
    group: string,
    stream: string,
    options: EnsureLogStreamOptions = {},
  ): Promise<{ created: boolean }> {
    const op = "ensureLogStream";
    checkGroupName(group, op);
    checkStreamName(stream, op);
    if (options.createGroup === true) {
      await this.ensureLogGroup(
        group,
        options.signal === undefined ? {} : { signal: options.signal },
      );
    }
    try {
      await this.#call(
        op,
        (sdk, s) =>
          sdk.send(new CreateLogStreamCommand({ logGroupName: group, logStreamName: stream }), {
            abortSignal: s,
          }),
        options.signal,
      );
      return { created: true };
    } catch (err) {
      if (awsErrorCode((err as Error).cause) === "ResourceAlreadyExistsException") {
        return { created: false };
      }
      throw err;
    }
  }

  /** Sets the retention (`PutRetentionPolicy`); `null` or `0` removes it (never expire). */
  async putRetentionPolicy(
    group: string,
    days: number | null,
    options: CallOptions = {},
  ): Promise<void> {
    const op = "putRetentionPolicy";
    checkGroupName(group, op);
    checkRetention(days, op);
    if (days === null || days === 0) {
      await this.#call(
        op,
        (sdk, s) =>
          sdk.send(new DeleteRetentionPolicyCommand({ logGroupName: group }), { abortSignal: s }),
        options.signal,
      );
      return;
    }
    await this.#call(
      op,
      (sdk, s) =>
        sdk.send(new PutRetentionPolicyCommand({ logGroupName: group, retentionInDays: days }), {
          abortSignal: s,
        }),
      options.signal,
    );
  }

  /** Deletes a log group with its streams and events. Returns false when missing and `ignoreMissing`. */
  async deleteLogGroup(name: string, options: DeleteOptions = {}): Promise<boolean> {
    const op = "deleteLogGroup";
    checkGroupName(name, op);
    try {
      await this.#call(
        op,
        (sdk, s) => sdk.send(new DeleteLogGroupCommand({ logGroupName: name }), { abortSignal: s }),
        options.signal,
      );
      return true;
    } catch (err) {
      if (options.ignoreMissing === true && err instanceof NotFoundError) return false;
      throw err;
    }
  }

  /** Deletes a log stream. Returns false when missing and `ignoreMissing`. */
  async deleteLogStream(
    group: string,
    stream: string,
    options: DeleteOptions = {},
  ): Promise<boolean> {
    const op = "deleteLogStream";
    checkGroupName(group, op);
    checkStreamName(stream, op);
    try {
      await this.#call(
        op,
        (sdk, s) =>
          sdk.send(new DeleteLogStreamCommand({ logGroupName: group, logStreamName: stream }), {
            abortSignal: s,
          }),
        options.signal,
      );
      return true;
    } catch (err) {
      if (options.ignoreMissing === true && err instanceof NotFoundError) return false;
      throw err;
    }
  }

  /** Iterates log groups (sorted by name) across pages. */
  async *describeLogGroups(options: DescribeLogGroupsOptions = {}): AsyncGenerator<LogGroupInfo> {
    const op = "describeLogGroups";
    checkRange(options.pageSize, 1, 50, "pageSize", op);
    checkRange(options.limit, 1, Number.MAX_SAFE_INTEGER, "limit", op);
    let token: string | undefined;
    let count = 0;
    do {
      const out: DescribeLogGroupsCommandOutput = await this.#call(
        op,
        (sdk, s) =>
          sdk.send(
            new DescribeLogGroupsCommand({
              limit: options.pageSize ?? 50,
              ...(options.prefix === undefined ? {} : { logGroupNamePrefix: options.prefix }),
              ...(token === undefined ? {} : { nextToken: token }),
            }),
            { abortSignal: s },
          ),
        options.signal,
      );
      for (const g of out.logGroups ?? []) {
        yield toGroup(g);
        if (options.limit !== undefined && ++count >= options.limit) return;
      }
      token = out.nextToken ?? undefined;
    } while (token !== undefined);
  }

  /** Iterates the log streams of a group across pages. */
  async *describeLogStreams(
    group: string,
    options: DescribeLogStreamsOptions = {},
  ): AsyncGenerator<LogStreamInfo> {
    const op = "describeLogStreams";
    checkGroupName(group, op);
    checkRange(options.pageSize, 1, 50, "pageSize", op);
    checkRange(options.limit, 1, Number.MAX_SAFE_INTEGER, "limit", op);
    if (options.orderBy === "LastEventTime" && options.prefix !== undefined) {
      throw new ValidationError(`${op}: prefix cannot be combined with orderBy LastEventTime`);
    }
    let token: string | undefined;
    let count = 0;
    do {
      const out = await this.#call(
        op,
        (sdk, s) =>
          sdk.send(
            new DescribeLogStreamsCommand({
              logGroupName: group,
              limit: options.pageSize ?? 50,
              ...(options.prefix === undefined ? {} : { logStreamNamePrefix: options.prefix }),
              ...(options.orderBy === undefined ? {} : { orderBy: options.orderBy }),
              ...(options.descending === undefined ? {} : { descending: options.descending }),
              ...(token === undefined ? {} : { nextToken: token }),
            }),
            { abortSignal: s },
          ),
        options.signal,
      );
      for (const st of out.logStreams ?? []) {
        const info: LogStreamInfo = { name: st.logStreamName ?? "" };
        if (st.arn !== undefined) info.arn = st.arn;
        const set = (key: keyof LogStreamInfo, ms: number | undefined): void => {
          const d = optDate(ms);
          if (d !== undefined) (info as unknown as Record<string, unknown>)[key] = d;
        };
        set("creationTime", st.creationTime);
        set("firstEventTime", st.firstEventTimestamp);
        set("lastEventTime", st.lastEventTimestamp);
        set("lastIngestionTime", st.lastIngestionTime);
        yield info;
        if (options.limit !== undefined && ++count >= options.limit) return;
      }
      token = out.nextToken ?? undefined;
    } while (token !== undefined);
  }

  /** Adds or overwrites tags of a log group (`TagResource`). */
  async tagLogGroup(
    name: string,
    tags: Record<string, string>,
    options: CallOptions = {},
  ): Promise<void> {
    const op = "tagLogGroup";
    const arn = (await this.#requireGroup(name, op, options.signal)).arn;
    await this.#call(
      op,
      (sdk, s) => sdk.send(new TagResourceCommand({ resourceArn: arn, tags }), { abortSignal: s }),
      options.signal,
    );
  }

  /** Removes tags from a log group (`UntagResource`). */
  async untagLogGroup(
    name: string,
    keys: readonly string[],
    options: CallOptions = {},
  ): Promise<void> {
    const op = "untagLogGroup";
    const arn = (await this.#requireGroup(name, op, options.signal)).arn;
    await this.#call(
      op,
      (sdk, s) =>
        sdk.send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [...keys] }), {
          abortSignal: s,
        }),
      options.signal,
    );
  }

  /** Tags of a log group (`ListTagsForResource`). */
  async listLogGroupTags(name: string, options: CallOptions = {}): Promise<Record<string, string>> {
    const op = "listLogGroupTags";
    const arn = (await this.#requireGroup(name, op, options.signal)).arn;
    const out = await this.#call(
      op,
      (sdk, s) =>
        sdk.send(new ListTagsForResourceCommand({ resourceArn: arn }), { abortSignal: s }),
      options.signal,
    );
    return { ...(out.tags ?? {}) };
  }

  // ---------------------------------------------------------------- ingest

  /**
   * Sends events with `PutLogEvents`. Events are sorted, oversized events are truncated / dropped /
   * rejected per `oversizePolicy`, and the events are split into batches that respect every service
   * limit (10 000 events, 1 MiB incl. 26 bytes per event, 24 h span). A missing stream is created
   * when `autoCreate` is on. Events the service rejects (too old/new, expired) are reported, never
   * retried. Throttling and 5xx are retried by the AWS SDK (a lost response may duplicate events).
   */
  async putLogEvents(
    group: string,
    stream: string,
    events: readonly InputLogEvent[],
    options: PutLogEventsOptions = {},
  ): Promise<PutLogEventsResult> {
    const op = "putLogEvents";
    checkGroupName(group, op);
    checkStreamName(stream, op);
    const plan = planLogEventBatches(events, {
      maxEventBytes: options.maxEventBytes ?? this.config.maxEventBytes,
      oversizePolicy: options.oversizePolicy ?? this.config.oversizePolicy,
    });
    const result: PutLogEventsResult = {
      accepted: 0,
      rejected: 0,
      truncated: plan.truncated,
      dropped: plan.dropped,
      batches: 0,
      rejectedInfo: [],
    };
    const autoCreate = options.autoCreate ?? this.config.autoCreate;
    for (const [index, batch] of plan.batches.entries()) {
      const send = (): Promise<{
        rejectedLogEventsInfo?: {
          tooNewLogEventStartIndex?: number;
          tooOldLogEventEndIndex?: number;
          expiredLogEventEndIndex?: number;
        };
      }> =>
        this.#call(
          op,
          (sdk, s) =>
            sdk.send(
              new PutLogEventsCommand({
                logGroupName: group,
                logStreamName: stream,
                logEvents: batch,
              }),
              { abortSignal: s },
            ),
          options.signal,
        );
      let out;
      try {
        out = await send();
      } catch (err) {
        if (!(autoCreate && err instanceof NotFoundError)) throw err;
        try {
          await this.ensureLogStream(group, stream, {
            ...(options.createGroup === true ? { createGroup: true } : {}),
            ...(options.signal === undefined ? {} : { signal: options.signal }),
          });
        } catch (createErr) {
          if (createErr instanceof NotFoundError) throw err;
          throw createErr;
        }
        out = await send();
      }
      result.batches++;
      const info = out.rejectedLogEventsInfo;
      let rejected = 0;
      if (info !== undefined) {
        const r: RejectedLogEventsInfo = { batch: index };
        let oldEnd = 0;
        let newStart = batch.length;
        if (typeof info.tooOldLogEventEndIndex === "number") {
          r.tooOldLogEventEndIndex = info.tooOldLogEventEndIndex;
          oldEnd = Math.max(oldEnd, info.tooOldLogEventEndIndex);
        }
        if (typeof info.expiredLogEventEndIndex === "number") {
          r.expiredLogEventEndIndex = info.expiredLogEventEndIndex;
          oldEnd = Math.max(oldEnd, info.expiredLogEventEndIndex);
        }
        if (typeof info.tooNewLogEventStartIndex === "number") {
          r.tooNewLogEventStartIndex = info.tooNewLogEventStartIndex;
          newStart = Math.min(newStart, info.tooNewLogEventStartIndex);
        }
        // AWS returns the "end index" fields as exclusive (events 0..end-1 were rejected) and
        // tooNewLogEventStartIndex as inclusive; Moto 5.2.4 returns end indexes one lower.
        const old = Math.min(batch.length, oldEnd);
        const tooNew = Math.max(0, batch.length - Math.max(newStart, old));
        rejected = old + tooNew;
        result.rejectedInfo.push(r);
      }
      result.rejected += rejected;
      result.accepted += batch.length - rejected;
    }
    return result;
  }

  /**
   * Builds an Embedded Metric Format document ({@link buildEmf}) and sends it as one log event to
   * `group`/`stream` (defaults: `config.logGroup` / `config.logStream`). Returns the document sent.
   */
  async emf(
    metric: string,
    value: number | readonly number[],
    dimensions: Readonly<Record<string, string>> = {},
    options: EmfSendOptions = {},
  ): Promise<Record<string, unknown>> {
    const group = this.#group(options.group, "emf");
    const doc = buildEmf(metric, value, dimensions, options);
    const result = await this.putLogEvents(
      group,
      options.stream ?? this.config.logStream,
      [{ timestamp: doc._aws.Timestamp, message: JSON.stringify(doc) }],
      {
        oversizePolicy: "error",
        ...(options.signal === undefined ? {} : { signal: options.signal }),
      },
    );
    if (result.rejected > 0) throw new ValidationError("emf: the service rejected the event");
    return doc;
  }

  // ---------------------------------------------------------------- query

  /**
   * One `FilterLogEvents` request. `nextToken` is `undefined` at the end; AWS may return empty
   * pages that still carry a token. Events are ordered by timestamp, then event id.
   */
  async filterLogEventsPage(
    options: FilterLogEventsOptions & { nextToken?: string } = {},
  ): Promise<{ events: FilteredLogEvent[]; nextToken?: string }> {
    const op = "filterLogEvents";
    const group = this.#group(options.group, op);
    checkRange(options.pageSize, 1, 10_000, "pageSize", op);
    if (options.streams !== undefined && options.streamPrefix !== undefined) {
      throw new ValidationError(`${op}: streams and streamPrefix are mutually exclusive`);
    }
    if (
      options.streams !== undefined &&
      (options.streams.length < 1 || options.streams.length > 100)
    ) {
      throw new ValidationError(`${op}: streams must hold 1-100 names`);
    }
    const start = options.start === undefined ? undefined : toMs(options.start, "start", op);
    const end = options.end === undefined ? undefined : toMs(options.end, "end", op);
    const byArn = group.startsWith("arn:");
    const out = await this.#call(
      op,
      (sdk, s) =>
        sdk.send(
          new FilterLogEventsCommand({
            ...(byArn ? { logGroupIdentifier: stripStar(group) } : { logGroupName: group }),
            ...(options.streams === undefined ? {} : { logStreamNames: [...options.streams] }),
            ...(options.streamPrefix === undefined
              ? {}
              : { logStreamNamePrefix: options.streamPrefix }),
            ...(options.pattern === undefined || options.pattern === ""
              ? {}
              : { filterPattern: options.pattern }),
            ...(start === undefined ? {} : { startTime: start }),
            ...(end === undefined ? {} : { endTime: end }),
            ...(options.pageSize === undefined ? {} : { limit: options.pageSize }),
            ...(options.nextToken === undefined ? {} : { nextToken: options.nextToken }),
          }),
          { abortSignal: s },
        ),
      options.signal,
    );
    const events = (out.events ?? [])
      .map((ev) => {
        const item: FilteredLogEvent = {
          timestamp: new Date(ev.timestamp ?? 0),
          message: ev.message ?? "",
        };
        const ingestion = optDate(ev.ingestionTime);
        if (ingestion !== undefined) item.ingestionTime = ingestion;
        if (ev.logStreamName !== undefined) item.logStreamName = ev.logStreamName;
        if (ev.eventId !== undefined) item.eventId = ev.eventId;
        return item;
      })
      .sort(
        (a, b) =>
          a.timestamp.getTime() - b.timestamp.getTime() ||
          (a.eventId ?? "").localeCompare(b.eventId ?? ""),
      );
    const nextToken = out.nextToken ?? undefined;
    return nextToken === undefined ? { events } : { events, nextToken };
  }

  /**
   * Iterates events matching a filter pattern (`FilterLogEvents`) across pages until no token is
   * left (or `limit`). Within a page, events are ordered by timestamp, then event id.
   */
  async *filterLogEvents(options: FilterLogEventsOptions = {}): AsyncGenerator<FilteredLogEvent> {
    checkRange(options.limit, 1, Number.MAX_SAFE_INTEGER, "limit", "filterLogEvents");
    let token: string | undefined;
    let count = 0;
    do {
      const remaining = options.limit === undefined ? undefined : options.limit - count;
      const pageSize =
        remaining === undefined
          ? options.pageSize
          : Math.min(remaining, options.pageSize ?? 10_000);
      const page = await this.filterLogEventsPage({
        ...options,
        ...(pageSize === undefined ? {} : { pageSize }),
        ...(token === undefined ? {} : { nextToken: token }),
      });
      for (const item of page.events) {
        yield item;
        if (options.limit !== undefined && ++count >= options.limit) return;
      }
      token = page.nextToken;
    } while (token !== undefined);
  }

  /**
   * Iterates the events of one stream (`GetLogEvents`). `startFromHead: true` reads oldest → newest
   * (forward tokens); the default reads the newest page first and then older pages (backward
   * tokens; events inside a page stay in ascending order). Iteration stops when the
   * service returns the token that was sent.
   */
  async *getLogEvents(
    group: string,
    stream: string,
    options: GetLogEventsOptions = {},
  ): AsyncGenerator<StreamLogEvent> {
    const op = "getLogEvents";
    checkGroupName(group, op);
    checkStreamName(stream, op);
    checkRange(options.pageSize, 1, 10_000, "pageSize", op);
    checkRange(options.limit, 1, Number.MAX_SAFE_INTEGER, "limit", op);
    const forward = options.startFromHead === true;
    const start = options.start === undefined ? undefined : toMs(options.start, "start", op);
    const end = options.end === undefined ? undefined : toMs(options.end, "end", op);
    let token: string | undefined;
    let count = 0;
    for (;;) {
      const sent = token;
      const out = await this.#call(
        op,
        (sdk, s) =>
          sdk.send(
            new GetLogEventsCommand({
              logGroupName: group,
              logStreamName: stream,
              startFromHead: forward,
              ...(start === undefined ? {} : { startTime: start }),
              ...(end === undefined ? {} : { endTime: end }),
              ...(options.pageSize === undefined ? {} : { limit: options.pageSize }),
              ...(sent === undefined ? {} : { nextToken: sent }),
            }),
            { abortSignal: s },
          ),
        options.signal,
      );
      const page = [...(out.events ?? [])];
      for (const ev of page) {
        const item: StreamLogEvent = {
          timestamp: new Date(ev.timestamp ?? 0),
          message: ev.message ?? "",
        };
        const ingestion = optDate(ev.ingestionTime);
        if (ingestion !== undefined) item.ingestionTime = ingestion;
        yield item;
        if (options.limit !== undefined && ++count >= options.limit) return;
      }
      const next = (forward ? out.nextForwardToken : out.nextBackwardToken) ?? undefined;
      if (next === undefined || next === sent || page.length === 0) return;
      token = next;
    }
  }

  #queryInput(queryString: string, options: StartQueryOptions, op: string): StartQueryCommandInput {
    if (typeof queryString !== "string" || queryString.trim() === "") {
      throw new ValidationError(`${op}: queryString must not be empty`);
    }
    const groups = options.groups ?? [this.#group(undefined, op)];
    if (groups.length < 1 || groups.length > 50) {
      throw new ValidationError(`${op}: groups must hold 1-50 log groups`);
    }
    checkRange(options.limit, 1, 10_000, "limit", op);
    const endMs = options.end === undefined ? Date.now() : toMs(options.end, "end", op);
    const startMs =
      options.start === undefined ? endMs - 3_600_000 : toMs(options.start, "start", op);
    if (startMs > endMs) throw new ValidationError(`${op}: start must not be after end`);
    const byArn = groups.some((g) => g.startsWith("arn:"));
    return {
      ...(byArn ? { logGroupIdentifiers: groups.map(stripStar) } : { logGroupNames: [...groups] }),
      queryString,
      // StartQuery takes epoch SECONDS.
      startTime: Math.floor(startMs / 1000),
      endTime: Math.ceil(endMs / 1000),
      ...(options.limit === undefined ? {} : { limit: options.limit }),
    };
  }

  /** Starts a Logs Insights query (`StartQuery`) and returns its id. */
  async startQuery(queryString: string, options: StartQueryOptions = {}): Promise<string> {
    const input = this.#queryInput(queryString, options, "startQuery");
    const out = await this.#call(
      "startQuery",
      (sdk, s) => sdk.send(new StartQueryCommand(input), { abortSignal: s }),
      options.signal,
    );
    if (out.queryId === undefined) throw new UnavailableError("startQuery: no query id returned");
    return out.queryId;
  }

  /** Reads the status and rows of a Logs Insights query (`GetQueryResults`). */
  async getQueryResults(queryId: string, options: CallOptions = {}): Promise<QueryResults> {
    if (typeof queryId !== "string" || queryId === "") {
      throw new ValidationError("getQueryResults: queryId must not be empty");
    }
    const out = await this.#call(
      "getQueryResults",
      (sdk, s) => sdk.send(new GetQueryResultsCommand({ queryId }), { abortSignal: s }),
      options.signal,
    );
    const rows = (out.results ?? []).map((row) => {
      const r: Record<string, string> = {};
      for (const f of row) if (f.field !== undefined) r[f.field] = f.value ?? "";
      return r;
    });
    const result: QueryResults = {
      queryId,
      status: out.status ?? "Unknown",
      rows,
    };
    const st = out.statistics;
    if (st !== undefined) {
      const stats: QueryStatistics = {
        recordsMatched: st.recordsMatched ?? 0,
        recordsScanned: st.recordsScanned ?? 0,
        bytesScanned: st.bytesScanned ?? 0,
      };
      result.statistics = stats;
    }
    return result;
  }

  /** Stops a running query (`StopQuery`). Returns false when it had already finished. */
  async stopQuery(queryId: string, options: CallOptions = {}): Promise<boolean> {
    if (typeof queryId !== "string" || queryId === "") {
      throw new ValidationError("stopQuery: queryId must not be empty");
    }
    const out = await this.#call(
      "stopQuery",
      (sdk, s) => sdk.send(new StopQueryCommand({ queryId }), { abortSignal: s }),
      options.signal,
    );
    return out.success === true;
  }

  /**
   * Runs a Logs Insights query to completion: `StartQuery`, then polls `GetQueryResults` until a
   * terminal status. `Failed`/`Unknown` → `UnavailableError`, `Timeout` → `TimeoutError`,
   * `Cancelled` → `CancelledError` (message includes the query id). When `timeoutMs` passes or
   * the caller cancels, the query is stopped best-effort.
   */
  async query(queryString: string, options: QueryOptions = {}): Promise<QueryResults> {
    const op = "query";
    const input = this.#queryInput(queryString, options, op);
    const timeoutMs = options.timeoutMs ?? 60_000;
    const deadline = Date.now() + timeoutMs;
    const attempts = Math.max(1, options.retryOnQueryLimit ?? 3);
    let queryId: string | undefined;
    for (let attempt = 1; queryId === undefined; attempt++) {
      try {
        const out = await this.#call(
          op,
          (sdk, s) => sdk.send(new StartQueryCommand(input), { abortSignal: s }),
          options.signal,
        );
        if (out.queryId === undefined) throw new UnavailableError(`${op}: no query id returned`);
        queryId = out.queryId;
      } catch (err) {
        const limit = awsErrorCode((err as Error).cause) === "LimitExceededException";
        if (!limit || attempt >= attempts || Date.now() >= deadline) throw err;
        await this.#sleep(Math.min(1000 * 2 ** (attempt - 1), 5000), options.signal);
      }
    }
    let interval = options.pollIntervalMs ?? 1000;
    const signalOpt = options.signal === undefined ? {} : { signal: options.signal };
    try {
      for (;;) {
        const res = await this.getQueryResults(queryId, signalOpt);
        if (QUERY_TERMINAL.has(res.status)) {
          switch (res.status) {
            case "Complete":
              return res;
            case "Timeout":
              throw new TimeoutError(`${op}: query ${queryId} timed out in the service`);
            case "Cancelled":
              throw new CancelledError(`${op}: query ${queryId} was cancelled`);
            default:
              throw new UnavailableError(
                `${op}: query ${queryId} ended with status ${res.status}`,
                {
                  retryable: false,
                },
              );
          }
        }
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          await this.#stopQuietly(queryId);
          throw new TimeoutError(`${op}: query ${queryId} not complete within ${timeoutMs} ms`);
        }
        await this.#sleep(Math.min(interval, remaining), options.signal);
        interval = Math.min(interval * 2, 5000);
      }
    } catch (err) {
      if (err instanceof CancelledError && options.signal?.aborted === true) {
        await this.#stopQuietly(queryId);
      }
      throw err;
    }
  }

  async #stopQuietly(queryId: string): Promise<void> {
    try {
      await this.stopQuery(queryId);
    } catch {
      // best effort
    }
  }

  /** Lists recent Logs Insights queries (`DescribeQueries`). */
  async describeQueries(options: DescribeQueriesOptions = {}): Promise<QueryInfo[]> {
    checkRange(options.maxResults, 1, 1000, "maxResults", "describeQueries");
    const out = await this.#call(
      "describeQueries",
      (sdk, s) =>
        sdk.send(
          new DescribeQueriesCommand({
            ...(options.group === undefined ? {} : { logGroupName: options.group }),
            ...(options.status === undefined ? {} : { status: options.status }),
            ...(options.maxResults === undefined ? {} : { maxResults: options.maxResults }),
          }),
          { abortSignal: s },
        ),
      options.signal,
    );
    return (out.queries ?? []).map((q) => {
      const info: QueryInfo = {
        queryId: q.queryId ?? "",
        queryString: q.queryString ?? "",
        status: q.status ?? "Unknown",
      };
      const created = optDate(q.createTime === undefined ? undefined : Math.trunc(q.createTime));
      if (created !== undefined) info.createTime = created;
      if (q.logGroupName !== undefined) info.logGroupName = q.logGroupName;
      return info;
    });
  }

  // ---------------------------------------------------------------- live tail

  /**
   * ARN of a log group (without `:*`): built from `config.accountId` and the region when the
   * account is configured, otherwise looked up with `DescribeLogGroups`.
   */
  async logGroupArn(name: string, options: CallOptions = {}): Promise<string> {
    if (name.startsWith("arn:")) return stripStar(name);
    checkGroupName(name, "logGroupArn");
    if (this.config.accountId !== undefined) {
      const region = this.config.region ?? (await this.#sdk.config.region());
      return `arn:${partitionForRegion(region)}:logs:${region}:${this.config.accountId}:log-group:${name}`;
    }
    return (await this.#requireGroup(name, "logGroupArn", options.signal)).arn;
  }

  /**
   * Starts a Live Tail session (`StartLiveTail`) and calls `onEvent` for every event. Resolves once
   * the session has started (`sessionStart`), with a handle to stop it. On AWS the session streams
   * new events for up to 3 hours; the Moto emulator replays matching existing events once and ends.
   */
  async startLiveTail(
    request: LiveTailRequest,
    onEvent: (event: LiveTailEvent) => void,
    options: LiveTailOptions = {},
  ): Promise<LiveTailHandle> {
    const op = "startLiveTail";
    const groups = request.groups;
    if (groups.length < 1 || groups.length > 10) {
      throw new ValidationError(`${op}: groups must hold 1-10 log groups`);
    }
    if (request.streams !== undefined && request.streamPrefixes !== undefined) {
      throw new ValidationError(`${op}: streams and streamPrefixes are mutually exclusive`);
    }
    if ((request.streams ?? request.streamPrefixes) !== undefined && groups.length !== 1) {
      throw new ValidationError(`${op}: streams and streamPrefixes need exactly one log group`);
    }
    const signalOpt = options.signal === undefined ? {} : { signal: options.signal };
    const arns: string[] = [];
    for (const g of groups) arns.push(await this.logGroupArn(g, signalOpt));
    const input: StartLiveTailCommandInput = {
      logGroupIdentifiers: arns,
      ...(request.streams === undefined ? {} : { logStreamNames: [...request.streams] }),
      ...(request.streamPrefixes === undefined
        ? {}
        : { logStreamNamePrefixes: [...request.streamPrefixes] }),
      ...(request.pattern === undefined || request.pattern === ""
        ? {}
        : { logEventFilterPattern: request.pattern }),
    };
    const session = new AbortController();
    const onCallerAbort = (): void => {
      session.abort();
    };
    options.signal?.addEventListener("abort", onCallerAbort, { once: true });
    let iterator: AsyncIterator<StartLiveTailResponseStream>;
    try {
      const out = await this.#call(
        op,
        (sdk, s) => {
          s.addEventListener(
            "abort",
            () => {
              session.abort();
            },
            { once: true },
          );
          return sdk.send(new StartLiveTailCommand(input), { abortSignal: session.signal });
        },
        options.signal,
      );
      if (out.responseStream === undefined) {
        throw new UnavailableError(`${op}: the service returned no event stream`);
      }
      iterator = out.responseStream[Symbol.asyncIterator]();
    } catch (err) {
      options.signal?.removeEventListener("abort", onCallerAbort);
      throw err;
    }
    const knownSecrets = this.config.knownSecrets();
    const mapFrameError = (frame: StartLiveTailResponseStream): CloudWatchLogsError | undefined => {
      if (frame.SessionStreamingException !== undefined) {
        return mapAwsError(
          op,
          Object.assign(new Error("stream"), { name: "SessionStreamingException" }),
        );
      }
      if (frame.SessionTimeoutException !== undefined) {
        return mapAwsError(
          op,
          Object.assign(new Error("timeout"), { name: "SessionTimeoutException" }),
        );
      }
      return undefined;
    };
    const deliver = (frame: StartLiveTailResponseStream): void => {
      const update = frame.sessionUpdate;
      if (update === undefined) return;
      const results = update.sessionResults ?? [];
      try {
        options.onUpdate?.({
          sampled: update.sessionMetadata?.sampled === true,
          count: results.length,
        });
      } catch {
        // callbacks must never break the session
      }
      for (const r of results) {
        const ev: LiveTailEvent = {
          timestamp: new Date(r.timestamp ?? 0),
          message: r.message ?? "",
        };
        const ingestion = optDate(r.ingestionTime);
        if (ingestion !== undefined) ev.ingestionTime = ingestion;
        if (r.logStreamName !== undefined) ev.logStreamName = r.logStreamName;
        if (r.logGroupIdentifier !== undefined) ev.logGroupIdentifier = r.logGroupIdentifier;
        try {
          onEvent(ev);
        } catch {
          // callbacks must never break the session
        }
      }
    };
    // Read frames until the session starts.
    let sessionId = "";
    let ended = false;
    try {
      for (;;) {
        const next = await iterator.next();
        if (next.done === true) {
          ended = true;
          break;
        }
        const frameError = mapFrameError(next.value);
        if (frameError !== undefined) throw frameError;
        if (next.value.sessionStart !== undefined) {
          sessionId = next.value.sessionStart.sessionId ?? "";
          break;
        }
        deliver(next.value);
      }
    } catch (err) {
      options.signal?.removeEventListener("abort", onCallerAbort);
      session.abort();
      if (options.signal?.aborted === true) throw new CancelledError(`${op}: cancelled`);
      throw mapAwsError(op, err, knownSecrets);
    }
    let closed = false;
    let failure: Error | undefined;
    const tails = this.#tails;
    const pump = async (): Promise<void> => {
      try {
        while (!ended) {
          const next = await iterator.next();
          if (next.done === true) break;
          const frameError = mapFrameError(next.value);
          if (frameError !== undefined) throw frameError;
          deliver(next.value);
        }
      } catch (err) {
        if (!session.signal.aborted) {
          const mapped = mapAwsError(op, err, knownSecrets);
          failure = mapped;
          try {
            options.onError?.(mapped);
          } catch {
            // ignored
          }
        }
      } finally {
        closed = true;
        session.abort();
        options.signal?.removeEventListener("abort", onCallerAbort);
        tails.delete(handle);
      }
    };
    const handle: LiveTailHandle = {
      sessionId,
      get done() {
        return done;
      },
      get error() {
        return failure;
      },
      get closed() {
        return closed;
      },
      async close() {
        session.abort();
        await done;
      },
    };
    tails.add(handle);
    const done = pump();
    return handle;
  }

  // ---------------------------------------------------------------- logging category

  /**
   * A {@link BatchingLogClient} that exports to `group`/`stream` (defaults `config.logGroup` /
   * `config.logStream`; the stream is created on first use). Records are sent as normalized
   * LogRecord JSON. `close()` of this client flushes it too.
   */
  logClient(options: CloudWatchLogClientOptions = {}): BatchingLogClient {
    const exporter = this.#exporter(options, "logClient");
    const { group: _g, stream: _s, createGroup: _c, ...batchOptions } = options;
    const client = new BatchingLogClient(exporter, batchOptions);
    this.#logClients.add(client);
    return client;
  }

  /**
   * A {@link CloudWatchLogsExporter} (the `LogExporter` behind {@link logClient}) writing to
   * `group`/`stream` (defaults `config.logGroup` / `config.logStream`). `CONFIG` when no group is
   * set, `VALIDATION` for invalid names.
   */
  exporter(
    options: { group?: string; stream?: string; createGroup?: boolean } = {},
  ): CloudWatchLogsExporter {
    return this.#exporter(options, "exporter");
  }

  #exporter(
    options: { group?: string; stream?: string; createGroup?: boolean },
    op: string,
  ): CloudWatchLogsExporter {
    const group = this.#group(options.group, op);
    checkGroupName(group, op);
    const stream = options.stream ?? this.config.logStream;
    checkStreamName(stream, op);
    return new CloudWatchLogsExporter(this, {
      group,
      stream,
      createGroup: options.createGroup === true,
    });
  }

  /** A {@link CloudWatchLogsQueryClient} over `group` (default `config.logGroup`). */
  logQueryClient(options: { group?: string } = {}): CloudWatchLogsQueryClient {
    const group = this.#group(options.group, "logQueryClient");
    return new CloudWatchLogsQueryClient(this, group);
  }
}
