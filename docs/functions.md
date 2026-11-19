# Functions

The Amazon CloudWatch Logs API of this SDK. Every function exists in all five languages with the
same behaviour, named per language convention (`putLogEvents` in TypeScript and Java,
`put_log_events` in Python and Rust, `PutLogEvents` in Go with `ctx` first). Each page shows the
functions of one area with an example in every language.

| Area | Functions | Description |
| - | - | - |
| [Client](functions/client-setup.md) | `new` / `fromEnv` / `builder` / `withSdkClient`, `health`, `ping`, `serverVersion`, `close` | builds or wraps the AWS SDK client (region, credentials, endpoint, assume role); signed `DescribeLogGroups(limit 1)` probe (`health` never throws, auth failures count as reachable); `serverVersion` is always `UNSUPPORTED`; `close` is idempotent, flushes log clients, stops live-tail sessions; later calls raise `CANCELLED` |
| [Bootstrap](functions/bootstrap.md) | `ensureLogGroup`, `ensureLogStream`, `putRetentionPolicy`, `deleteLogGroup`, `deleteLogStream`, `describeLogGroups`, `describeLogStreams`, `getLogGroup`, `tagLogGroup`, `untagLogGroup`, `listLogGroupTags`, `logGroupArn` | idempotent create (existing group: tags added, never removed; retention re-applied when given, `0` = never expire; different KMS key → `CONFLICT`); retention `0`/none removes the policy; lazy pagination iterators; delete with `ignoreMissing` |
| [Ingest](functions/ingest.md) | `putLogEvents`, `emf`, `buildEmf`, `truncateMessage` (Go/Rust) | client-side batching that respects the service limits (10,000 events and 1,048,576 bytes incl. 26 bytes per event per call, 24 h span, sorted by timestamp), 256 KiB per-event policy `truncate` / `drop` / `error`, missing stream auto-created, throttling retried; Embedded Metric Format documents (`emf` returns the document sent, `VALIDATION` when rejected) |
| [Query](functions/query.md) | `filterLogEvents`, `filterLogEventsPage`, `getLogEvents` | pattern search over a group (streams or a stream prefix, time window) as a lazy iterator or one page; reading one stream forward (`startFromHead`) or backward, ending when the token repeats |
| [Logs Insights](functions/insights.md) | `startQuery`, `getQueryResults`, `query`, `stopQuery`, `describeQueries` | Insights queries; `query` polls until a terminal status, retries `LimitExceeded`, stops the query on timeout or cancellation |
| [Live tail](functions/live-tail.md) | `startLiveTail` | streaming `StartLiveTail` session (up to 3 h on AWS) with `onEvent` / `onUpdate` / `onError` callbacks and a handle to stop it; names are turned into ARNs |
| [Logging](functions/logging.md) | `logClient`, `exporter`, `logQueryClient` → `search`, `tail`, `listStreams`, `listLabels`, `waitForLog` | the shared Grow Codes `LogClient` (batching, logger facade, redaction, framework adapters) writing normalized `LogRecord` JSON; `search` uses `FilterLogEvents` (`asc`) or walks the time window backwards (`desc`, default); `tail` is live tail; `listLabels` is `UNSUPPORTED` |

All calls go through the official AWS SDK of the language (SigV4, endpoint resolution, standard
retry mode). Errors are mapped by **AWS error code** (HTTP status only as a fallback), see
[Error handling](errors.md) and the code table in [Compatibility](compatibility.md#error-mapping).

## Service configuration

These settings come on top of the [common settings](configuration.md). Precedence: explicit value >
`GROW_AWS_CLOUDWATCH_LOGS_*` > standard AWS / CloudWatch variable > AWS SDK default chain / default.

| Setting | Environment variable | Default |
| - | - | - |
| `url` — endpoint override (emulator, VPC endpoint) | `GROW_AWS_CLOUDWATCH_LOGS_URL`, `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`, `AWS_ENDPOINT_URL` | none: the AWS SDK resolves `https://logs.<region>.amazonaws.com` |
| `region` | `GROW_AWS_CLOUDWATCH_LOGS_REGION`, `AWS_REGION`, `AWS_DEFAULT_REGION` | AWS SDK chain (profile region); none → `CONFIG` |
| `accessKeyId` | `GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID`, `AWS_ACCESS_KEY_ID` | AWS default credential chain |
| `secretAccessKey` | `GROW_AWS_CLOUDWATCH_LOGS_SECRET_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY` (same layer as the key id) | AWS default credential chain |
| `sessionToken` | `GROW_AWS_CLOUDWATCH_LOGS_SESSION_TOKEN`, `AWS_SESSION_TOKEN` (same layer as the key id) | none |
| `profile` | `GROW_AWS_CLOUDWATCH_LOGS_PROFILE`, `AWS_PROFILE` | AWS SDK default |
| `credentials` — an AWS SDK credentials provider | — | none |
| `roleArn` — assume a role with STS (standard STS endpoint, `AWS_ENDPOINT_URL_STS` honoured) | `GROW_AWS_CLOUDWATCH_LOGS_ROLE_ARN` | none |
| `roleSessionName` | `GROW_AWS_CLOUDWATCH_LOGS_ROLE_SESSION_NAME` | `grow-sdk` |
| `externalId` | `GROW_AWS_CLOUDWATCH_LOGS_EXTERNAL_ID` | none |
| `logGroup` — default group of `emf`, `filterLogEvents`, `logClient`, `logQueryClient`, `startLiveTail` | `GROW_AWS_CLOUDWATCH_LOGS_LOG_GROUP`, `CLOUDWATCH_LOG_GROUP` | none |
| `logStream` — default stream of `emf` and `logClient` | `GROW_AWS_CLOUDWATCH_LOGS_LOG_STREAM`, `CLOUDWATCH_LOG_STREAM` | `<hostname>-<pid>-<random6>` |
| `accountId` — builds live-tail log group ARNs without a lookup | `GROW_AWS_CLOUDWATCH_LOGS_ACCOUNT_ID` | none: the ARN is read with `DescribeLogGroups` |
| `maxEventBytes` — per-event message limit in UTF-8 bytes | `GROW_AWS_CLOUDWATCH_LOGS_MAX_EVENT_BYTES` | 262,144 (256 KiB; at most 1,048,550) |
| `oversizePolicy` — `truncate`, `drop` or `error` | `GROW_AWS_CLOUDWATCH_LOGS_OVERSIZE_POLICY` | `truncate` |
| `autoCreate` — create a missing stream in `putLogEvents` | `GROW_AWS_CLOUDWATCH_LOGS_AUTO_CREATE` | `true` |
| `sdkClient` — use an existing AWS SDK client | — | none (the SDK builds one) |

Static credentials are read **as one set**: when `GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID` is set, the
secret key and session token come from `GROW_AWS_CLOUDWATCH_LOGS_*` only, never from `AWS_*`.
`sdkClient` replaces the AWS SDK client the package would build (Grow TLS, proxy, hooks and
User-Agent settings do not apply to it; `close()` leaves it usable).

## Integration coverage

Every function is called against `motoserver/moto:5.2.4` in the Docker suite with a round-trip
assertion. Paths Moto cannot serve (for example `stopQuery`, Insights `filter`/`stats`, the
`PutLogEvents` service limits, live-tail streaming semantics) are covered by unit tests against
recorded AWS responses and listed in [Compatibility](compatibility.md).
