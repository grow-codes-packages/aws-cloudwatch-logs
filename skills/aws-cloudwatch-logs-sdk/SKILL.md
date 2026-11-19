---
name: aws-cloudwatch-logs-sdk
description: Add and use the Grow Codes SDK for Amazon CloudWatch Logs (`aws-cloudwatch-logs`) in TypeScript, Rust, Go, Python or Java projects. Use when code must talk to Amazon CloudWatch Logs - creating log groups and streams, retention and tags, writing log events (putLogEvents batching, EMF metrics), filterLogEvents/getLogEvents, Logs Insights queries, live tail, shipping application logs through the Grow LogClient or framework adapters - installing @grow-codes/aws-cloudwatch-logs, the grow-aws-cloudwatch-logs crate or PyPI package, the Go module github.com/grow-codes-packages/aws-cloudwatch-logs/go or Maven codes.grow.packages:aws-cloudwatch-logs, configuring it with GROW_AWS_CLOUDWATCH_LOGS_* variables, handling CloudWatchLogsError codes, testing with the bundled fakes, or running the Docker integration suite of this repository.
---

# Amazon CloudWatch Logs SDK (Grow Codes)

One API for Amazon CloudWatch Logs in five languages (categories: logging). Pick the package for the
project's language, configure it from the environment, and handle errors by their canonical code.

## 1. Add the package

| Language | Command | Import |
| - | - | - |
| TypeScript | `npm install @grow-codes/aws-cloudwatch-logs` | `import { CloudWatchLogsClient } from "@grow-codes/aws-cloudwatch-logs"` |
| Rust | `cargo add grow-aws-cloudwatch-logs` (add `tokio` with `macros,rt-multi-thread`; sync API: feature `blocking`) | `use grow_aws_cloudwatch_logs::CloudWatchLogsClient;` |
| Go | `go get github.com/grow-codes-packages/aws-cloudwatch-logs/go` | `cloudwatchlogs "github.com/grow-codes-packages/aws-cloudwatch-logs/go"` |
| Python | `pip install grow-aws-cloudwatch-logs` (async client: `grow-aws-cloudwatch-logs[async]`; or `uv add …`) | `from grow_aws_cloudwatch_logs import CloudWatchLogsClient` |
| Java | Maven `codes.grow.packages:aws-cloudwatch-logs` (Gradle `implementation("codes.grow.packages:aws-cloudwatch-logs:<version>")`) | `import codes.grow.packages.cloudwatchlogs.CloudWatchLogsClient;` |

Check the latest version in the README badges before pinning one.

## 2. Configure

Prefer environment configuration so no credential is written into code:

```sh
export AWS_REGION=eu-central-1
export AWS_PROFILE=dev                 # or the normal AWS credential chain (SSO, IAM role, keys)
export CLOUDWATCH_LOG_GROUP=my-app/api # default group for emf, logClient, logQueryClient, live tail
# export GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000   # emulator (Moto) only
```

Precedence: explicit config value > `GROW_AWS_CLOUDWATCH_LOGS_*` > standard AWS / CloudWatch
variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`…, `AWS_PROFILE`, `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`,
`CLOUDWATCH_LOG_GROUP`, `CLOUDWATCH_LOG_STREAM`) > AWS SDK default chain / default. Service
variables: `…_REGION`, `…_ACCESS_KEY_ID` / `…_SECRET_ACCESS_KEY` / `…_SESSION_TOKEN` (one set, never
mixed with `AWS_*`), `…_PROFILE`, `…_ROLE_ARN`, `…_ROLE_SESSION_NAME`, `…_EXTERNAL_ID`,
`…_LOG_GROUP`, `…_LOG_STREAM`, `…_ACCOUNT_ID`, `…_MAX_EVENT_BYTES` (default 262144),
`…_OVERSIZE_POLICY` (`truncate`|`drop`|`error`), `…_AUTO_CREATE`. Common: `…_CONNECT_TIMEOUT`,
`…_REQUEST_TIMEOUT`, `…_TOTAL_TIMEOUT` (`10s`, `1500`), `…_MAX_RETRIES`, `…_CA_CERT`,
`…_CLIENT_CERT`, `…_CLIENT_KEY`, `…_PROXY`. Environment proxies (`HTTPS_PROXY`) are ignored unless
`proxy` is set. The client never needs STS for its own calls; `roleArn` assumes a role through the
standard STS endpoint.

## 3. Use

Create one client per process and close it on shutdown (TypeScript `await client.close()`, Rust
`client.close().await`, Go `defer client.Close()`, Python `with …`/`close()`, Java
try-with-resources).

- `health()` never throws for an unhealthy service; check `healthy`.
- `ping()` verifies credentials (`AUTHENTICATION` error when they are wrong).
- Calls go through the official AWS SDK (standard retry mode, `maxAttempts = maxRetries + 1`):
  throttling, transient 5xx and network errors are retried with full jitter.
- `serverVersion()` always raises `UNSUPPORTED`; calls on a closed client raise `CANCELLED`.
- Errors carry `code` (`CONFIG`, `AUTHENTICATION`, `PERMISSION_DENIED`, `NOT_FOUND`, `CONFLICT`,
  `RATE_LIMITED`, `VALIDATION`, `UNAVAILABLE`, `TIMEOUT`, `UNSUPPORTED`, `CANCELLED`), `retryable`,
  `httpStatus` and `requestId`. Branch on the code, not on the message text. Go:
  `errors.Is(err, cloudwatchlogs.ErrNotFound)`.

### Logging

Create the log client once, send records with `send`/`sendBatch` or use the logger facade
(`logger("component").info("message", attrs)`) or the framework adapter for the language (pino or
winston, `tracing`/`log`, `slog`/zap, `logging`/structlog, Logback/Log4j2/JUL). Records are batched
and redacted; call `close()` on shutdown so the queue is flushed.

<!-- service:skill-usage — the service adds its own functions and examples here. -->

### CloudWatch Logs functions

| Need | Function (camelCase; snake_case in Python/Rust, PascalCase + `ctx` in Go) |
| - | - |
| Make sure a group / stream exists (safe at every start) | `ensureLogGroup(name, {retentionDays, kmsKeyId, tags})`, `ensureLogStream(group, stream)` |
| Retention | `putRetentionPolicy(group, days)` — `0` / none removes it; `ensureLogGroup` `retentionDays` unset = unchanged, `0` = never expire (Go: `cloudwatchlogs.Days(n)`) |
| List / inspect / remove | `describeLogGroups`, `describeLogStreams` (lazy iterators), `getLogGroup`, `deleteLogGroup`, `deleteLogStream` (`ignoreMissing`) |
| Tags, ARN | `tagLogGroup`, `untagLogGroup`, `listLogGroupTags`, `logGroupArn` (name or ARN) |
| Write events | `putLogEvents(group, stream, events)` — sorts, splits at 10,000 events / 1 MiB / 24 h, truncates events > 256 KiB, creates a missing stream; check `accepted` / `rejected` |
| Metrics from logs | `emf(metric, value, dimensions, {unit, namespace})` — returns the EMF document, `VALIDATION` if rejected; `buildEmf` for the document only |
| Search | `filterLogEvents({group, streams, pattern, start, end})`, `filterLogEventsPage`, `getLogEvents(group, stream, {startFromHead})` |
| Insights | `query(q, {groups, start, end, timeout})` (polls), `startQuery`, `getQueryResults`, `stopQuery`, `describeQueries` |
| Live tail | `startLiveTail({groups, pattern}, onEvent, {onUpdate, onError})` → handle `close()` |
| App logs | `logClient({group, stream, resource})` → `logger(name).info(msg, attrs)`, `flush`, `close`; `exporter`; `logQueryClient(group)` → `search` (newest first), `tail`, `listStreams`, `waitForLog` |

Pitfalls:

- `rejectedLogEventsInfo` end indexes are exclusive on AWS (Moto returns them one lower); events
  older than 14 days / the retention or more than 2 h in the future are rejected, never retried.
- A lost `PutLogEvents` response can duplicate events (the SDK retries throttling and 5xx).
- Live tail sessions last up to 3 h on AWS; close the handle (or the client) when done.
- IAM: the principal needs the `logs:` actions you call (`logs:PutLogEvents`,
  `logs:CreateLogStream`, `logs:DescribeLogGroups`, `logs:FilterLogEvents`, `logs:StartLiveTail`,
  `logs:StartQuery`, …).

## 4. Test code that uses the SDK

Use the in-memory fakes from the testing module instead of a real Amazon CloudWatch Logs in unit tests:
TypeScript `@grow-codes/aws-cloudwatch-logs/testing`, Rust feature `testing`, Go package `testkit`, Python
`grow_aws_cloudwatch_logs.testing`, Java `codes.grow.packages.cloudwatchlogs.testing`.

## 5. Work on this repository

Read `AGENTS.md` first. Short version: the five languages keep the same API; files with the
`Generated by sdk-kit` header are changed in sdk-kit, not here; `make lint coverage` must pass
(≥ 90 % lines and branches); `make test-docker LANGS="<lang>"` runs the integration suite against
`motoserver/moto:5.2.4` (the AWS emulator; Moto quirks and unit-test-only paths are listed in
`docs/compatibility.md`).
