# Grow Codes SDK for Amazon CloudWatch Logs

[![CI](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml/badge.svg)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/grow-codes-packages/aws-cloudwatch-logs?sort=semver)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/releases)
[![Docs](https://img.shields.io/badge/docs-grow--codes--packages.github.io-blue)](https://grow-codes-packages.github.io/aws-cloudwatch-logs/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Grow Codes](https://img.shields.io/badge/Grow%20Codes-grow.codes-2ea44f)](https://grow.codes)

| Language | Package | Coverage |
| - | - | - |
| TypeScript | [![npm](https://img.shields.io/npm/v/@grow-codes/aws-cloudwatch-logs)](https://www.npmjs.com/package/@grow-codes/aws-cloudwatch-logs) | [![TypeScript coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-typescript.json)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml) |
| Rust | [![crates.io](https://img.shields.io/crates/v/grow-aws-cloudwatch-logs)](https://crates.io/crates/grow-aws-cloudwatch-logs) [![docs.rs](https://img.shields.io/docsrs/grow-aws-cloudwatch-logs)](https://docs.rs/grow-aws-cloudwatch-logs) | [![Rust coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-rust.json)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml) |
| Go | [![Go Reference](https://pkg.go.dev/badge/github.com/grow-codes-packages/aws-cloudwatch-logs/go.svg)](https://pkg.go.dev/github.com/grow-codes-packages/aws-cloudwatch-logs/go) | [![Go coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-go.json)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml) |
| Python | [![PyPI](https://img.shields.io/pypi/v/grow-aws-cloudwatch-logs)](https://pypi.org/project/grow-aws-cloudwatch-logs/) | [![Python coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-python.json)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml) |
| Java | [![Maven Central](https://img.shields.io/maven-central/v/codes.grow.packages/aws-cloudwatch-logs)](https://central.sonatype.com/artifact/codes.grow.packages/aws-cloudwatch-logs) | [![Java coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-java.json)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml) |

Open-source client libraries for [Amazon CloudWatch Logs](#trademarks-and-disclaimer) in **TypeScript, Rust, Go,
Python and Java** — one API, five idiomatic packages on top of the official AWS SDKs, tested against
the Moto AWS emulator in Docker.

- **Typed client** with timeouts, retries with jitter, `Retry-After`, cancellation and one error
  model (`CloudWatchLogsError` with canonical codes) in every language.
- **Secrets stay secret**: credentials are redacted in `toString`, logs, hooks and error messages.
- **Service functions**: log groups and streams (idempotent `ensure…`), retention, tags,
  `putLogEvents` batching within the service limits, Embedded Metric Format, `filterLogEvents`,
  `getLogEvents`, Logs Insights queries and live tail.
- **Logging**: batched log shipping with back-pressure, redaction, a logger facade and adapters for
  the common logging frameworks of each language, plus log search and live tail through the shared
  Grow Codes `LogClient` / `LogQueryClient` interfaces.

## Contents

- [Supported service version](#supported-service-version)
- [Install](#install)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Function overview](#function-overview)
- [Testing with Docker](#testing-with-docker)
- [Compatibility](#compatibility)
- [Contributing](#contributing)
- [Security](#security)
- [Licence](#licence)
- [Trademarks and disclaimer](#trademarks-and-disclaimer)
- [About Grow Codes](#about-grow-codes)

## Supported service version

Amazon CloudWatch Logs is a managed AWS service with one public API (`Logs_20140328`); the packages
use the official AWS SDKs and follow the current API. The Docker integration suite runs against
**`motoserver/moto:5.2.4`** (the AWS emulator pinned in the Grow Codes stack catalog). Behaviour that
Moto cannot reproduce (Logs Insights query language, `StopQuery`, service limits, live-tail
streaming) is covered by unit tests against recorded AWS responses; see
[Compatibility](#compatibility) and [docs/compatibility.md](docs/compatibility.md).

## Install

| Language | Package | Install |
| - | - | - |
| TypeScript (Node.js ≥ 22.12) | `@grow-codes/aws-cloudwatch-logs` | `npm install @grow-codes/aws-cloudwatch-logs` |
| Rust (MSRV 1.88) | `grow-aws-cloudwatch-logs` | `cargo add grow-aws-cloudwatch-logs` (sync API: `--features blocking`) |
| Go (1.26+) | `github.com/grow-codes-packages/aws-cloudwatch-logs/go` | `go get github.com/grow-codes-packages/aws-cloudwatch-logs/go` |
| Python (≥ 3.10) | `grow-aws-cloudwatch-logs` | `pip install grow-aws-cloudwatch-logs` (async client: `pip install "grow-aws-cloudwatch-logs[async]"`) |
| Java (17+) | `codes.grow.packages:aws-cloudwatch-logs` | see below |

Each package is built on the official AWS SDK of its language (AWS SDK for JavaScript v3,
aws-sdk-rust, AWS SDK for Go v2, boto3 / aioboto3, AWS SDK for Java v2), so the standard AWS
credential chain, profiles, SigV4 and endpoint resolution work as usual.

Maven:

```xml
<dependency>
  <groupId>codes.grow.packages</groupId>
  <artifactId>aws-cloudwatch-logs</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle (Kotlin DSL):

```kotlin
implementation("codes.grow.packages:aws-cloudwatch-logs:0.1.0")
```

## Quickstart

Every package reads its configuration from the environment (`AWS_REGION`, the AWS credential chain,
`CLOUDWATCH_LOG_GROUP`, `GROW_AWS_CLOUDWATCH_LOGS_*`, see [Configuration](#configuration)) or from an
explicit config object. The examples create a log group, write two events and read them back.

### TypeScript

```ts
import { CloudWatchLogsClient } from "@grow-codes/aws-cloudwatch-logs";

const client = CloudWatchLogsClient.fromEnv();
try {
  await client.ensureLogGroup("my-app/api", { retentionDays: 7, tags: { team: "platform" } });
  const res = await client.putLogEvents("my-app/api", "web-1", [
    { timestamp: Date.now(), message: "service started" },
    { timestamp: Date.now(), message: "ERROR something failed" },
  ]);
  console.log(res.accepted, res.rejected);
  for await (const e of client.filterLogEvents({ group: "my-app/api", pattern: "ERROR" })) {
    console.log(e.timestamp, e.message);
  }
} finally {
  await client.close();
}
```

### Rust

```rust
use grow_aws_cloudwatch_logs::{
    CloudWatchLogsClient, EnsureLogGroupOptions, FilterLogEventsOptions, LogEvent, PutLogEventsOptions,
};

#[tokio::main]
async fn main() -> Result<(), grow_aws_cloudwatch_logs::Error> {
    let client = CloudWatchLogsClient::from_env()?;
    client
        .ensure_log_group("my-app/api", EnsureLogGroupOptions::default().retention_days(7).tag("team", "platform"))
        .await?;
    let res = client
        .put_log_events(
            "my-app/api",
            "web-1",
            vec![LogEvent::new("service started"), LogEvent::new("ERROR something failed")],
            PutLogEventsOptions::default(),
        )
        .await?;
    println!("{} {}", res.accepted, res.rejected);
    let errors = client
        .filter_log_events(FilterLogEventsOptions {
            group: Some("my-app/api".into()),
            pattern: Some("ERROR".into()),
            ..Default::default()
        })
        .try_collect()
        .await?;
    println!("{errors:?}");
    client.close().await;
    Ok(())
}
```

### Go

```go
package main

import (
	"context"
	"fmt"
	"log"

	cloudwatchlogs "github.com/grow-codes-packages/aws-cloudwatch-logs/go"
)

func main() {
	ctx := context.Background()
	client, err := cloudwatchlogs.NewClientFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if _, err := client.EnsureLogGroup(ctx, "my-app/api", cloudwatchlogs.EnsureLogGroupOptions{
		RetentionDays: cloudwatchlogs.Days(7), Tags: map[string]string{"team": "platform"},
	}); err != nil {
		log.Fatal(err)
	}
	res, err := client.PutLogEvents(ctx, "my-app/api", "web-1", []cloudwatchlogs.LogEvent{
		{Message: "service started"}, {Message: "ERROR something failed"},
	}, cloudwatchlogs.PutLogEventsOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.Accepted, res.Rejected)
	for e, err := range client.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: "my-app/api", Pattern: "ERROR"}) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(e.Timestamp, e.Message)
	}
}
```

### Python

```python
import grow_aws_cloudwatch_logs as sdk

with sdk.CloudWatchLogsClient.from_env() as client:
    client.ensure_log_group("my-app/api", retention_days=7, tags={"team": "platform"})
    res = client.put_log_events(
        "my-app/api",
        "web-1",
        [sdk.InputLogEvent("service started"), sdk.InputLogEvent("ERROR something failed")],
    )
    print(res.accepted, res.rejected)
    for e in client.filter_log_events(group="my-app/api", pattern="ERROR"):
        print(e.timestamp, e.message)
```

Async code uses `AsyncCloudWatchLogsClient` (extra `[async]`) with the same methods (`async with`,
`await client.put_log_events(...)`, `async for e in client.filter_log_events(...)`).

### Java

```java
import codes.grow.packages.cloudwatchlogs.*;
import java.util.List;
import java.util.Map;

try (CloudWatchLogsClient client = CloudWatchLogsClient.fromEnv()) {
    client.ensureLogGroup("my-app/api",
        EnsureLogGroupOptions.builder().retentionDays(7).tags(Map.of("team", "platform")).build());
    PutLogEventsResult res = client.putLogEvents("my-app/api", "web-1",
        List.of(LogEvent.of("service started"), LogEvent.of("ERROR something failed")));
    System.out.println(res.accepted() + " " + res.rejected());
    for (FilteredLogEvent e : client.filterLogEvents(
            FilterLogEventsOptions.builder().group("my-app/api").pattern("ERROR").build())) {
        System.out.println(e.timestamp() + " " + e.message());
    }
}
```

Every method also has a `…Async` variant returning a `CompletableFuture`.

More complete programs live in [examples/](examples/) — one folder per language.

## Configuration

All settings can be passed in code or through environment variables. Precedence: **explicit value >
`GROW_AWS_CLOUDWATCH_LOGS_*` variable > standard AWS / CloudWatch variable > AWS SDK default chain /
default.** A present but invalid value raises a `ConfigError` that names the variable (never its
value).

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
| `connectTimeout` | `GROW_AWS_CLOUDWATCH_LOGS_CONNECT_TIMEOUT` | 10 s |
| `requestTimeout` (per attempt; not applied to live-tail streams) | `GROW_AWS_CLOUDWATCH_LOGS_REQUEST_TIMEOUT` | 30 s |
| `totalTimeout` (including retries) | `GROW_AWS_CLOUDWATCH_LOGS_TOTAL_TIMEOUT` | 120 s |
| `retry.maxRetries` (AWS SDK `maxAttempts = maxRetries + 1`, standard mode) | `GROW_AWS_CLOUDWATCH_LOGS_MAX_RETRIES` | 3 |
| `tls.caCertPath` | `GROW_AWS_CLOUDWATCH_LOGS_CA_CERT` | system roots |
| `tls.clientCertPath` / `tls.clientKeyPath` | `GROW_AWS_CLOUDWATCH_LOGS_CLIENT_CERT` / `GROW_AWS_CLOUDWATCH_LOGS_CLIENT_KEY` | none |
| `tls.insecureSkipVerify` (tests only) | `GROW_AWS_CLOUDWATCH_LOGS_INSECURE_SKIP_VERIFY` | `false` |
| `proxy` | `GROW_AWS_CLOUDWATCH_LOGS_PROXY` | none (environment proxies are ignored) |
| `headers`, `userAgentSuffix`, `hooks`, `logger` | — | none |

Static credentials are read **as one set**: when `GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID` is set,
the secret key and session token come from `GROW_AWS_CLOUDWATCH_LOGS_*` only, never from `AWS_*`.
Durations accept milliseconds (`1500`) or units (`500ms`, `10s`, `1m30s`); booleans accept
`true/false/1/0/yes/no/on/off`. Field names follow each language's convention (`request_timeout` in
Python and Rust, `RequestTimeout` in Go; Go turns `autoCreate` into `DisableAutoCreate`). The full
reference is in the [documentation](https://grow-codes-packages.github.io/aws-cloudwatch-logs/configuration/).

## Function overview

<!-- service:function-overview — the service fills this section; keep the heading. -->

The same functions exist in every language, named per language convention (`putLogEvents` in
TypeScript and Java, `put_log_events` in Python and Rust, `PutLogEvents` in Go with `ctx` first).
Group arguments accept a log group **name or ARN** where AWS allows it (tags, `logGroupArn`, live
tail); the SDK sends `logGroupName` everywhere else. Each area links to its documentation page with
full examples in all five languages.

| Area | Functions | Description |
| - | - | - |
| [Client](docs/functions/client-setup.md) | `new` / `fromEnv` / `builder` / `withSdkClient`, `health`, `ping`, `serverVersion`, `close` | builds or wraps the AWS SDK client (region, credentials, endpoint, assume role); signed `DescribeLogGroups(limit 1)` probe (`health` never throws, auth failures count as reachable); `serverVersion` is always `UNSUPPORTED`; `close` is idempotent, flushes log clients, stops live-tail sessions; later calls raise `CANCELLED` |
| [Bootstrap](docs/functions/bootstrap.md) | `ensureLogGroup`, `ensureLogStream`, `putRetentionPolicy`, `deleteLogGroup`, `deleteLogStream`, `describeLogGroups`, `describeLogStreams`, `getLogGroup`, `tagLogGroup`, `untagLogGroup`, `listLogGroupTags`, `logGroupArn` | idempotent create (existing group: tags added, never removed; retention re-applied when given, `0` = never expire; different KMS key → `CONFLICT`); retention `0`/none removes the policy; lazy pagination iterators; delete with `ignoreMissing` |
| [Ingest](docs/functions/ingest.md) | `putLogEvents`, `emf`, `buildEmf`, `truncateMessage` (Go/Rust) | client-side batching that respects the service limits (10,000 events and 1,048,576 bytes incl. 26 bytes per event per call, 24 h span, sorted by timestamp), 256 KiB per-event policy `truncate` / `drop` / `error`, missing stream auto-created, throttling retried; Embedded Metric Format documents (`emf` returns the document sent, `VALIDATION` when rejected) |
| [Query](docs/functions/query.md) | `filterLogEvents`, `filterLogEventsPage`, `getLogEvents` | pattern search over a group (streams or a stream prefix, time window) as a lazy iterator or one page; reading one stream forward (`startFromHead`) or backward, ending when the token repeats |
| [Logs Insights](docs/functions/insights.md) | `startQuery`, `getQueryResults`, `query`, `stopQuery`, `describeQueries` | Insights queries; `query` polls until a terminal status, retries `LimitExceeded`, stops the query on timeout or cancellation |
| [Live tail](docs/functions/live-tail.md) | `startLiveTail` | streaming `StartLiveTail` session (up to 3 h on AWS) with `onEvent` / `onUpdate` / `onError` callbacks and a handle to stop it; names are turned into ARNs |
| [Logging](docs/functions/logging.md) | `logClient`, `exporter`, `logQueryClient` → `search`, `tail`, `listStreams`, `listLabels`, `waitForLog` | the shared Grow Codes `LogClient` (batching, logger facade, redaction, framework adapters) writing normalized `LogRecord` JSON; `search` uses `FilterLogEvents` (`asc`) or walks the time window backwards (`desc`, default); `tail` is live tail; `listLabels` is `UNSUPPORTED` |

### Bootstrap

`ensureLogGroup`, `ensureLogStream`, `putRetentionPolicy`, `deleteLogGroup`, `deleteLogStream`,
`describeLogGroups`, `describeLogStreams`, `getLogGroup`, `tagLogGroup`, `untagLogGroup`,
`listLogGroupTags`, `logGroupArn` — [documentation](docs/functions/bootstrap.md)

<details><summary>Example in all five languages</summary>

**TypeScript**

```ts
const { created, arn } = await client.ensureLogGroup("my-app/api", { retentionDays: 30, tags: { team: "platform" } });
await client.ensureLogStream("my-app/api", "web-1");
await client.putRetentionPolicy("my-app/api", 0); // 0 or null: never expire
for await (const g of client.describeLogGroups({ prefix: "my-app/" })) console.log(g.name, g.retentionDays);
for await (const s of client.describeLogStreams("my-app/api")) console.log(s.name);
await client.tagLogGroup("my-app/api", { env: "prod" });
console.log(await client.listLogGroupTags("my-app/api"));
await client.deleteLogStream("my-app/api", "web-1", { ignoreMissing: true });
```

**Rust**

```rust
let r = client.ensure_log_group("my-app/api", EnsureLogGroupOptions::default().retention_days(30).tag("team", "platform")).await?;
client.ensure_log_stream("my-app/api", "web-1", EnsureLogStreamOptions::default()).await?;
client.put_retention_policy("my-app/api", None).await?; // None or Some(0): never expire
let groups = client.describe_log_groups(DescribeLogGroupsOptions { prefix: Some("my-app/".into()), ..Default::default() }).try_collect().await?;
let streams = client.describe_log_streams("my-app/api", DescribeLogStreamsOptions::default()).try_collect().await?;
client.tag_log_group("my-app/api", [("env".to_owned(), "prod".to_owned())].into()).await?;
client.delete_log_stream("my-app/api", "web-1", DeleteOptions { ignore_missing: true }).await?;
```

**Go**

```go
r, err := client.EnsureLogGroup(ctx, "my-app/api", cloudwatchlogs.EnsureLogGroupOptions{RetentionDays: cloudwatchlogs.Days(30), Tags: map[string]string{"team": "platform"}})
_, err = client.EnsureLogStream(ctx, "my-app/api", "web-1", cloudwatchlogs.EnsureLogStreamOptions{})
err = client.PutRetentionPolicy(ctx, "my-app/api", 0) // 0: never expire
for g, err := range client.DescribeLogGroups(ctx, cloudwatchlogs.DescribeLogGroupsOptions{Prefix: "my-app/"}) {
	if err != nil {
		return err
	}
	fmt.Println(g.Name, g.RetentionDays)
}
err = client.TagLogGroup(ctx, "my-app/api", map[string]string{"env": "prod"})
_, err = client.DeleteLogStream(ctx, "my-app/api", "web-1", cloudwatchlogs.DeleteOptions{IgnoreMissing: true})
```

**Python**

```python
r = client.ensure_log_group("my-app/api", retention_days=30, tags={"team": "platform"})
client.ensure_log_stream("my-app/api", "web-1")
client.put_retention_policy("my-app/api", None)  # None or 0: never expire
names = [g.name for g in client.describe_log_groups(prefix="my-app/")]
streams = [s.name for s in client.describe_log_streams("my-app/api")]
client.tag_log_group("my-app/api", {"env": "prod"})
client.delete_log_stream("my-app/api", "web-1", ignore_missing=True)
```

**Java**

```java
EnsureLogGroupResult r = client.ensureLogGroup("my-app/api",
    EnsureLogGroupOptions.builder().retentionDays(30).tags(Map.of("team", "platform")).build());
client.ensureLogStream("my-app/api", "web-1");
client.putRetentionPolicy("my-app/api", 0); // 0: never expire
for (LogGroupInfo g : client.describeLogGroups(DescribeLogGroupsOptions.builder().prefix("my-app/").build())) {
    System.out.println(g.name());
}
client.tagLogGroup("my-app/api", Map.of("env", "prod"));
client.deleteLogStream("my-app/api", "web-1", true);
```

</details>

### Ingest

`putLogEvents`, `emf`, `buildEmf` — [documentation](docs/functions/ingest.md)

<details><summary>Example in all five languages</summary>

**TypeScript**

```ts
const res = await client.putLogEvents("my-app/api", "web-1", events, { oversizePolicy: "drop" });
console.log(res.accepted, res.rejected, res.truncated, res.dropped, res.batches, res.rejectedInfo);
const doc = await client.emf("latency", 42, { service: "api" }, { unit: "Milliseconds", namespace: "MyApp" });
```

**Rust**

```rust
let res = client.put_log_events("my-app/api", "web-1", events, PutLogEventsOptions::default()).await?;
let dims: BTreeMap<String, String> = [("service".to_owned(), "api".to_owned())].into();
let doc: String = client.emf("latency", 42.0, &dims, EmfOptions::default().unit("Milliseconds").namespace("MyApp")).await?;
```

**Go**

```go
res, err := client.PutLogEvents(ctx, "my-app/api", "web-1", events, cloudwatchlogs.PutLogEventsOptions{OversizePolicy: cloudwatchlogs.OversizeDrop})
doc, err := client.EMF(ctx, "latency", 42, map[string]string{"service": "api"}, cloudwatchlogs.EMFOptions{Unit: "Milliseconds", Namespace: "MyApp"})
```

**Python**

```python
res = client.put_log_events("my-app/api", "web-1", events, oversize_policy="drop")
doc = client.emf("latency", 42, {"service": "api"}, unit="Milliseconds", namespace="MyApp")
```

**Java**

```java
PutLogEventsResult res = client.putLogEvents("my-app/api", "web-1", events,
    PutLogEventsOptions.builder().oversizePolicy(OversizePolicy.DROP).build());
Map<String, Object> doc = client.emf("latency", 42, Map.of("service", "api"),
    Emf.Options.defaults().withUnit("Milliseconds").withNamespace("MyApp"));
```

</details>

### Query

`filterLogEvents`, `filterLogEventsPage`, `getLogEvents` — [documentation](docs/functions/query.md)

<details><summary>Example in all five languages</summary>

**TypeScript**

```ts
for await (const e of client.filterLogEvents({ group: "my-app/api", pattern: '"timed out"', start: new Date(Date.now() - 3_600_000) })) {
  console.log(e.logStreamName, e.message);
}
const page = await client.filterLogEventsPage({ group: "my-app/api", pageSize: 100 });
const head = client.getLogEvents("my-app/api", "web-1", { startFromHead: true, limit: 50 });
```

**Rust**

```rust
let hits = client.filter_log_events(FilterLogEventsOptions { group: Some("my-app/api".into()), pattern: Some("\"timed out\"".into()), ..Default::default() }).try_collect().await?;
let page = client.filter_log_events_page(FilterLogEventsOptions { group: Some("my-app/api".into()), ..Default::default() }, None).await?;
let head = client.get_log_events("my-app/api", "web-1", GetLogEventsOptions { start_from_head: true, limit: Some(50), ..Default::default() }).try_collect().await?;
```

**Go**

```go
for e, err := range client.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: "my-app/api", Pattern: `"timed out"`, Start: time.Now().Add(-time.Hour)}) {
	if err != nil {
		return err
	}
	fmt.Println(e.LogStreamName, e.Message)
}
page, err := client.FilterLogEventsPage(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: "my-app/api", PageSize: 100}, "")
head := client.GetLogEvents(ctx, "my-app/api", "web-1", cloudwatchlogs.GetLogEventsOptions{StartFromHead: true, Limit: 50})
```

**Python**

```python
for e in client.filter_log_events(group="my-app/api", pattern='"timed out"', start=datetime.now(timezone.utc) - timedelta(hours=1)):
    print(e.log_stream_name, e.message)
page = client.filter_log_events_page(group="my-app/api", page_size=100)
head = list(client.get_log_events("my-app/api", "web-1", start_from_head=True, limit=50))
```

**Java**

```java
for (FilteredLogEvent e : client.filterLogEvents(FilterLogEventsOptions.builder()
        .group("my-app/api").pattern("\"timed out\"").start(Instant.now().minusSeconds(3600)).build())) {
    System.out.println(e.logStreamName().orElse("") + " " + e.message());
}
FilterLogEventsPage page = client.filterLogEventsPage(FilterLogEventsOptions.builder().group("my-app/api").pageSize(100).build(), null);
var head = client.getLogEvents("my-app/api", "web-1", GetLogEventsOptions.builder().startFromHead(true).limit(50).build());
```

</details>

### Logs Insights

`startQuery`, `getQueryResults`, `query`, `stopQuery`, `describeQueries` —
[documentation](docs/functions/insights.md)

<details><summary>Example in all five languages</summary>

**TypeScript**

```ts
const r = await client.query("fields @timestamp, @message | filter @message like /ERROR/ | limit 20", {
  groups: ["my-app/api"], start: new Date(Date.now() - 3_600_000), end: new Date(), timeoutMs: 30_000,
});
for (const row of r.rows) console.log(row["@timestamp"], row["@message"]);
const id = await client.startQuery("stats count(*) by bin(5m)", { groups: ["my-app/api"] });
const partial = await client.getQueryResults(id);
await client.stopQuery(id);
```

**Rust**

```rust
let r = client.query("fields @timestamp, @message | limit 20", QueryOptions::default().groups(["my-app/api"]).range(Some(SystemTime::now() - Duration::from_secs(3600)), None)).await?;
let id = client.start_query("stats count(*) by bin(5m)", QueryOptions::default().groups(["my-app/api"])).await?;
let partial = client.get_query_results(&id).await?;
client.stop_query(&id).await?;
```

**Go**

```go
r, err := client.Query(ctx, "fields @timestamp, @message | limit 20", cloudwatchlogs.QueryOptions{Groups: []string{"my-app/api"}, Start: time.Now().Add(-time.Hour)})
for _, row := range r.Rows {
	fmt.Println(row["@timestamp"], row["@message"])
}
id, err := client.StartQuery(ctx, "stats count(*) by bin(5m)", cloudwatchlogs.QueryOptions{Groups: []string{"my-app/api"}})
partial, err := client.GetQueryResults(ctx, id)
_, err = client.StopQuery(ctx, id)
```

**Python**

```python
r = client.query("fields @timestamp, @message | limit 20", groups=["my-app/api"], start=datetime.now(timezone.utc) - timedelta(hours=1))
qid = client.start_query("stats count(*) by bin(5m)", groups=["my-app/api"])
partial = client.get_query_results(qid)
client.stop_query(qid)
```

**Java**

```java
QueryResults r = client.query("fields @timestamp, @message | limit 20",
    QueryOptions.builder().groups(List.of("my-app/api")).start(Instant.now().minusSeconds(3600)).build());
String id = client.startQuery("stats count(*) by bin(5m)", QueryOptions.builder().groups(List.of("my-app/api")).build());
QueryResults partial = client.getQueryResults(id);
client.stopQuery(id);
```

</details>

### Live tail

`startLiveTail` — [documentation](docs/functions/live-tail.md)

<details><summary>Example in all five languages</summary>

**TypeScript**

```ts
const handle = await client.startLiveTail({ groups: ["my-app/api"], pattern: "ERROR" }, (e) => console.log(e.message), {
  onUpdate: (u) => console.log("batch of", u.count),
});
await handle.close();
```

**Rust**

```rust
let session = client.start_live_tail(LiveTailOptions::default().groups(["my-app/api"]).pattern("ERROR"), Arc::new(|e| println!("{}", e.message))).await?;
session.close();
```

**Go**

```go
s, err := client.StartLiveTail(ctx, cloudwatchlogs.LiveTailOptions{Groups: []string{"my-app/api"}, Pattern: "ERROR"}, func(e cloudwatchlogs.LiveTailEvent) {
	fmt.Println(e.Message)
})
defer s.Close()
```

**Python**

```python
session = client.start_live_tail(["my-app/api"], lambda e: print(e.message), pattern="ERROR")
session.close()
```

**Java**

```java
LiveTailHandle h = client.startLiveTail(
    LiveTailRequest.builder().groups(List.of("my-app/api")).pattern("ERROR").build(),
    e -> System.out.println(e.message()));
h.close();
```

</details>

### Logging (`LogClient`)

`logClient`, `exporter`, `logQueryClient` → `search`, `tail`, `listStreams`, `listLabels`,
`waitForLog` — [documentation](docs/functions/logging.md) and [Logging](docs/logging.md)

<details><summary>Example in all five languages</summary>

**TypeScript**

```ts
const logs = client.logClient({ group: "my-app/api", resource: { "service.name": "api" } });
logs.logger("http").info("request served", { status: 200 });
await logs.flush();
const q = client.logQueryClient({ group: "my-app/api" });
const { records, nextCursor } = await q.search("request", { limit: 20 }); // newest first
```

**Rust**

```rust
let logs = client.log_client(LogClientOptions { exporter: ExporterOptions { group: Some("my-app/api".into()), ..Default::default() }, ..Default::default() })?;
logs.logger(Some("http"), attrs! {}).info("request served", attrs! { "status" => 200 });
logs.flush(None).await?;
let q = client.log_query_client(Some("my-app/api"))?;
let page = q.search("request", SearchOptions::default().limit(20)).await?;
```

**Go**

```go
logs, err := client.LogClient(cloudwatchlogs.LogClientConfig{Group: "my-app/api"})
logs.Logger("http", nil).Info(ctx, "request served", "status", 200)
err = logs.Flush(ctx)
q, err := client.LogQueryClient("my-app/api")
page, err := q.Search(ctx, "request", cloudwatchlogs.SearchOptions{Limit: 20})
```

**Python**

```python
logs = client.log_client(group="my-app/api", resource={"service.name": "api"})
logs.logger("http").info("request served", {"status": 200})
logs.flush()
q = client.log_query_client("my-app/api")
page = q.search("request", limit=20)
```

**Java**

```java
BatchingLogClient logs = client.logClient(CloudWatchLogClientOptions.defaults().withTarget("my-app/api", null));
logs.logger("http").info("request served", Map.of("status", 200));
logs.flush(Duration.ofSeconds(5));
LogSearchResult page = client.logQueryClient("my-app/api").search("request", SearchOptions.builder().limit(20).build());
```

</details>

<!-- /service:function-overview -->

## Testing with Docker

The integration suite runs every public function against `motoserver/moto:5.2.4` (IAM
authentication on) in Docker. A one-shot bootstrap container creates an admin user and a read-only
user, a seed log group with a stream and three events, and writes the credentials to
`/bootstrap/env`. Each language package is packed and installed into a separate consumer project
first, so the tests use the package exactly as you would.

```sh
make test-docker                     # all five languages
make test-docker LANGS="go python"   # a subset
```

No host ports are published and every run uses its own Compose project, so runs never collide; the
stack is always removed afterwards (`docker compose down -v`). Unit tests need no Docker:
`make test` or `make coverage`.

## Compatibility

| | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| Runtime | Node.js ≥ 22.12, ESM + CJS | stable, MSRV 1.88 | 1.26, 1.27 | 3.10 – 3.14 | 17, 21, 25 |
| Async | `Promise` + `AbortSignal` | `async` (tokio), `blocking` feature | `context.Context` | sync + `asyncio` (`[async]` extra) | sync + `CompletableFuture` |
| AWS SDK | `@aws-sdk/client-cloudwatch-logs` v3 | `aws-sdk-cloudwatchlogs` 1.x | `aws-sdk-go-v2/service/cloudwatchlogs` | boto3 / aioboto3 | AWS SDK for Java v2 |
| Docker suite | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` |

Moto cannot serve everything: `StopQuery` returns HTTP 500, Logs Insights only runs `fields`,
`sort` and `limit`, the `PutLogEvents` limits are not enforced, live tail replays existing events
once, and `rejectedLogEventsInfo` end indexes are one lower than on AWS. Those paths are covered by
unit tests; the full list, the language differences and the unit-test-only functions are in
[docs/compatibility.md](docs/compatibility.md).

## Contributing

Contributions are welcome — please read [CONTRIBUTING.md](CONTRIBUTING.md) and
[AGENTS.md](AGENTS.md) first. All five languages keep the same API; a change lands in all of them
together. Parts of this repository are generated by
[sdk-kit](https://github.com/grow-codes-packages/sdk-kit) and changed there.

## Security

Please report vulnerabilities privately to [opensource@grow.codes](mailto:opensource@grow.codes).
See [SECURITY.md](SECURITY.md).

## Licence

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE) for attributions.

## Trademarks and disclaimer

This is an independent, community-maintained project. It is **not affiliated with, endorsed or
sponsored by its owner (Amazon.com, Inc. or its affiliates)**. Amazon CloudWatch Logs is a trademark of Amazon.com, Inc. or its affiliates; it is used here only to name the
service this SDK connects to. The Amazon CloudWatch Logs server is licensed separately by its owner
(Proprietary managed service; AWS SDKs Apache-2.0).

The software is provided "AS IS", without warranties or conditions of any kind. Grow Codes is not
responsible for any damage, data loss, security incident, outage or cost that results from using it.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## About Grow Codes

This SDK is built and maintained by [Grow Codes](https://grow.codes). Grow Codes publishes the
client libraries it builds as open source in the
[grow-codes-packages](https://github.com/grow-codes-packages) organisation, generated from one
shared kit so that every service and every language behaves the same way. Learn more at
[grow.codes](https://grow.codes).
