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
Python and Java** — one API, five idiomatic packages, tested against a real Amazon CloudWatch Logs in Docker.

- **Typed client** with timeouts, retries with jitter, `Retry-After`, cancellation and one error
  model (`CloudWatchLogsError` with canonical codes) in every language.
- **Secrets stay secret**: credentials are redacted in `toString`, logs, hooks and error messages.
- **Logging**: batched, compressed log shipping with back-pressure, redaction, a logger facade and
  adapters for the common logging frameworks of each language; log search where Amazon CloudWatch Logs offers it.

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

Tested in CI against **`motoserver/moto:5.2.4`** (the version pinned in the Grow Codes stack catalog). Other
versions with the same API usually work; see [Compatibility](#compatibility) and
[docs/compatibility.md](docs/compatibility.md).

## Install

| Language | Package | Install |
| - | - | - |
| TypeScript (Node.js ≥ 22.12) | `@grow-codes/aws-cloudwatch-logs` | `npm install @grow-codes/aws-cloudwatch-logs` |
| Rust (MSRV 1.88) | `grow-aws-cloudwatch-logs` | `cargo add grow-aws-cloudwatch-logs` |
| Go (1.26+) | `github.com/grow-codes-packages/aws-cloudwatch-logs/go` | `go get github.com/grow-codes-packages/aws-cloudwatch-logs/go` |
| Python (≥ 3.10) | `grow-aws-cloudwatch-logs` | `pip install grow-aws-cloudwatch-logs` |
| Java (17+) | `codes.grow.packages:aws-cloudwatch-logs` | see below |

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

Every package reads its configuration from the environment (`GROW_AWS_CLOUDWATCH_LOGS_URL`,
`GROW_AWS_CLOUDWATCH_LOGS_TOKEN`, …, see [Configuration](#configuration)) or from an explicit config
object. The examples check the service health and make one authenticated call.

### TypeScript

```ts
import { CloudWatchLogsClient } from "@grow-codes/aws-cloudwatch-logs";

const client = CloudWatchLogsClient.fromEnv();
try {
  const health = await client.health();
  console.log(health.healthy, health.status);
  console.log("latency ms:", await client.ping());
} finally {
  await client.close();
}
```

### Rust

```rust
use grow_aws_cloudwatch_logs::CloudWatchLogsClient;

#[tokio::main]
async fn main() -> Result<(), grow_aws_cloudwatch_logs::Error> {
    let client = CloudWatchLogsClient::from_env()?;
    let health = client.health().await?;
    println!("{} {}", health.healthy, health.status);
    println!("latency: {:?}", client.ping().await?);
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

	health, err := client.Health(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(health.Healthy, health.Status)
}
```

### Python

```python
from grow_aws_cloudwatch_logs import CloudWatchLogsClient

with CloudWatchLogsClient.from_env() as client:
    health = client.health()
    print(health.healthy, health.status)
    print("latency ms:", client.ping())
```

Async code uses `AsyncCloudWatchLogsClient` with the same methods (`async with`, `await client.health()`).

### Java

```java
import codes.grow.packages.cloudwatchlogs.CloudWatchLogsClient;

try (CloudWatchLogsClient client = CloudWatchLogsClient.fromEnv()) {
    var health = client.health();
    System.out.println(health.healthy() + " " + health.status());
    System.out.println("latency ms: " + client.ping());
}
```

More complete programs live in [examples/](examples/) — one folder per language.

## Configuration

All settings can be passed in code or through environment variables. Precedence: **explicit value >
`GROW_AWS_CLOUDWATCH_LOGS_*` variable > vendor variable > default.** A present but invalid value raises a
`ConfigError` that names the variable (never its value).

| Setting | Environment variable | Default |
| - | - | - |
| `url` | `GROW_AWS_CLOUDWATCH_LOGS_URL` | service default |
| `token` | `GROW_AWS_CLOUDWATCH_LOGS_TOKEN` | none |
| `connectTimeout` | `GROW_AWS_CLOUDWATCH_LOGS_CONNECT_TIMEOUT` | 10 s |
| `requestTimeout` (per attempt) | `GROW_AWS_CLOUDWATCH_LOGS_REQUEST_TIMEOUT` | 30 s |
| `totalTimeout` (including retries) | `GROW_AWS_CLOUDWATCH_LOGS_TOTAL_TIMEOUT` | 120 s |
| `retry.maxRetries` | `GROW_AWS_CLOUDWATCH_LOGS_MAX_RETRIES` | 3 |
| `tls.caCertPath` | `GROW_AWS_CLOUDWATCH_LOGS_CA_CERT` | system roots |
| `tls.clientCertPath` / `tls.clientKeyPath` | `GROW_AWS_CLOUDWATCH_LOGS_CLIENT_CERT` / `GROW_AWS_CLOUDWATCH_LOGS_CLIENT_KEY` | none |
| `tls.insecureSkipVerify` (tests only) | `GROW_AWS_CLOUDWATCH_LOGS_INSECURE_SKIP_VERIFY` | `false` |
| `proxy` | `GROW_AWS_CLOUDWATCH_LOGS_PROXY` | none (environment proxies are ignored) |
| `headers`, `userAgentSuffix`, `hooks`, `logger` | — | none |

Vendor variables that are also read: standard AWS chain + `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`, `CLOUDWATCH_LOG_GROUP`, `CLOUDWATCH_LOG_STREAM`, `GROW_AWS_CLOUDWATCH_LOGS_*`.

Durations accept milliseconds (`1500`) or units (`500ms`, `10s`, `1m30s`); booleans accept
`true/false/1/0/yes/no/on/off`. Field names follow each language's convention
(`request_timeout` in Python and Rust, `RequestTimeout` in Go). The full reference is in the
[documentation](https://grow-codes-packages.github.io/aws-cloudwatch-logs/).

## Function overview

<!-- service:function-overview — the service fills this section; keep the heading. -->

The same functions exist in every language, named per language convention (`getString` in
TypeScript and Java, `get_string` in Python and Rust, `GetString` in Go).

| Area | Functions | Description |
| - | - | - |
| Client | `health`, `ping`, `serverVersion`, `close` | liveness, authenticated round trip, server version, shutdown |
| Logging | `send`, `sendBatch`, `flush`, `close`, `stats`, `logger`, `search`, `tail`, `listStreams`, `listLabels`, `waitForLog` | log shipping and querying |

<!-- /service:function-overview -->

## Testing with Docker

The integration suite runs every public function against a real Amazon CloudWatch Logs (`motoserver/moto:5.2.4`) in Docker.
Each language package is packed and installed into a separate consumer project first, so the tests
use the package exactly as you would.

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
| Async | `Promise` + `AbortSignal` | `async` (tokio), `blocking` feature | `context.Context` | sync + `asyncio` | sync + `CompletableFuture` |
| Amazon CloudWatch Logs | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` | `motoserver/moto:5.2.4` |

Functions that Amazon CloudWatch Logs (or its emulator) cannot serve are listed with the reason in
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
sponsored by Amazon.com, Inc. or its affiliates**. Amazon CloudWatch Logs is a trademark of Amazon.com, Inc. or its affiliates; it is used here only to name the
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
