# Amazon CloudWatch Logs SDK

[![Grow Codes](https://img.shields.io/badge/Grow%20Codes-grow.codes-2ea44f)](https://grow.codes)
[![CI](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml/badge.svg)](https://github.com/grow-codes-packages/aws-cloudwatch-logs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](license.md)

One API for **Amazon CloudWatch Logs** in five languages: TypeScript, Rust, Go, Python and Java. Every package
has the same functions, options, defaults, error codes and behaviour, tested against a real
`motoserver/moto:5.2.4` in Docker.

| Language | Package | Coverage |
| - | - | - |
| TypeScript | [`@grow-codes/aws-cloudwatch-logs`](https://www.npmjs.com/package/@grow-codes/aws-cloudwatch-logs) | ![TypeScript coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-typescript.json) |
| Rust | [`grow-aws-cloudwatch-logs`](https://crates.io/crates/grow-aws-cloudwatch-logs) | ![Rust coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-rust.json) |
| Go | [`github.com/grow-codes-packages/aws-cloudwatch-logs/go`](https://pkg.go.dev/github.com/grow-codes-packages/aws-cloudwatch-logs/go) | ![Go coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-go.json) |
| Python | [`grow-aws-cloudwatch-logs`](https://pypi.org/project/grow-aws-cloudwatch-logs/) | ![Python coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-python.json) |
| Java | [`codes.grow.packages:aws-cloudwatch-logs`](https://central.sonatype.com/artifact/codes.grow.packages/aws-cloudwatch-logs) | ![Java coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/aws-cloudwatch-logs/badges/coverage-java.json) |

## What you get

- **Client** with health, authenticated ping and server version ([Client](client.md)).
- **Configuration** from code or `GROW_AWS_CLOUDWATCH_LOGS_*` / vendor environment variables, immutable
  and validated ([Configuration](configuration.md)).
- **Retries** with full-jitter backoff and `Retry-After`, connect/request/total timeouts and
  cancellation in every language.
- **Typed errors** with one set of codes across languages ([Error handling](errors.md)).
- **Redaction everywhere**: credentials never appear in errors, logs, hooks or `toString`
  ([Redaction](redaction.md)).
- **Logging**: batching log client, logger facade, redaction processor, framework adapters and
  log search ([Logging](logging.md)).
- **Service functions** for the Amazon CloudWatch Logs API ([Functions](functions.md)).

Start with [Install](install.md) and the [Quickstart](quickstart.md).

!!! note "Independent project"
    This SDK is made by [Grow Codes](https://grow.codes). Amazon CloudWatch Logs is a trademark of
    Amazon.com, Inc. or its affiliates. This project is not affiliated with or endorsed by Amazon.com, Inc. or its affiliates. See
    [License & disclaimer](license.md).
