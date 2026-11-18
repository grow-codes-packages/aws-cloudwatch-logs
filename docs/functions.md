# Functions

The Amazon CloudWatch Logs API of this SDK. Every function exists in all five languages with the same
behaviour; this page shows each one with an example per language.

!!! info "Work in progress"
    The service functions are being added. Until then the client offers the shared surface:
    [`health()`, `ping()`, `serverVersion()`, `close()`](client.md).

## Overview

| Function | Description | Integration test |
| - | - | - |
| `health()` | unauthenticated liveness | `core.health` |
| `ping()` | authenticated round trip | `core.health` |
| `serverVersion()` | server version | `core.health` |

## Service configuration

| Setting | Default | Environment variables (precedence order) |
| - | - | - |
| `url` | `http://127.0.0.1:5000` | `GROW_AWS_CLOUDWATCH_LOGS_URL` |
| `token` | none | `GROW_AWS_CLOUDWATCH_LOGS_TOKEN` |

Environment variables recognised by this SDK (explicit config values always win; `GROW_*` names
come before vendor names): standard AWS chain + `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`, `CLOUDWATCH_LOG_GROUP`, `CLOUDWATCH_LOG_STREAM`, `GROW_AWS_CLOUDWATCH_LOGS_*`.
