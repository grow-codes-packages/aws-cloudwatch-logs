# Compatibility

## Runtimes

| Language | Supported | Tested in CI | AWS SDK |
| - | - | - | - |
| TypeScript | Node.js ≥ 22.12, ESM and CommonJS | Node 22, 24 | `@aws-sdk/client-cloudwatch-logs` v3 |
| Rust | MSRV 1.88, edition 2021, tokio | stable, 1.88 | `aws-sdk-cloudwatchlogs` 1.x + `aws-config` |
| Go | Go ≥ 1.26 | 1.26, 1.27 | `github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs` |
| Python | ≥ 3.10 | 3.10, 3.13, 3.14 | `boto3` (sync), `aioboto3` (async, extra `[async]`) |
| Java | ≥ 17 (Java 17 bytecode) | Temurin 17, 21, 25 | AWS SDK for Java v2 (`cloudwatchlogs`, async client) |

## Amazon CloudWatch Logs versions

CloudWatch Logs is a managed service with one public API version (`Logs_20140328`); the packages
follow the current API through the official AWS SDKs. The Docker integration suite runs against the
AWS emulator **`motoserver/moto:5.2.4`** (pinned by digest, IAM authentication on). Recorded AWS
responses drive the unit tests for everything Moto cannot reproduce.

## Error mapping

Errors are mapped by the **AWS error code**; the HTTP status is only the fallback (AWS JSON services
answer HTTP 400 for throttling and access denied). The table is identical in all five languages.

| AWS error code | Code | Retryable |
| - | - | - |
| `ResourceNotFoundException` | `NOT_FOUND` | no |
| `ResourceAlreadyExistsException`, `DataAlreadyAcceptedException`, `InvalidSequenceTokenException` | `CONFLICT` | no |
| `OperationAbortedException` | `CONFLICT` | **yes** (Java: no, see below) |
| `InvalidParameterException`, `ValidationException`, `MalformedQueryException`, `InvalidOperationException`, `LimitExceededException` | `VALIDATION` | no (`query` retries `LimitExceededException` itself) |
| `AccessDeniedException`, `AccessDenied` | `PERMISSION_DENIED` | no |
| `UnrecognizedClientException`, `InvalidClientTokenId`, `InvalidSignatureException`, `SignatureDoesNotMatch`, `IncompleteSignature`, `MissingAuthenticationToken(Exception)`, `ExpiredToken(Exception)` | `AUTHENTICATION` | no |
| `ThrottlingException`, `TooManyRequestsException` | `RATE_LIMITED` | yes |
| `ServiceUnavailableException`, `InternalFailure`, `SessionStreamingException` | `UNAVAILABLE` | yes |
| `SessionTimeoutException` | `TIMEOUT` | yes |
| no AWS code, HTTP 403 | `AUTHENTICATION` when the body names `InvalidClientTokenId`, `SignatureDoesNotMatch`, `security token` or `UnrecognizedClient`, else `PERMISSION_DENIED` | no |
| no AWS code, other status | by status (400 `VALIDATION`, 404 `NOT_FOUND`, 5xx `UNAVAILABLE`, network `UNAVAILABLE`) | 5xx / network |

Error messages pass through redaction; the AWS error code of a mapped error is available with
`awsErrorCode(err)` (Go `AWSErrorCode`, Rust / Python `aws_error_code`).

## Functions covered by unit tests only

Every public function is called against Moto in the Docker suite. These **paths** cannot be
reproduced on Moto 5.2.4 and are covered by unit tests against recorded AWS responses (the optional
live AWS job checks them against a sandbox account):

| Function / path | Why |
| - | - |
| `stopQuery` success | Moto answers HTTP 500 (`NotImplemented`); the Docker suite asserts `UNAVAILABLE` |
| `query` pending, `Failed`, `Cancelled`, `Timeout` and deadline paths; AWS `@timestamp` format | Moto completes every query at once and returns `@timestamp` in epoch ms |
| Logs Insights `filter`, `stats`, `parse` | Moto only runs `fields`, `sort` and `limit` |
| `putLogEvents` service-limit errors (10,001 events, > 1 MiB, span > 24 h, oversize events) | Moto accepts them; the SDK splits and truncates client side and unit-tests the limits |
| `putLogEvents` `rejectedLogEventsInfo` counting | Moto returns end indexes one lower than AWS (below) |
| `startLiveTail` keep-alive updates, `SessionStreamingException`, `SessionTimeoutException`, long sessions, close mid-stream | Moto replays the existing events once in one update and closes the stream |
| `ensureLogGroup` KMS association on AWS (`AssociateKmsKey`) | Moto answers `AssociateKmsKey` with HTTP 500; the SDK never re-associates keys and raises `CONFLICT` on a mismatch, which runs on Moto |
| Throttling, `Retry-After`, `OperationAbortedException`, AWS-exact error shapes | Moto does not throttle and answers IAM errors as HTTP 403 XML |
| Assume role (`roleArn`) | needs STS with a role; unit-tested with an STS mock |

## Moto differences

| Area | Moto 5.2.4 | AWS | What the SDK does |
| - | - | - | - |
| `rejectedLogEventsInfo` | `tooOldLogEventEndIndex` / `expiredLogEventEndIndex` one lower (one old event at index 0 → `0`) | **exclusive** end index (`[old, ok, new]` → `tooOldLogEventEndIndex: 1, tooNewLogEventStartIndex: 2`; `[expired, ok]` → `expiredLogEventEndIndex: 1`), verified on AWS 2026-11-19 | follows AWS; integration tests never assert exact rejected counts |
| `logGroupIdentifier` | every identifier resolves to the most recently created log group (`GetLogEvents`, `DescribeLogStreams`, `StartLiveTail`) | resolves the given group | sends `logGroupName` wherever the API allows; live-tail integration tests tail the group created last |
| `DescribeQueries` `createTime` | float epoch milliseconds | integer | Go, Rust and Java drop the fraction before the AWS SDK parses it (their parsers reject floats) |
| `StopQuery` | HTTP 500 | stops the query | unit tests |
| Insights query states | completes at once, no `Scheduled` / `Running`, no failures | real states | unit tests for every state |
| Live tail endpoint | plain host | host prefix `stream-` (AWS SDK for JavaScript, Go, Java) or `streaming-` (botocore, aws-sdk-rust) | the Docker network gives Moto the aliases `stream-aws-cloudwatch-logs` and `streaming-aws-cloudwatch-logs` |
| Live tail stream | replays matching existing events in one update, then ends | streams new events for up to 3 h with keep-alive updates | unit tests for streaming semantics |
| IAM errors | HTTP 403 with a query-protocol XML body | JSON error with `__type` | 403 rule above; Go rewrites XML error bodies into the AWS JSON error shape before the SDK parses them |
| Filter patterns | JSON, `OR`/`NOT` patterns and `logStreamNamePrefix` are ignored | full syntax | integration tests use terms and phrases only |
| Retention values | accepts any value | only the listed values | validated client side (`VALIDATION`) |

## Skipped conformance scenarios

| Scenario | Reason |
| - | - |
| `core.health` step `serverVersion` | CloudWatch Logs has no version endpoint; `serverVersion()` raises `UNSUPPORTED` |

## Language differences

The API is the same in every language; only the conventions differ (naming, durations, async
model).

| Topic | Difference |
| - | - |
| `emf()` return value | TypeScript, Python and Java return the EMF document as an object / map; Go and Rust return the JSON text of the document (the same text `buildEmf` returns there) |
| `ensureLogGroup` retention | Go uses `RetentionDays *int` (helper `cloudwatchlogs.Days(n)`; `nil` = unchanged, `Days(0)` = never expire); the other languages use an optional number |
| `OperationAbortedException` | mapped to `CONFLICT` everywhere, but Java's generated `ConflictException` always has `retryable() == false` (template gap in sdk-kit 0.1.0, no constructor that sets it); the other four mark it retryable |
| Async | Python's `AsyncCloudWatchLogsClient` needs the extra `[async]` (aioboto3 pins a narrow boto3/botocore window; sync users keep an unpinned boto3); Rust offers a synchronous API behind the `blocking` feature; Java has `…Async` variants returning `CompletableFuture` |
| `exporter()` arguments | Java takes `(group, stream, createGroup)` positionally; the others take an options object |
| Iterators | TypeScript async iterators, Rust `Paginator` (`try_collect`), Go `iter.Seq2`, Python iterators / async iterators, Java lazy `Iterable` (+ `…Async` lists) |
| `truncateMessage` / `parseQueryTimestamp` | public helpers in Go and Rust (`parseInsightsTimestamp` in Java); internal elsewhere |
