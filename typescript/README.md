# @grow-codes/aws-cloudwatch-logs

TypeScript SDK for [Amazon CloudWatch Logs](https://grow-codes-packages.github.io/aws-cloudwatch-logs/) by Grow Codes.
Node.js 22.12 or newer, ESM and CommonJS, full type definitions.

```sh
npm install @grow-codes/aws-cloudwatch-logs
```

```ts
import { CloudWatchLogsClient } from "@grow-codes/aws-cloudwatch-logs";

// Reads GROW_AWS_CLOUDWATCH_LOGS_URL, GROW_AWS_CLOUDWATCH_LOGS_TOKEN, timeouts, TLS and proxy settings.
const client = CloudWatchLogsClient.fromEnv();
try {
  console.log(await client.health()); // { healthy: true, status: "ok", details: {} }
  console.log(`ping: ${(await client.ping()).toFixed(1)} ms`);
} finally {
  await client.close();
}
```

Every error is a `CloudWatchLogsError` subclass with a canonical `code` (`NOT_FOUND`,
`AUTHENTICATION`, `RATE_LIMITED`, …), `httpStatus`, `requestId` and `retryable`. Credentials
never appear in messages, logs, hook data or `toString()` output.

Test helpers (in-memory fakes and the conformance runner) live in
`@grow-codes/aws-cloudwatch-logs/testing`.

Documentation: https://grow-codes-packages.github.io/aws-cloudwatch-logs/ · Licence: Apache-2.0.
Amazon CloudWatch Logs is a trademark of Amazon.com, Inc. or its affiliates; this package is not affiliated with or endorsed by it.
