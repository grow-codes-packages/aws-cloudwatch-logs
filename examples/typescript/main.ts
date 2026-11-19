// Amazon CloudWatch Logs with the Grow Codes TypeScript SDK: configuration from the environment,
// health and ping, log group / stream bootstrap, batched ingest, Embedded Metric Format, filter
// search, a Logs Insights query, Live Tail, the LogClient / Logger facade and error handling.
//
//   (cd ../../typescript && npm install && npm run build)   # or use the published package
//   npm install
//   AWS_REGION=us-east-1 AWS_PROFILE=dev npm start
//   # against an emulator: GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000 AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…

import {
  CloudWatchLogsClient,
  CloudWatchLogsError,
} from "@grow-codes/aws-cloudwatch-logs";

const group = process.env.EXAMPLE_LOG_GROUP ?? `grow-sdk-example/${Date.now()}`;
const client = CloudWatchLogsClient.fromEnv({
  logGroup: group,
  // Warnings only; the default logger is silent.
  logger: { debug() {}, info() {}, warn: console.warn, error: console.error },
  hooks: {
    onError(info) {
      if (info.willRetry)
        console.error(`retrying ${info.url} (attempt ${info.attempt})`);
    },
  },
});
console.log(`config: ${client.config}`); // credentials print as [REDACTED]

try {
  console.log("health:", await client.health()); // never throws for an unreachable service
  console.log(
    `ping: ${(await client.ping({ signal: AbortSignal.timeout(10_000) })).toFixed(1)} ms`,
  );

  // Bootstrap: idempotent group + stream.
  const { created, arn } = await client.ensureLogGroup(group, {
    retentionDays: 1,
    tags: { app: "example" },
  });
  console.log(`log group ${arn} (created: ${created})`);
  await client.ensureLogStream(group, "events");

  // Ingest: sorted, size-checked and split into PutLogEvents batches automatically.
  const now = Date.now();
  const put = await client.putLogEvents(group, "events", [
    { timestamp: now - 1000, message: "user signed in" },
    { timestamp: now, message: "ERROR payment declined" },
  ]);
  console.log("putLogEvents:", put);
  await client.emf(
    "checkout.latency",
    87,
    { service: "shop" },
    { unit: "Milliseconds", stream: "metrics" },
  );

  // Query: filter pattern search and stream reads.
  for await (const ev of client.filterLogEvents({
    pattern: "ERROR",
    start: now - 60_000,
  })) {
    console.log("filter:", ev.timestamp.toISOString(), ev.message);
  }
  for await (const ev of client.getLogEvents(group, "events", {
    startFromHead: true,
  })) {
    console.log("stream:", ev.message);
  }

  // Logs Insights: start + poll until complete.
  const insights = await client.query(
    "fields @timestamp, @message | sort @timestamp desc | limit 5",
    {
      start: new Date(now - 3_600_000),
      timeoutMs: 30_000,
    },
  );
  console.log(`insights (${insights.status}):`, insights.rows);

  // Live Tail (AWS streams new events until close(); emulators may replay and end).
  const tail = await client.startLiveTail({ groups: [group] }, (e) =>
    console.log("tail:", e.message),
  );
  setTimeout(() => void tail.close(), 3000);
  await tail.done;

  // Logging category: batched, redacted, trace-correlated records + normalized search.
  const logs = client.logClient({
    stream: "app",
    resource: { "service.name": "example" },
  });
  logs
    .logger("checkout")
    .warn("card retry", { order: 42, password: "never-sent" });
  await logs.flush();
  const found = await client
    .logQueryClient()
    .waitForLog("card retry", { timeoutMs: 30_000 });
  console.log("search:", found.severityText, found.body, found.attributes);
} catch (err) {
  if (err instanceof CloudWatchLogsError) {
    // Canonical code + HTTP status + request id; the message never contains credentials.
    console.error(`${err.code}: ${err.message}`, {
      httpStatus: err.httpStatus,
      requestId: err.requestId,
    });
    process.exitCode = 1;
  } else {
    throw err;
  }
} finally {
  if (process.env.EXAMPLE_LOG_GROUP === undefined) {
    await client
      .deleteLogGroup(group, { ignoreMissing: true })
      .catch(() => undefined);
  }
  await client.close(); // flushes log clients, stops live-tail sessions
}
