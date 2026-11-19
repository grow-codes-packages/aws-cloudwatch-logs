// Integration tests: every service function against the real service (Moto 5.2.4) with a
// round-trip assertion. stopQuery is not implemented by Moto (HTTP 500): it is called once and
// must fail with UNAVAILABLE; its behaviour is covered by unit tests.

import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { after, before, test } from "node:test";
import {
  CloudWatchLogsClient,
  type LiveTailEvent,
  type LogRecord,
  Severity,
} from "@grow-codes/aws-cloudwatch-logs";
import { loadBootstrapEnv, runId } from "./env.ts";

const run = `${runId}-${randomBytes(4).toString("hex")}`.replace(/[^A-Za-z0-9_-]/g, "_");
const prefix = `sdk-ts/${run}`;
let client: CloudWatchLogsClient;
const createdGroups: string[] = [];

async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of it) out.push(x);
  return out;
}

async function newGroup(
  name: string,
  options: Parameters<CloudWatchLogsClient["ensureLogGroup"]>[1] = {},
) {
  const group = `${prefix}/${name}`;
  createdGroups.push(group);
  return { group, result: await client.ensureLogGroup(group, options) };
}

before(() => {
  loadBootstrapEnv();
  client = CloudWatchLogsClient.fromEnv({ retry: { maxRetries: 2 } });
});

after(async () => {
  for (const g of createdGroups) await client.deleteLogGroup(g, { ignoreMissing: true });
  await client.close();
});

test("seed data from the bootstrap is readable (filterLogEvents, getLogEvents)", async () => {
  const group = process.env.GROW_TEST_SEED_LOG_GROUP as string;
  const stream = process.env.GROW_TEST_SEED_LOG_STREAM as string;
  const marker = process.env.GROW_TEST_SEED_MARKER as string;
  const found = await collect(client.filterLogEvents({ group, pattern: marker }));
  assert.equal(found.length, Number(process.env.GROW_TEST_SEED_EVENT_COUNT));
  const all = await collect(client.getLogEvents(group, stream, { startFromHead: true }));
  assert.equal(all.length, found.length);
  assert.ok(all[0]?.message.includes(`${marker} 1`));
  const info = await client.getLogGroup(group);
  assert.equal(info?.retentionDays, 7);
  assert.equal(await client.logGroupArn(group), process.env.GROW_TEST_SEED_LOG_GROUP_ARN);
});

test("bootstrap: ensureLogGroup / tags / retention / ensureLogStream / describe / delete", async () => {
  const { group, result } = await newGroup("bootstrap", {
    retentionDays: 14,
    tags: { team: "sdk" },
  });
  assert.equal(result.created, true);
  assert.match(result.arn, /^arn:aws:logs:.*:log-group:sdk-ts\//);
  assert.ok(!result.arn.endsWith(":*"));

  // existing group: tags are added (never removed), retention re-applied
  const again = await client.ensureLogGroup(group, { retentionDays: 30, tags: { env: "it" } });
  assert.deepEqual(again, { created: false, arn: result.arn });
  assert.deepEqual(await client.listLogGroupTags(group), { team: "sdk", env: "it" });
  await client.tagLogGroup(group, { extra: "1" });
  await client.untagLogGroup(group, ["team"]);
  assert.deepEqual(await client.listLogGroupTags(group), { env: "it", extra: "1" });

  const described = await collect(client.describeLogGroups({ prefix: group }));
  assert.equal(described.length, 1);
  assert.equal(described[0]?.retentionDays, 30);
  await client.putRetentionPolicy(group, null);
  assert.equal((await client.getLogGroup(group))?.retentionDays, undefined);
  await client.putRetentionPolicy(group, 7);
  assert.equal((await client.getLogGroup(group))?.retentionDays, 7);

  assert.deepEqual(await client.ensureLogStream(group, "s-b"), { created: true });
  assert.deepEqual(await client.ensureLogStream(group, "s-b"), { created: false });
  await client.ensureLogStream(group, "s-a");
  await assert.rejects(client.ensureLogStream(`${prefix}/missing`, "s"), { code: "NOT_FOUND" });
  const streams = await collect(client.describeLogStreams(group, { pageSize: 1 }));
  assert.deepEqual(
    streams.map((s) => s.name),
    ["s-a", "s-b"],
  );
  assert.equal(
    (await collect(client.describeLogStreams(group, { prefix: "s-b" })))[0]?.name,
    "s-b",
  );

  // paging over several groups
  for (const n of ["p1", "p2", "p3"]) await newGroup(`pages/${n}`);
  const paged = await collect(
    client.describeLogGroups({ prefix: `${prefix}/pages/`, pageSize: 2 }),
  );
  assert.deepEqual(
    paged.map((g) => g.name.split("/").at(-1)),
    ["p1", "p2", "p3"],
  );

  assert.equal(await client.deleteLogStream(group, "s-a"), true);
  assert.equal(await client.deleteLogStream(group, "s-a", { ignoreMissing: true }), false);
  await assert.rejects(client.deleteLogStream(group, "s-a"), { code: "NOT_FOUND" });
  assert.equal(await client.deleteLogGroup(group), true);
  assert.equal(await client.deleteLogGroup(group, { ignoreMissing: true }), false);
  await assert.rejects(client.deleteLogGroup(group), { code: "NOT_FOUND" });
  assert.equal(await client.getLogGroup(group), undefined);
});

test("ensureLogGroup with a KMS key; a different key is a CONFLICT", async () => {
  const kms = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555";
  const { group } = await newGroup("kms", { kmsKeyId: kms });
  assert.equal((await client.getLogGroup(group))?.kmsKeyId, kms);
  await assert.rejects(client.ensureLogGroup(group, { kmsKeyId: "other-key" }), {
    code: "CONFLICT",
  });
});

test("ingest: putLogEvents (sorting, auto-created stream, truncation, many events) → read back", async () => {
  const { group } = await newGroup("ingest");
  const now = Date.now();
  const res = await client.putLogEvents(group, "auto-stream", [
    { timestamp: now - 1000, message: `b-${run}` },
    { timestamp: new Date(now - 2000), message: `a-${run}` },
    { timestamp: now - 500, message: "ü".repeat(200_000) }, // 400,000 bytes > 256 KiB → truncated
  ]);
  assert.deepEqual(res, {
    accepted: 3,
    rejected: 0,
    truncated: 1,
    dropped: 0,
    batches: 1,
    rejectedInfo: [],
  });
  const head = await collect(client.getLogEvents(group, "auto-stream", { startFromHead: true }));
  assert.deepEqual(
    head.slice(0, 2).map((e) => e.message),
    [`a-${run}`, `b-${run}`],
  );
  assert.ok(Buffer.byteLength(head[2]?.message ?? "") <= 262_144);
  assert.match(head[2]?.message ?? "", /…\[truncated \d+ bytes\]$/);
  const tail = await collect(client.getLogEvents(group, "auto-stream", { limit: 1 }));
  assert.equal(tail.length, 1);

  const many = Array.from({ length: 2500 }, (_, i) => ({
    timestamp: now - 3000 + (i % 1000),
    message: `bulk ${run} ${i}`,
  }));
  const bulk = await client.putLogEvents(group, "bulk", many);
  assert.equal(bulk.accepted, 2500);
  const back = await collect(
    client.filterLogEvents({ group, streams: ["bulk"], pattern: `"bulk ${run}"`, pageSize: 1000 }),
  );
  assert.equal(back.length, 2500);
  const limited = await collect(client.filterLogEvents({ group, pattern: run, limit: 10 }));
  assert.equal(limited.length, 10);
  const page = await client.filterLogEventsPage({ group, streams: ["bulk"], pageSize: 100 });
  assert.equal(page.events.length, 100);
  assert.ok(page.nextToken !== undefined);
  const windowed = await collect(
    client.filterLogEvents({
      group,
      streams: ["auto-stream"],
      start: new Date(now - 2000),
      end: new Date(now - 1000),
    }),
  );
  assert.deepEqual(
    windowed.map((e) => e.message),
    [`a-${run}`, `b-${run}`],
  );
  await assert.rejects(
    client.putLogEvents(`${prefix}/nogroup`, "s", [{ timestamp: now, message: "x" }]),
    {
      code: "NOT_FOUND",
    },
  );
});

test("emf: the EMF document is stored byte-identical", async () => {
  const { group } = await newGroup("emf");
  const doc = await client.emf(
    "latency",
    42,
    { service: "api" },
    { group, stream: "metrics", unit: "Milliseconds" },
  );
  const stored = await collect(client.getLogEvents(group, "metrics", { startFromHead: true }));
  assert.equal(stored.length, 1);
  assert.deepEqual(JSON.parse(stored[0]?.message ?? "{}"), doc);
});

test("Logs Insights: startQuery / getQueryResults / query / describeQueries (Moto: fields, sort, limit)", async () => {
  const { group } = await newGroup("insights");
  const now = Date.now();
  await client.putLogEvents(
    group,
    "s",
    [1, 2, 3].map((i) => ({ timestamp: now - 10_000 + i, message: `insight ${run} ${i}` })),
  );
  const range = { groups: [group], start: new Date(now - 3_600_000), end: new Date(now + 60_000) };
  const id = await client.startQuery(
    "fields @timestamp, @message | sort @timestamp desc | limit 2",
    range,
  );
  const results = await client.getQueryResults(id);
  assert.equal(results.status, "Complete");
  assert.equal(results.rows.length, 2);
  assert.equal(results.rows[0]?.["@message"], `insight ${run} 3`);
  const q = await client.query("fields @message | sort @timestamp asc", {
    ...range,
    timeoutMs: 20_000,
  });
  assert.equal(q.status, "Complete");
  assert.deepEqual(
    q.rows.map((r) => r["@message"]),
    [1, 2, 3].map((i) => `insight ${run} ${i}`),
  );
  const queries = await client.describeQueries({ group });
  assert.ok(queries.some((x) => x.queryId === id && x.status === "Complete"));
  // StopQuery is not implemented by Moto 5.2.4 (HTTP 500) → unit-tested only
  await assert.rejects(client.stopQuery(id), { code: "UNAVAILABLE" });
  await assert.rejects(client.startQuery("fields @message", { groups: [`${prefix}/none`] }), {
    code: "NOT_FOUND",
  });
});

test("startLiveTail: Moto replays matching existing events once, then ends the session", async () => {
  // Moto resolves every logGroupIdentifier to the most recently created group: create it last.
  const { group } = await newGroup(`tail-${randomBytes(3).toString("hex")}`);
  const now = Date.now();
  await client.putLogEvents(group, "tail-stream", [
    { timestamp: now - 2000, message: `tail ${run} one` },
    { timestamp: now - 1000, message: `tail ${run} two` },
  ]);
  const events: LiveTailEvent[] = [];
  const updates: number[] = [];
  const handle = await client.startLiveTail({ groups: [group] }, (e) => events.push(e), {
    onUpdate: (u) => updates.push(u.count),
  });
  assert.ok(handle.sessionId.length > 0);
  await Promise.race([handle.done, new Promise((r) => setTimeout(r, 15_000))]);
  await handle.close();
  assert.deepEqual(
    events.map((e) => e.message),
    [`tail ${run} one`, `tail ${run} two`],
  );
  assert.equal(events[0]?.logStreamName, "tail-stream");
  assert.ok(updates.length >= 1);
  assert.equal(handle.error, undefined);
});

test("logging category: logClient → search (desc/asc) / waitForLog / listStreams / tail / listLabels", async () => {
  const { group } = await newGroup(`logging-${randomBytes(3).toString("hex")}`);
  const logs = client.logClient({
    group,
    stream: "app",
    resource: { "service.name": `svc-${run}`, "deployment.environment": "it" },
    batch: { flushIntervalMs: 100 },
  });
  const log = logs.logger("integration", { component: "it" });
  const t0 = Date.now();
  log.info(`hello ${run}`, { request_id: `r-${run}`, password: `hunter2-${run}` });
  logs.send({
    timestamp: new Date(t0 + 5),
    message: `traced ${run}`,
    severity: Severity.Error,
    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    spanId: "00f067aa0ba902b7",
    traceFlags: 1,
  });
  await logs.flush();
  assert.deepEqual(logs.stats(), { queued: 0, sent: 2, dropped: 0, failed: 0, retries: 0 });

  const query = client.logQueryClient({ group });
  const traced = await query.waitForLog(`traced ${run}`, { timeoutMs: 20_000, intervalMs: 250 });
  assert.equal(traced.traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
  assert.equal(traced.spanId, "00f067aa0ba902b7");
  assert.equal(traced.severity, Severity.Error);
  assert.equal(traced.resource["service.name"], `svc-${run}`);

  const desc = await query.search(run, { limit: 1 });
  assert.equal(desc.records[0]?.body, `traced ${run}`);
  assert.ok(desc.nextCursor !== undefined);
  const desc2 = await query.search(run, { limit: 1, cursor: desc.nextCursor });
  const hello = desc2.records[0] as LogRecord;
  assert.equal(hello.body, `hello ${run}`);
  assert.equal(hello.attributes.request_id, `r-${run}`);
  assert.ok(!JSON.stringify(desc2).includes(`hunter2-${run}`), "password must be redacted");
  const asc = await query.search(run, { order: "asc", limit: 10 });
  assert.deepEqual(
    asc.records.map((r) => r.body),
    [`hello ${run}`, `traced ${run}`],
  );
  assert.deepEqual(await query.listStreams(), ["app"]);
  await assert.rejects(query.listLabels(), { code: "UNSUPPORTED" });

  // tail = Live Tail; on Moto it replays the existing matching events of the newest group
  const tailed: LogRecord[] = [];
  const handle = query.tail(`hello ${run}`, (r) => tailed.push(r));
  const deadline = Date.now() + 15_000;
  while (tailed.length === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 100));
  handle.stop();
  assert.equal(tailed[0]?.body, `hello ${run}`);
  await logs.close();
});
