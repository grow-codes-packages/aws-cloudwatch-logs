// Service-owned: LogExporter, LogQueryClient (search asc/desc, cursors, tail, listStreams) and
// client.logClient() against the stateful fake.

import { afterEach, describe, expect, it } from "vitest";
import {
  CloudWatchLogsExporter,
  type LogRecord,
  SEARCH_SCAN_CAP,
  Severity,
  eventToLogRecord,
  recordToMessage,
  serializeLogRecord,
  toFilterPattern,
  utf8Bytes,
} from "../src/index.js";
import { eventStream, ok, streamResponse, testClient } from "./helpers/aws.js";
import { startFakeLogs } from "./helpers/fake-logs.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups.splice(0).reverse()) await fn();
});

async function setup(
  options: Parameters<typeof testClient>[1] = {},
  override?: Parameters<typeof startFakeLogs>[0],
) {
  const fake = await startFakeLogs(override);
  const client = testClient(fake, { logGroup: "app/logs", logStream: "s1", ...options });
  cleanups.push(
    () => fake.close(),
    () => client.close(),
  );
  return { fake, client, state: fake.state };
}

function record(body: string, extra: Partial<LogRecord> = {}): LogRecord {
  return {
    timestamp: new Date(1_795_000_000_000),
    severity: Severity.Info,
    severityText: "INFO",
    body,
    attributes: {},
    resource: {},
    ...extra,
  };
}

describe("helpers", () => {
  it("toFilterPattern quotes plain text and passes native patterns through", () => {
    expect(toFilterPattern("")).toBe("");
    expect(toFilterPattern("  ")).toBe("");
    expect(toFilterPattern("hello world")).toBe('"hello world"');
    expect(toFilterPattern('say "hi" \\ ok')).toBe('"say \\"hi\\" \\\\ ok"');
    for (const native of [
      '{ $.level = "error" }',
      "?ERROR ?WARN",
      "[ip, user]",
      "-DEBUG",
      '"quoted"',
    ]) {
      expect(toFilterPattern(native)).toBe(native);
    }
  });

  it("eventToLogRecord decodes normalized records and wraps other messages", () => {
    const r = record("hello", {
      attributes: { k: "v" },
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    });
    const decoded = eventToLogRecord({ timestamp: new Date(1), message: serializeLogRecord(r) });
    expect(decoded.body).toBe("hello");
    expect(decoded.traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
    const plain = eventToLogRecord({
      timestamp: new Date(5),
      message: '{"level":"info"}',
      logStreamName: "s",
      eventId: "e1",
      ingestionTime: new Date(6),
    });
    expect(plain).toMatchObject({
      body: '{"level":"info"}',
      severity: Severity.Info,
      attributes: { "aws.log.stream": "s", "aws.event.id": "e1" },
    });
    expect(plain.observedTimestamp?.getTime()).toBe(6);
    const tail = eventToLogRecord({
      timestamp: new Date(5),
      message: "x",
      logGroupIdentifier: "arn:g",
    });
    expect(tail.attributes).toEqual({ "aws.log.group": "arn:g" });
  });

  it("recordToMessage keeps JSON valid while truncating the body", () => {
    const small = recordToMessage(record("hi"), 1000, "truncate");
    expect(JSON.parse(small ?? "")).toMatchObject({ body: "hi" });
    const big = record(`${"é".repeat(2000)}"quotes"\n`, { attributes: { a: "x" } });
    const msg = recordToMessage(big, 1000, "truncate") ?? "";
    expect(utf8Bytes(msg)).toBeLessThanOrEqual(1000);
    expect(JSON.parse(msg).body).toMatch(/truncated \d+ bytes\]$/);
    expect(recordToMessage(big, 1000, "drop")).toBeUndefined();
    expect(recordToMessage(big, 1000, "error")).toBeUndefined();
    // attributes alone exceed the budget: plain cut as the last resort
    const fat = record("b", { attributes: { blob: "z".repeat(5000) } });
    expect(utf8Bytes(recordToMessage(fat, 1000, "truncate") ?? "")).toBeLessThanOrEqual(1000);
  });
});

describe("CloudWatchLogsExporter", () => {
  it("exports records as JSON events and counts rejections", async () => {
    const { client, state } = await setup();
    state.addGroup("app/logs");
    const exporter = new CloudWatchLogsExporter(client, { group: "app/logs", stream: "s1" });
    const res = await exporter.export([record("one"), record("two")], { gzip: true });
    expect(res).toEqual({ accepted: 2, rejected: 0 });
    const stored = state.group("app/logs")?.streams.get("s1") ?? [];
    expect(stored.map((e) => (JSON.parse(e.message) as { body: string }).body)).toEqual([
      "one",
      "two",
    ]);
    expect(stored[0]?.timestamp).toBe(1_795_000_000_000);
    await exporter.shutdown();
  });

  it("reports oversized (drop/error) and service-rejected records with an error", async () => {
    const { client } = await setup({ maxEventBytes: 200, oversizePolicy: "error" }, () =>
      ok({ rejectedLogEventsInfo: { tooOldLogEventEndIndex: 0 } }),
    );
    const exporter = new CloudWatchLogsExporter(client, {
      group: "app/logs",
      stream: "s1",
      createGroup: true,
    });
    const res = await exporter.export([record("x".repeat(500)), record("a"), record("b")], {
      gzip: false,
      signal: new AbortController().signal,
    });
    expect(res.accepted).toBe(1);
    expect(res.rejected).toBe(2);
    expect(res.error?.code).toBe("VALIDATION");
    const onlyOversized = await exporter.export([record("x".repeat(500))], { gzip: false });
    expect(onlyOversized).toMatchObject({ accepted: 0, rejected: 1 });
  });

  it("creates the group with createGroup", async () => {
    const { client, state } = await setup();
    const exporter = new CloudWatchLogsExporter(client, {
      group: "new/group",
      stream: "s",
      createGroup: true,
    });
    expect((await exporter.export([record("x")], { gzip: true })).accepted).toBe(1);
    expect(state.group("new/group")?.streams.get("s")).toHaveLength(1);
  });
});

describe("logClient + logQueryClient", () => {
  it("round trip: logger → flush → search (desc default, asc) with cursors", async () => {
    const { client, state } = await setup();
    state.addGroup("app/logs");
    const logs = client.logClient({
      resource: { "service.name": "svc" },
      batch: { flushIntervalMs: 50 },
    });
    const log = logs.logger("it", { component: "x" });
    const base = Date.now() - 10_000;
    for (let i = 0; i < 5; i++) {
      logs.send({
        message: `msg run42 ${i}`,
        timestamp: new Date(base + i),
        attributes: { password: "hunter2" },
      });
    }
    log.warn("unrelated");
    await logs.flush();
    expect(logs.stats()).toMatchObject({ sent: 6, dropped: 0, failed: 0 });
    const q = client.logQueryClient();
    expect(q.group).toBe("app/logs");

    const desc1 = await q.search("run42", { limit: 2 });
    expect(desc1.records.map((r) => r.body)).toEqual(["msg run42 4", "msg run42 3"]);
    expect(desc1.records[0]?.resource["service.name"]).toBe("svc");
    expect(JSON.stringify(desc1)).not.toContain("hunter2");
    const desc2 = await q.search("run42", { limit: 2, cursor: desc1.nextCursor });
    expect(desc2.records.map((r) => r.body)).toEqual(["msg run42 2", "msg run42 1"]);
    const desc3 = await q.search("run42", { limit: 2, cursor: desc2.nextCursor });
    expect(desc3.records.map((r) => r.body)).toEqual(["msg run42 0"]);

    const asc1 = await q.search("run42", { order: "asc", limit: 3 });
    expect(asc1.records.map((r) => r.body)).toEqual(["msg run42 0", "msg run42 1", "msg run42 2"]);
    const asc2 = await q.search("run42", { order: "asc", limit: 3, cursor: asc1.nextCursor });
    expect(asc2.records.map((r) => r.body)).toEqual(["msg run42 3", "msg run42 4"]);
    expect(asc2.nextCursor).toBeUndefined();
    const windowed = await q.search("run42", {
      order: "asc",
      start: new Date(base + 1),
      end: new Date(base + 2),
    });
    expect(windowed.records).toHaveLength(2);

    const found = await q.waitForLog("run42 3", { timeoutMs: 1000, intervalMs: 10 });
    expect(found.body).toBe("msg run42 3");
    expect(await q.listStreams()).toEqual(["s1"]);
    await expect(q.listLabels()).rejects.toMatchObject({ code: "UNSUPPORTED" });
    await logs.close();
  });

  it("desc search walks older slices, honours start/end and stops at the window", async () => {
    const { client, state } = await setup();
    state.addGroup("app/logs", { creationTime: Date.now() - 24 * 3_600_000 });
    const now = Date.now();
    state.put("app/logs", "s1", [
      { timestamp: now - 3 * 3_600_000, message: "old hit" },
      { timestamp: now - 30_000, message: "new hit" },
      { timestamp: now - 26 * 3_600_000, message: "too old hit" },
    ]);
    const q = client.logQueryClient();
    const res = await q.search("hit", { limit: 10 });
    expect(res.records.map((r) => r.body)).toEqual(["new hit", "old hit"]);
    expect(res.nextCursor).toBeUndefined();
    const all = await q.search("hit", {
      start: new Date(now - 30 * 3_600_000),
      end: new Date(now),
    });
    expect(all.records).toHaveLength(3);
    // exactly `limit` records with more window left → cursor to the next slice
    const one = await q.search("new hit", { limit: 1 });
    expect(one.records).toHaveLength(1);
    expect(one.nextCursor).toBeDefined();
    expect((await q.search("new hit", { limit: 1, cursor: one.nextCursor })).records).toEqual([]);
  });

  it("narrows a slice that holds more than the scan cap", async () => {
    let scans = 0;
    const { client } = await setup({}, (req) => {
      if (req.target !== "FilterLogEvents") return undefined;
      scans++;
      const start = req.body.startTime as number;
      const end = req.body.endTime as number;
      const many = end - start > 20_000;
      const count = many ? SEARCH_SCAN_CAP + 1 : 3;
      return ok({
        events: Array.from({ length: count }, (_, i) => ({
          eventId: String(i),
          timestamp: end - i,
          message: `e${i}`,
        })),
      });
    });
    const res = await client.logQueryClient().search("", { limit: 2 });
    expect(res.records).toHaveLength(2);
    expect(scans).toBeGreaterThan(2);
  });

  it("validates limit and cursors", async () => {
    const { client } = await setup();
    const q = client.logQueryClient({ group: "other" });
    expect(q.group).toBe("other");
    await expect(q.search("x", { limit: 0 })).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(q.search("x", { cursor: "%%%" })).rejects.toMatchObject({ code: "VALIDATION" });
    const asc = Buffer.from(JSON.stringify({ o: "a", t: "t" })).toString("base64url");
    await expect(q.search("x", { cursor: asc })).rejects.toMatchObject({ code: "VALIDATION" });
    const bad = Buffer.from(JSON.stringify({ o: "d", e: 1, w: 0, k: 0 })).toString("base64url");
    await expect(q.search("x", { cursor: bad })).rejects.toMatchObject({ code: "VALIDATION" });
  });

  it("logClient / logQueryClient need a log group", async () => {
    const fake = await startFakeLogs();
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    expect(() => client.logClient()).toThrow(/config.logGroup/);
    expect(() => client.logQueryClient()).toThrow(/config.logGroup/);
    expect(() => client.logClient({ group: "g", stream: "bad:stream" })).toThrow(/log stream/);
  });

  it("client.close() flushes log clients", async () => {
    const { client, state } = await setup();
    state.addGroup("app/logs");
    const logs = client.logClient({ batch: { flushIntervalMs: 60_000 } });
    logs.send({ message: "flushed on close" });
    await client.close();
    expect(state.group("app/logs")?.streams.get("s1")?.length).toBe(1);
  });

  it("tail streams records through Live Tail and stops", async () => {
    const start = { type: "sessionStart", payload: { sessionId: "t1" } };
    const upd = {
      type: "sessionUpdate",
      payload: {
        sessionMetadata: { sampled: false },
        sessionResults: [{ message: "tail me", timestamp: 1, logStreamName: "s1" }],
      },
    };
    const { client, fake } = await setup({ accountId: "123456789012" }, (req) =>
      req.target === "StartLiveTail"
        ? {
            ...streamResponse(Buffer.alloc(0)),
            chunks: [{ data: eventStream([start, upd]) }],
            keepOpen: true,
          }
        : undefined,
    );
    const q = client.logQueryClient();
    const got: LogRecord[] = [];
    const handle = q.tail("tail me", (r) => got.push(r));
    await new Promise((r) => setTimeout(r, 150));
    expect(got.map((r) => r.body)).toEqual(["tail me"]);
    expect(
      fake.requests.find((r) => r.target === "StartLiveTail")?.body.logEventFilterPattern,
    ).toBe('"tail me"');
    handle.stop();
    handle.stop();
    // stop before the session started, and errors go to onError
    const errors: unknown[] = [];
    const early = q.tail("x", () => undefined, { onError: (e) => errors.push(e) });
    early.stop();
    const failing = client.logQueryClient({ group: "bad name" }).tail("x", () => undefined, {
      onError: (e) => errors.push(e),
    });
    await new Promise((r) => setTimeout(r, 50));
    failing.stop();
    expect(errors).toHaveLength(1);
  });
});
