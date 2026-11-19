// Service-owned: PutLogEvents batching limits (unit-only: Moto enforces none), putLogEvents, emf.

import { afterEach, describe, expect, it } from "vitest";
import {
  DEFAULT_EMF_NAMESPACE,
  EVENT_OVERHEAD_BYTES,
  MAX_BATCH_BYTES,
  MAX_BATCH_SPAN_MS,
  MAX_EVENTS_PER_BATCH,
  buildEmf,
  planLogEventBatches,
  truncateMessage,
  utf8Bytes,
} from "../src/index.js";
import { awsError, fixtureResponse, ok, startFakeCloudWatch, testClient } from "./helpers/aws.js";
import { startFakeLogs } from "./helpers/fake-logs.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups.splice(0).reverse()) await fn();
});

async function setup(
  override?: Parameters<typeof startFakeLogs>[0],
  options: Parameters<typeof testClient>[1] = {},
) {
  const fake = await startFakeLogs(override);
  const client = testClient(fake, options);
  cleanups.push(
    () => fake.close(),
    () => client.close(),
  );
  return { fake, client, state: fake.state };
}

describe("truncateMessage", () => {
  it("keeps short messages and cuts on a UTF-8 boundary with a marker", () => {
    expect(truncateMessage("hello", 10)).toBe("hello");
    const long = "ü".repeat(200); // 400 bytes
    const cut = truncateMessage(long, 100);
    expect(utf8Bytes(cut)).toBeLessThanOrEqual(100);
    expect(cut).toMatch(/^ü+ …\[truncated \d+ bytes\]$/);
    const kept = cut.slice(0, cut.indexOf(" …"));
    const removed = Number(/truncated (\d+) bytes/.exec(cut)?.[1]);
    expect(utf8Bytes(kept) + removed).toBe(400);
    // marker longer than the budget
    expect(utf8Bytes(truncateMessage("x".repeat(100), 10))).toBeLessThanOrEqual(10);
    // digit-count boundary of the marker
    for (const n of [1000, 10_005, 100_010]) {
      const t = truncateMessage("a".repeat(n), 990);
      expect(utf8Bytes(t)).toBeLessThanOrEqual(990);
      expect(Number(/truncated (\d+)/.exec(t)?.[1])).toBe(n - t.indexOf(" …"));
    }
  });
});

describe("planLogEventBatches", () => {
  it("sorts stably and keeps one batch when everything fits", () => {
    const plan = planLogEventBatches([
      { timestamp: 3, message: "c" },
      { timestamp: new Date(1), message: "a" },
      { timestamp: 3, message: "d" },
      { timestamp: 2, message: "b" },
    ]);
    expect(plan.batches).toEqual([
      [
        { timestamp: 1, message: "a" },
        { timestamp: 2, message: "b" },
        { timestamp: 3, message: "c" },
        { timestamp: 3, message: "d" },
      ],
    ]);
    expect(plan).toMatchObject({ truncated: 0, dropped: 0 });
    expect(planLogEventBatches([]).batches).toEqual([]);
  });

  it("splits at 10,000 events", () => {
    const events = Array.from({ length: 10_001 }, (_, i) => ({ timestamp: i, message: "x" }));
    const plan = planLogEventBatches(events);
    expect(plan.batches.map((b) => b.length)).toEqual([MAX_EVENTS_PER_BATCH, 1]);
  });

  it("splits at 1,048,576 bytes counting 26 bytes per event", () => {
    const message = "x".repeat(100_000 - EVENT_OVERHEAD_BYTES);
    const events = Array.from({ length: 11 }, (_, i) => ({ timestamp: i, message }));
    const plan = planLogEventBatches(events, { maxEventBytes: 200_000 });
    expect(plan.batches.map((b) => b.length)).toEqual([10, 1]);
    const exact = "y".repeat(MAX_BATCH_BYTES / 4 - EVENT_OVERHEAD_BYTES);
    const four = planLogEventBatches(
      Array.from({ length: 4 }, (_, i) => ({ timestamp: i, message: exact })),
    );
    expect(four.batches).toHaveLength(1);
  });

  it("splits when the span exceeds 24 hours", () => {
    const plan = planLogEventBatches([
      { timestamp: 0, message: "a" },
      { timestamp: MAX_BATCH_SPAN_MS, message: "b" },
      { timestamp: MAX_BATCH_SPAN_MS + 1, message: "c" },
    ]);
    expect(plan.batches.map((b) => b.map((e) => e.message))).toEqual([["a", "b"], ["c"]]);
  });

  it("applies the oversize policy", () => {
    const big = { timestamp: 1, message: "z".repeat(300_000) };
    const truncated = planLogEventBatches([big]);
    expect(truncated.truncated).toBe(1);
    expect(utf8Bytes(truncated.batches[0]?.[0]?.message ?? "")).toBeLessThanOrEqual(262_144);
    const dropped = planLogEventBatches([big, { timestamp: 2, message: "ok" }], {
      oversizePolicy: "drop",
    });
    expect(dropped).toMatchObject({ dropped: 1, batches: [[{ message: "ok" }]] });
    expect(() => planLogEventBatches([big], { oversizePolicy: "error" })).toThrow(/maxEventBytes/);
  });

  it("validates input", () => {
    expect(() => planLogEventBatches([{ timestamp: Number.NaN, message: "x" }])).toThrow(
      /invalid timestamp/,
    );
    expect(() => planLogEventBatches([{ timestamp: 1, message: 5 as never }])).toThrow(
      /string message/,
    );
    expect(() => planLogEventBatches([], { maxEventBytes: 0 })).toThrow(/maxEventBytes/);
  });
});

describe("putLogEvents", () => {
  it("sends sorted batches without sequence tokens and reads back", async () => {
    const { client, state, fake } = await setup();
    state.addGroup("g");
    state.put("g", "s", []);
    const now = Date.now();
    const res = await client.putLogEvents("g", "s", [
      { timestamp: now + 1, message: "second" },
      { timestamp: new Date(now), message: "first" },
    ]);
    expect(res).toEqual({
      accepted: 2,
      rejected: 0,
      truncated: 0,
      dropped: 0,
      batches: 1,
      rejectedInfo: [],
    });
    const sent = fake.requests.find((r) => r.target === "PutLogEvents")?.body;
    expect(sent).toEqual({
      logGroupName: "g",
      logStreamName: "s",
      logEvents: [
        { timestamp: now, message: "first" },
        { timestamp: now + 1, message: "second" },
      ],
    });
    expect(
      state
        .group("g")
        ?.streams.get("s")
        ?.map((e) => e.message),
    ).toEqual(["first", "second"]);
  });

  it("makes no call for an empty list", async () => {
    const { client, fake } = await setup();
    expect((await client.putLogEvents("g", "s", [])).batches).toBe(0);
    expect(fake.requests).toHaveLength(0);
  });

  it("creates a missing stream and retries once (autoCreate)", async () => {
    const { client, state, fake } = await setup();
    state.addGroup("g");
    const res = await client.putLogEvents("g", "new-stream", [
      { timestamp: Date.now(), message: "m" },
    ]);
    expect(res.accepted).toBe(1);
    expect(fake.ops()).toEqual(["PutLogEvents", "CreateLogStream", "PutLogEvents"]);
  });

  it("does not create a missing group unless createGroup; never auto-creates when disabled", async () => {
    const { client, state } = await setup();
    const ev = [{ timestamp: Date.now(), message: "m" }];
    await expect(client.putLogEvents("nogroup", "s", ev)).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
    expect((await client.putLogEvents("nogroup", "s", ev, { createGroup: true })).accepted).toBe(1);
    expect(state.group("nogroup")?.streams.get("s")).toHaveLength(1);
    await expect(
      client.putLogEvents("nogroup", "other", ev, { autoCreate: false }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  });

  it("propagates stream-creation errors other than NOT_FOUND", async () => {
    const { client } = await setup((req) =>
      req.target === "CreateLogStream"
        ? fixtureResponse("putLogEvents.access-denied")
        : awsError("ResourceNotFoundException"),
    );
    await expect(
      client.putLogEvents("g", "s", [{ timestamp: 1, message: "m" }]),
    ).rejects.toMatchObject({ code: "PERMISSION_DENIED" });
  });

  it("reports rejected events with the AWS exclusive end index (AWS fixture)", async () => {
    const fake = await startFakeCloudWatch(() => fixtureResponse("putLogEvents.rejected-aws"));
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    const now = Date.now();
    const res = await client.putLogEvents("g", "s", [
      { timestamp: now - 20 * 3_600_000, message: "old" },
      { timestamp: now, message: "ok" },
      { timestamp: now + 3 * 3_600_000, message: "new" },
    ]);
    expect(res).toMatchObject({
      accepted: 1,
      rejected: 2,
      rejectedInfo: [{ batch: 0, tooOldLogEventEndIndex: 1, tooNewLogEventStartIndex: 2 }],
    });
  });

  it("reads Moto's one-lower end index with the AWS rule (recorded Moto fixture)", async () => {
    const fake = await startFakeCloudWatch(() =>
      fixtureResponse("putLogEvents.rejected-too-old-too-new"),
    );
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    const now = Date.now();
    const res = await client.putLogEvents("g", "s", [
      { timestamp: now - 20 * 3_600_000, message: "old" },
      { timestamp: now, message: "ok" },
      { timestamp: now + 3 * 3_600_000, message: "new" },
    ]);
    expect(res).toMatchObject({
      accepted: 2,
      rejected: 1,
      rejectedInfo: [{ batch: 0, tooOldLogEventEndIndex: 0, tooNewLogEventStartIndex: 2 }],
    });
  });

  it("counts expired events", async () => {
    const { client } = await setup(() =>
      ok({ nextSequenceToken: "1", rejectedLogEventsInfo: { expiredLogEventEndIndex: 1 } }),
    );
    const res = await client.putLogEvents("g", "s", [
      { timestamp: 1, message: "a" },
      { timestamp: 2, message: "b" },
      { timestamp: 3, message: "c" },
    ]);
    expect(res).toMatchObject({ accepted: 2, rejected: 1 });
    expect(res.rejectedInfo[0]?.expiredLogEventEndIndex).toBe(1);
  });

  it("splits large inputs into several calls and maps service errors", async () => {
    const { client, fake } = await setup(() => ok({ nextSequenceToken: "1" }));
    const events = Array.from({ length: 10_050 }, (_, i) => ({ timestamp: i, message: "e" }));
    const res = await client.putLogEvents("g", "s", events);
    expect(res).toMatchObject({ accepted: 10_050, batches: 2 });
    expect(fake.requests).toHaveLength(2);
    const denied = await startFakeCloudWatch(() => fixtureResponse("putLogEvents.access-denied"));
    const c2 = testClient(denied);
    cleanups.push(
      () => denied.close(),
      () => c2.close(),
    );
    await expect(c2.putLogEvents("g", "s", [{ timestamp: 1, message: "x" }])).rejects.toMatchObject(
      {
        code: "PERMISSION_DENIED",
      },
    );
  });

  it("uses config maxEventBytes / oversizePolicy and validates names", async () => {
    const { client } = await setup(() => ok({}), { maxEventBytes: 100, oversizePolicy: "drop" });
    const res = await client.putLogEvents("g", "s", [
      { timestamp: 1, message: "x".repeat(101) },
      { timestamp: 2, message: "fits" },
    ]);
    expect(res).toMatchObject({ dropped: 1, accepted: 1 });
    await expect(client.putLogEvents("bad group", "s", [])).rejects.toMatchObject({
      code: "VALIDATION",
    });
    await expect(client.putLogEvents("g", "s*", [])).rejects.toMatchObject({ code: "VALIDATION" });
  });
});

describe("emf", () => {
  it("builds a valid EMF document", () => {
    const ts = new Date(1_795_033_642_000);
    const doc = buildEmf(
      "latency",
      42,
      { service: "api" },
      {
        unit: "Milliseconds",
        timestamp: ts,
        properties: { requestId: "r1" },
      },
    );
    expect(doc).toEqual({
      _aws: {
        Timestamp: ts.getTime(),
        CloudWatchMetrics: [
          {
            Namespace: DEFAULT_EMF_NAMESPACE,
            Dimensions: [["service"]],
            Metrics: [{ Name: "latency", Unit: "Milliseconds" }],
          },
        ],
      },
      requestId: "r1",
      service: "api",
      latency: 42,
    });
    expect(buildEmf("m", [1, 2], {}, { namespace: "N" })).toMatchObject({ m: [1, 2] });
  });

  it("validates metric, namespace, unit, values, dimensions and properties", () => {
    expect(() => buildEmf("", 1)).toThrow(/metric/);
    expect(() => buildEmf("m", 1, {}, { namespace: "" })).toThrow(/namespace/);
    expect(() => buildEmf("m", 1, {}, { unit: "Parsecs" })).toThrow(/unit/);
    expect(() => buildEmf("m", Number.NaN)).toThrow(/value/);
    expect(() => buildEmf("m", [])).toThrow(/value/);
    expect(() => buildEmf("m", new Array<number>(101).fill(1))).toThrow(/value/);
    const dims = Object.fromEntries(Array.from({ length: 31 }, (_, i) => [`d${i}`, "v"]));
    expect(() => buildEmf("m", 1, dims)).toThrow(/30 dimensions/);
    expect(() => buildEmf("m", 1, { m: "x" })).toThrow(/dimension/);
    expect(() => buildEmf("m", 1, {}, { properties: { _aws: "x" } })).toThrow(/reserved/);
    expect(() => buildEmf("m", 1, {}, { timestamp: new Date(Number.NaN) })).toThrow(/timestamp/);
  });

  it("client.emf sends one event to the configured group/stream", async () => {
    const { client, state } = await setup(undefined, { logGroup: "metrics", logStream: "emf" });
    state.addGroup("metrics");
    const doc = await client.emf("latency", 12, { service: "api" }, { unit: "Milliseconds" });
    const stored = state.group("metrics")?.streams.get("emf")?.[0]?.message;
    expect(JSON.parse(stored ?? "{}")).toEqual(doc);
    await expect(
      testClient(await startFakeCloudWatch(() => ok({})), {}).emf("m", 1),
    ).rejects.toMatchObject({ code: "CONFIG" });
  });

  it("client.emf raises when the event is rejected", async () => {
    const { client } = await setup(
      () => ok({ rejectedLogEventsInfo: { tooNewLogEventStartIndex: 0 } }),
      { logGroup: "g" },
    );
    await expect(client.emf("m", 1, {}, { stream: "s" })).rejects.toMatchObject({
      code: "VALIDATION",
    });
  });
});
