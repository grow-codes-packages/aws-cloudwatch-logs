// Service-owned: filterLogEvents, getLogEvents and Logs Insights (startQuery, getQueryResults,
// query, stopQuery, describeQueries). Pending / failure paths are unit-only (Moto completes at once).

import { afterEach, describe, expect, it } from "vitest";
import { parseInsightsTimestamp, partitionForRegion } from "../src/index.js";
import {
  awsError,
  fixture,
  fixtureResponse,
  ok,
  startFakeCloudWatch,
  testClient,
} from "./helpers/aws.js";
import { startFakeLogs } from "./helpers/fake-logs.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups.splice(0).reverse()) await fn();
});

async function fakeWith(
  handler: Parameters<typeof startFakeCloudWatch>[0],
  options: Parameters<typeof testClient>[1] = {},
) {
  const fake = await startFakeCloudWatch(handler);
  const sleeps: number[] = [];
  const client = testClient(fake, options, {
    sleep: async (ms) => {
      sleeps.push(ms);
    },
  });
  cleanups.push(
    () => fake.close(),
    () => client.close(),
  );
  return { fake, client, sleeps };
}

async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of it) out.push(x);
  return out;
}

describe("filterLogEvents", () => {
  it("iterates pages until no token (empty pages included) and sorts each page", async () => {
    const pages = [
      {
        events: [
          { eventId: "2", timestamp: 20, message: "b", logStreamName: "s2", ingestionTime: 21 },
          { eventId: "1", timestamp: 10, message: "a", logStreamName: "s1" },
        ],
        nextToken: "t1",
      },
      { events: [], nextToken: "t2" },
      { events: [{ eventId: "3", timestamp: 30, message: "c" }], nextToken: null },
    ];
    const { client, fake } = await fakeWith((_r, i) => ok(pages[i]));
    const events = await collect(
      client.filterLogEvents({
        group: "g",
        pattern: "x",
        start: new Date(5),
        end: 100,
        streams: ["s1", "s2"],
        pageSize: 2,
      }),
    );
    expect(events.map((e) => e.message)).toEqual(["a", "b", "c"]);
    expect(events[1]).toMatchObject({ eventId: "2", logStreamName: "s2" });
    expect(events[1]?.ingestionTime?.getTime()).toBe(21);
    expect(fake.requests[0]?.body).toEqual({
      logGroupName: "g",
      logStreamNames: ["s1", "s2"],
      filterPattern: "x",
      startTime: 5,
      endTime: 100,
      limit: 2,
    });
    expect(fake.requests.map((r) => r.body.nextToken)).toEqual([undefined, "t1", "t2"]);
  });

  it("stops at limit, accepts ARNs, streamPrefix and the recorded fixtures", async () => {
    const { client, fake } = await fakeWith((req) =>
      fixtureResponse(
        req.body.nextToken === undefined ? "filterLogEvents.page1" : "filterLogEvents.page2",
      ),
    );
    const two = await collect(
      client.filterLogEvents({
        group: "arn:aws:logs:us-east-1:1:log-group:g:*",
        limit: 2,
        streamPrefix: "s",
      }),
    );
    expect(two).toHaveLength(2);
    expect(fake.requests[0]?.body).toMatchObject({
      logGroupIdentifier: "arn:aws:logs:us-east-1:1:log-group:g",
      logStreamNamePrefix: "s",
      limit: 2,
    });
    const all = await collect(client.filterLogEvents({ group: "g" }));
    expect(all.length).toBeGreaterThan(2);
  });

  it("validates options and needs a group", async () => {
    const { client } = await fakeWith(() => ok({ events: [] }));
    for (const opts of [
      { group: "g", streams: ["a"], streamPrefix: "a" },
      { group: "g", streams: [] },
      { group: "g", pageSize: 0 },
      { group: "g", limit: 0 },
      { group: "g", start: Number.NaN },
    ]) {
      await expect(collect(client.filterLogEvents(opts))).rejects.toMatchObject({
        code: "VALIDATION",
      });
    }
    await expect(collect(client.filterLogEvents())).rejects.toMatchObject({ code: "CONFIG" });
  });

  it("maps a missing group (fixture) to NOT_FOUND", async () => {
    const { client } = await fakeWith(() => fixtureResponse("filterLogEvents.not-found"));
    await expect(collect(client.filterLogEvents({ group: "g" }))).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
  });
});

describe("getLogEvents", () => {
  it("forward: follows nextForwardToken until the token repeats (recorded fixtures)", async () => {
    const forward = fixture("getLogEvents.forward");
    const end = fixture("getLogEvents.end-of-stream");
    const { client, fake } = await fakeWith((_r, i) =>
      fixtureResponse(i === 0 ? "getLogEvents.forward" : "getLogEvents.end-of-stream"),
    );
    const events = await collect(
      client.getLogEvents("g", "s", {
        startFromHead: true,
        pageSize: 2,
        start: 1,
        end: new Date(9e12),
      }),
    );
    expect(events.length).toBe(
      (forward.response.body as { events: unknown[] }).events.length +
        (end.response.body as { events: unknown[] }).events.length,
    );
    expect(fake.requests[0]?.body).toMatchObject({
      startFromHead: true,
      limit: 2,
      startTime: 1,
      logGroupName: "g",
      logStreamName: "s",
    });
  });

  it("forward stops when the returned token equals the sent token", async () => {
    const { client, fake } = await fakeWith((_r, i) =>
      ok(
        i === 0
          ? {
              events: [{ timestamp: 1, message: "a", ingestionTime: 2 }],
              nextForwardToken: "f/1",
              nextBackwardToken: "b/0",
            }
          : {
              events: [{ timestamp: 2, message: "b" }],
              nextForwardToken: "f/1",
              nextBackwardToken: "b/0",
            },
      ),
    );
    const events = await collect(client.getLogEvents("g", "s", { startFromHead: true }));
    expect(events.map((e) => e.message)).toEqual(["a", "b"]);
    expect(events[0]?.ingestionTime?.getTime()).toBe(2);
    expect(fake.requests).toHaveLength(2);
  });

  it("backward (default): newest page first, follows nextBackwardToken, honours limit", async () => {
    const { client, fake } = await fakeWith((_r, i) =>
      ok(
        i === 0
          ? {
              events: [
                { timestamp: 3, message: "c" },
                { timestamp: 4, message: "d" },
              ],
              nextForwardToken: "f/9",
              nextBackwardToken: "b/2",
            }
          : {
              events: [
                { timestamp: 1, message: "a" },
                { timestamp: 2, message: "b" },
              ],
              nextForwardToken: "f/9",
              nextBackwardToken: "b/0",
            },
      ),
    );
    expect((await collect(client.getLogEvents("g", "s"))).map((e) => e.message)).toEqual([
      "c",
      "d",
      "a",
      "b",
      "a",
      "b",
    ]);
    expect(fake.requests[1]?.body.nextToken).toBe("b/2");
    expect(await collect(client.getLogEvents("g", "s", { limit: 1 }))).toHaveLength(1);
  });

  it("validates and maps missing streams", async () => {
    const { client } = await fakeWith(() => fixtureResponse("getLogEvents.stream-not-found"));
    await expect(collect(client.getLogEvents("g", "s"))).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
    await expect(
      collect(client.getLogEvents("g", "s", { pageSize: 10_001 })),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(
      collect(client.getLogEvents("g", "s", { end: Number.POSITIVE_INFINITY })),
    ).rejects.toMatchObject({ code: "VALIDATION" });
  });
});

describe("Logs Insights", () => {
  it("startQuery sends names and epoch seconds (floor start, ceil end)", async () => {
    const { client, fake } = await fakeWith(() => fixtureResponse("startQuery.ok"));
    const id = await client.startQuery("fields @message", {
      groups: ["g1", "g2"],
      start: 1_500,
      end: new Date(10_001),
      limit: 5,
    });
    expect(id).toBe((fixture("startQuery.ok").response.body as { queryId: string }).queryId);
    expect(fake.requests[0]?.body).toEqual({
      logGroupNames: ["g1", "g2"],
      queryString: "fields @message",
      startTime: 1,
      endTime: 11,
      limit: 5,
    });
  });

  it("startQuery defaults to config.logGroup and the last hour; ARNs use logGroupIdentifiers", async () => {
    const { client, fake } = await fakeWith(() => ok({ queryId: "q" }), { logGroup: "cfg" });
    await client.startQuery("fields @message");
    const body = fake.requests[0]?.body as {
      logGroupNames: string[];
      startTime: number;
      endTime: number;
    };
    expect(body.logGroupNames).toEqual(["cfg"]);
    expect(body.endTime - body.startTime).toBeGreaterThanOrEqual(3600);
    await client.startQuery("x", { groups: ["arn:aws:logs:us-east-1:1:log-group:g:*"] });
    expect(fake.requests[1]?.body.logGroupIdentifiers).toEqual([
      "arn:aws:logs:us-east-1:1:log-group:g",
    ]);
  });

  it("startQuery validates and maps errors", async () => {
    const { client } = await fakeWith(() => fixtureResponse("startQuery.not-found"), {
      logGroup: "g",
    });
    await expect(client.startQuery(" ")).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.startQuery("x", { groups: [] })).rejects.toMatchObject({
      code: "VALIDATION",
    });
    await expect(client.startQuery("x", { start: 10, end: 5 })).rejects.toMatchObject({
      code: "VALIDATION",
    });
    await expect(client.startQuery("x", { limit: 0 })).rejects.toMatchObject({
      code: "VALIDATION",
    });
    await expect(client.startQuery("x")).rejects.toMatchObject({ code: "NOT_FOUND" });
    const { client: none } = await fakeWith(() => ok({}), { logGroup: "g" });
    await expect(none.startQuery("x")).rejects.toMatchObject({ code: "UNAVAILABLE" });
  });

  it("getQueryResults turns rows into maps and keeps statistics (AWS shape)", async () => {
    const { client } = await fakeWith((_r, i) =>
      i === 0
        ? fixtureResponse("getQueryResults.complete")
        : ok({
            status: "Running",
            results: [
              [
                { field: "@timestamp", value: "2026-11-18 10:00:00.123" },
                { field: "@message" },
                { value: "orphan" },
              ],
            ],
            statistics: { recordsMatched: 1, recordsScanned: 2, bytesScanned: 3 },
          }),
    );
    const done = await client.getQueryResults("q1");
    expect(done.status).toBe("Complete");
    expect(done.rows[0]?.["@message"]).toBeDefined();
    expect(done.statistics).toBeUndefined();
    const running = await client.getQueryResults("q1");
    expect(running).toEqual({
      queryId: "q1",
      status: "Running",
      rows: [{ "@timestamp": "2026-11-18 10:00:00.123", "@message": "" }],
      statistics: { recordsMatched: 1, recordsScanned: 2, bytesScanned: 3 },
    });
    await expect(client.getQueryResults("")).rejects.toMatchObject({ code: "VALIDATION" });
    const { client: empty } = await fakeWith(() => ok({ statistics: {} }));
    expect(await empty.getQueryResults("q")).toMatchObject({
      status: "Unknown",
      rows: [],
      statistics: { recordsMatched: 0 },
    });
  });

  it("parses both @timestamp formats", () => {
    expect(parseInsightsTimestamp("1795033642000")?.getTime()).toBe(1795033642000);
    expect(parseInsightsTimestamp("2026-11-18 10:00:00.123")?.toISOString()).toBe(
      "2026-11-18T10:00:00.123Z",
    );
    expect(parseInsightsTimestamp("2026-11-18T10:00:00Z")?.toISOString()).toBe(
      "2026-11-18T10:00:00.000Z",
    );
    expect(parseInsightsTimestamp("nope")).toBeUndefined();
    expect(parseInsightsTimestamp("2026-13-45 99:00:00")).toBeUndefined();
  });

  it("query polls with backoff until Complete", async () => {
    const statuses = ["Scheduled", "Running", "Running", "Complete"];
    let polls = 0;
    const { client, sleeps } = await fakeWith(
      (req) =>
        req.target === "StartQuery"
          ? ok({ queryId: "q9" })
          : ok({ status: statuses[polls++], results: [] }),
      { logGroup: "g" },
    );
    const res = await client.query("fields @message", { pollIntervalMs: 1000 });
    expect(res).toMatchObject({ queryId: "q9", status: "Complete" });
    expect(sleeps).toEqual([1000, 2000, 4000]);
  });

  it("query maps terminal failure statuses", async () => {
    for (const [status, code] of [
      ["Failed", "UNAVAILABLE"],
      ["Unknown", "UNAVAILABLE"],
      ["Timeout", "TIMEOUT"],
      ["Cancelled", "CANCELLED"],
    ] as const) {
      const { client } = await fakeWith(
        (req) => (req.target === "StartQuery" ? ok({ queryId: "qf" }) : ok({ status })),
        { logGroup: "g" },
      );
      const err = await client.query("x").then(
        () => new Error("resolved"),
        (e: unknown) => e as Error,
      );
      expect(err).toMatchObject({ code });
      expect(err.message).toContain("qf");
    }
  });

  it("query retries StartQuery on LimitExceededException, then gives up", async () => {
    let starts = 0;
    const { client, sleeps } = await fakeWith(
      (req) => {
        if (req.target === "StartQuery")
          return ++starts < 3 ? awsError("LimitExceededException") : ok({ queryId: "q" });
        return ok({ status: "Complete", results: [] });
      },
      { logGroup: "g" },
    );
    expect((await client.query("x")).status).toBe("Complete");
    expect(sleeps).toEqual([1000, 2000]);
    const { client: always } = await fakeWith(() => awsError("LimitExceededException"), {
      logGroup: "g",
    });
    await expect(always.query("x", { retryOnQueryLimit: 2 })).rejects.toMatchObject({
      code: "VALIDATION",
    });
    const { client: other } = await fakeWith(() => awsError("MalformedQueryException"), {
      logGroup: "g",
    });
    await expect(other.query("x")).rejects.toMatchObject({ code: "VALIDATION" });
    const { client: noId } = await fakeWith(() => ok({}), { logGroup: "g" });
    await expect(noId.query("x")).rejects.toMatchObject({ code: "UNAVAILABLE" });
  });

  it("query stops the query best-effort on its deadline and on cancellation", async () => {
    const seen: string[] = [];
    const { client } = await fakeWith(
      (req) => {
        seen.push(req.target);
        if (req.target === "StartQuery") return ok({ queryId: "qs" });
        if (req.target === "StopQuery") return fixtureResponse("stopQuery.moto-500");
        return ok({ status: "Running" });
      },
      { logGroup: "g" },
    );
    await expect(client.query("x", { timeoutMs: 1 })).rejects.toMatchObject({ code: "TIMEOUT" });
    expect(seen).toContain("StopQuery");

    seen.length = 0;
    const fake = await startFakeCloudWatch((req) => {
      seen.push(req.target);
      if (req.target === "StartQuery") return ok({ queryId: "qc" });
      if (req.target === "StopQuery") return ok({ success: true });
      return ok({ status: "Running" });
    });
    const ac = new AbortController();
    const real = testClient(fake, { logGroup: "g" });
    cleanups.push(
      () => fake.close(),
      () => real.close(),
    );
    setTimeout(() => ac.abort(), 50);
    await expect(real.query("x", { signal: ac.signal, pollIntervalMs: 20 })).rejects.toMatchObject({
      code: "CANCELLED",
    });
    expect(seen).toContain("StopQuery");
  });

  it("stopQuery returns success and is unit-only (Moto 500 maps to UNAVAILABLE)", async () => {
    const { client } = await fakeWith((_r, i) =>
      i === 0
        ? ok({ success: true })
        : i === 1
          ? ok({ success: false })
          : fixtureResponse("stopQuery.moto-500"),
    );
    expect(await client.stopQuery("q")).toBe(true);
    expect(await client.stopQuery("q")).toBe(false);
    await expect(client.stopQuery("q")).rejects.toMatchObject({ code: "UNAVAILABLE" });
    await expect(client.stopQuery("")).rejects.toMatchObject({ code: "VALIDATION" });
  });

  it("describeQueries maps the recorded fixture (float createTime)", async () => {
    const { client, fake } = await fakeWith((_r, i) =>
      i === 0 ? fixtureResponse("describeQueries.ok") : ok({ queries: [{}] }),
    );
    const queries = await client.describeQueries({
      group: "g",
      status: "Complete",
      maxResults: 10,
    });
    expect(queries.length).toBeGreaterThan(0);
    expect(queries[0]?.createTime).toBeInstanceOf(Date);
    expect(queries[0]?.status).toBe("Complete");
    expect(fake.requests[0]?.body).toEqual({
      logGroupName: "g",
      status: "Complete",
      maxResults: 10,
    });
    expect(await client.describeQueries()).toEqual([
      { queryId: "", queryString: "", status: "Unknown" },
    ]);
    await expect(client.describeQueries({ maxResults: 0 })).rejects.toMatchObject({
      code: "VALIDATION",
    });
  });

  it("works against the stateful fake end to end", async () => {
    const fake = await startFakeLogs();
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    fake.state.addGroup("g");
    const res = await client.query("fields @message", { groups: ["g"] });
    expect(res.rows[0]?.["@message"]).toBe("hello");
    expect((await client.describeQueries())[0]?.queryString).toBe("fields @message");
  });

  it("partitionForRegion", () => {
    expect(partitionForRegion("cn-north-1")).toBe("aws-cn");
    expect(partitionForRegion("us-gov-west-1")).toBe("aws-us-gov");
    expect(partitionForRegion("eu-west-1")).toBe("aws");
  });
});
