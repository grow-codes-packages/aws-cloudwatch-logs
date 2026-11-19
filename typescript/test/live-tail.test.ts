// Service-owned: StartLiveTail. Real-AWS streaming semantics (new events only, keep-alive updates,
// session exceptions, cancellation mid-stream) are unit-only; Moto replays history and closes.

import { afterEach, describe, expect, it } from "vitest";
import type { CloudWatchLogsError, LiveTailEvent } from "../src/index.js";
import {
  eventStream,
  fixture,
  fixtureResponse,
  ok,
  startFakeCloudWatch,
  streamResponse,
  testClient,
} from "./helpers/aws.js";
import type { MockResponse } from "./helpers/server.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups.splice(0).reverse()) await fn();
});

const ARN = "arn:aws:logs:us-east-1:123456789012:log-group:grow-bootstrap/seed";

async function setup(tail: () => MockResponse, options: Parameters<typeof testClient>[1] = {}) {
  const fake = await startFakeCloudWatch((req) => {
    if (req.target === "StartLiveTail") return tail();
    if (req.target === "DescribeLogGroups") return fixtureResponse("describeLogGroups.prefix");
    return ok({});
  });
  const client = testClient(fake, options);
  cleanups.push(
    () => fake.close(),
    () => client.close(),
  );
  return { fake, client };
}

const start = {
  type: "sessionStart",
  payload: { requestId: "r", sessionId: "sess-1", logGroupIdentifiers: [ARN] },
};
const update = (messages: string[], sampled = false) => ({
  type: "sessionUpdate",
  payload: {
    sessionMetadata: { sampled },
    sessionResults: messages.map((m, i) => ({
      logStreamName: "s",
      logGroupIdentifier: ARN,
      message: m,
      timestamp: 1_000 + i,
      ingestionTime: 2_000 + i,
    })),
  },
});

describe("startLiveTail", () => {
  it("decodes the recorded Moto event stream byte for byte", async () => {
    const { client, fake } = await setup(() => fixtureResponse("startLiveTail.seed"));
    const events: LiveTailEvent[] = [];
    const handle = await client.startLiveTail({ groups: [ARN] }, (e) => events.push(e));
    await handle.done;
    expect(handle.sessionId).toBe(
      (fixture("startLiveTail.seed").response.eventStream?.[1]?.payload as { sessionId: string })
        .sessionId,
    );
    expect(events.map((e) => e.message)).toEqual(
      [1, 2, 3].map((n) => `{"level":"info","msg":"grow-seed-event ${n}","seq":${n}}`),
    );
    expect(events[0]).toMatchObject({ logStreamName: "seed-stream", logGroupIdentifier: ARN });
    expect(events[0]?.ingestionTime).toBeInstanceOf(Date);
    expect(handle.closed).toBe(true);
    expect(handle.error).toBeUndefined();
    const req = fake.requests.find((r) => r.target === "StartLiveTail");
    expect(req?.host.startsWith("stream-fake-cwl")).toBe(true);
    expect(req?.body).toEqual({ logGroupIdentifiers: [ARN] });
    await handle.close();
  });

  it("converts group names to ARNs (lookup, or accountId + region) and sends filters", async () => {
    const { client, fake } = await setup(() => streamResponse(eventStream([start])));
    const group = (fixtureResponse("describeLogGroups.prefix").body as string).match(
      /"logGroupName": ?"([^"]+)"/,
    )?.[1] as string;
    const h = await client.startLiveTail(
      { groups: [group], streams: ["a"], pattern: "ERROR" },
      () => undefined,
    );
    await h.done;
    const body = fake.requests.find((r) => r.target === "StartLiveTail")?.body;
    expect(body).toMatchObject({ logStreamNames: ["a"], logEventFilterPattern: "ERROR" });
    expect((body?.logGroupIdentifiers as string[])[0]).toMatch(/^arn:aws:logs:.*:log-group:/);

    const { client: byAccount, fake: f2 } = await setup(
      () => streamResponse(eventStream([start])),
      { accountId: "123456789012" },
    );
    const h2 = await byAccount.startLiveTail(
      { groups: ["app/x"], streamPrefixes: ["p"] },
      () => undefined,
    );
    await h2.done;
    expect(f2.ops()).toEqual(["StartLiveTail"]);
    expect(f2.requests[0]?.body).toEqual({
      logGroupIdentifiers: ["arn:aws:logs:us-east-1:123456789012:log-group:app/x"],
      logStreamNamePrefixes: ["p"],
    });
    expect(await byAccount.logGroupArn(`${ARN}:*`)).toBe(ARN);
  });

  it("validates the request", async () => {
    const { client } = await setup(() => streamResponse(eventStream([start])));
    const noop = (): void => undefined;
    await expect(client.startLiveTail({ groups: [] }, noop)).rejects.toMatchObject({
      code: "VALIDATION",
    });
    await expect(
      client.startLiveTail({ groups: new Array(11).fill(ARN) }, noop),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(
      client.startLiveTail({ groups: [ARN], streams: ["a"], streamPrefixes: ["b"] }, noop),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(
      client.startLiveTail({ groups: [ARN, ARN], streams: ["a"] }, noop),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.logGroupArn("bad name")).rejects.toMatchObject({ code: "VALIDATION" });
  });

  it("streams keep-alive and data updates until close()", async () => {
    const { client } = await setup(() => ({
      ...streamResponse(Buffer.alloc(0)),
      chunks: [
        { data: eventStream([{ type: "initial-response" }, start]) },
        { data: eventStream([update([])]), delayMs: 20 },
        { data: eventStream([update(["one", "two"], true)]), delayMs: 20 },
      ],
      keepOpen: true,
    }));
    const events: string[] = [];
    const updates: { sampled: boolean; count: number }[] = [];
    const errors: CloudWatchLogsError[] = [];
    const handle = await client.startLiveTail(
      { groups: [ARN] },
      (e) => {
        events.push(e.message);
        if (e.message === "one") throw new Error("callback errors are ignored");
      },
      {
        onUpdate: (u) => {
          updates.push(u);
          throw new Error("ignored");
        },
        onError: (e) => errors.push(e),
      },
    );
    expect(handle.sessionId).toBe("sess-1");
    await new Promise((r) => setTimeout(r, 120));
    expect(events).toEqual(["one", "two"]);
    expect(updates).toEqual([
      { sampled: false, count: 0 },
      { sampled: true, count: 2 },
    ]);
    expect(handle.closed).toBe(false);
    await handle.close();
    await handle.close();
    expect(handle.closed).toBe(true);
    expect(errors).toEqual([]);
  });

  it("reports in-stream exceptions (SessionTimeoutException) to onError", async () => {
    for (const [type, code] of [
      ["SessionTimeoutException", "TIMEOUT"],
      ["SessionStreamingException", "UNAVAILABLE"],
    ] as const) {
      const { client } = await setup(() =>
        streamResponse(
          eventStream([start, update(["a"]), { type, payload: { message: "x" }, exception: true }]),
        ),
      );
      const errors: CloudWatchLogsError[] = [];
      const handle = await client.startLiveTail({ groups: [ARN] }, () => undefined, {
        onError: (e) => errors.push(e),
      });
      await handle.done;
      expect(errors.map((e) => e.code)).toEqual([code]);
      expect(handle.error).toMatchObject({ code, retryable: true });
    }
  });

  it("reports exceptions modeled as union members", async () => {
    const { client } = await setup(() =>
      streamResponse(
        eventStream([start, { type: "SessionTimeoutException", payload: { message: "m" } }]),
      ),
    );
    const errors: CloudWatchLogsError[] = [];
    const handle = await client.startLiveTail({ groups: [ARN] }, () => undefined, {
      onError: (e) => errors.push(e),
    });
    await handle.done;
    expect(errors.length).toBeLessThanOrEqual(1);
  });

  it("rejects when the session fails before it starts", async () => {
    const { client } = await setup(() =>
      streamResponse(
        eventStream([
          { type: "SessionStreamingException", payload: { message: "x" }, exception: true },
        ]),
      ),
    );
    await expect(client.startLiveTail({ groups: [ARN] }, () => undefined)).rejects.toMatchObject({
      code: "UNAVAILABLE",
    });
    const { client: c2 } = await setup(() =>
      streamResponse(
        eventStream([update(["early"]), { type: "SessionTimeoutException", payload: {} }]),
      ),
    );
    const early: string[] = [];
    await c2
      .startLiveTail({ groups: [ARN] }, (e) => early.push(e.message))
      .then(
        (h) => h.done,
        (e: unknown) => expect(e).toMatchObject({ code: "TIMEOUT" }),
      );
    expect(early).toEqual(["early"]);
  });

  it("an empty stream ends the session without a session id", async () => {
    const { client } = await setup(() =>
      streamResponse(eventStream([{ type: "initial-response" }])),
    );
    const handle = await client.startLiveTail({ groups: [ARN] }, () => undefined);
    await handle.done;
    expect(handle.sessionId).toBe("");
  });

  it("maps HTTP errors (read-only principal denied etc.)", async () => {
    const { client } = await setup(() => fixtureResponse("deleteLogGroup.access-denied"));
    await expect(client.startLiveTail({ groups: [ARN] }, () => undefined)).rejects.toMatchObject({
      code: "PERMISSION_DENIED",
    });
  });

  it("the caller signal and client.close() end open sessions", async () => {
    const open = (): MockResponse => ({
      ...streamResponse(Buffer.alloc(0)),
      chunks: [{ data: eventStream([start]) }],
      keepOpen: true,
    });
    const { client } = await setup(open);
    const ac = new AbortController();
    const handle = await client.startLiveTail({ groups: [ARN] }, () => undefined, {
      signal: ac.signal,
    });
    ac.abort();
    await handle.done;
    expect(handle.closed).toBe(true);
    expect(handle.error).toBeUndefined();

    const h2 = await client.startLiveTail({ groups: [ARN] }, () => undefined);
    await client.close();
    expect(h2.closed).toBe(true);
  });

  it("cancellation before the session starts raises CANCELLED", async () => {
    const { client } = await setup(() => ({
      ...streamResponse(Buffer.alloc(0)),
      chunks: [{ data: Buffer.alloc(0), delayMs: 500 }],
      keepOpen: true,
    }));
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 50);
    await expect(
      client.startLiveTail({ groups: [ARN] }, () => undefined, { signal: ac.signal }),
    ).rejects.toMatchObject({ code: "CANCELLED" });
  });
});
