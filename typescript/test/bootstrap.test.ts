// Service-owned: bootstrap functions (groups, streams, retention, tags, describe iterators).

import { afterEach, describe, expect, it } from "vitest";
import { CloudWatchLogsClient, type LogGroupInfo, RETENTION_DAYS } from "../src/index.js";
import { awsError, fixtureResponse, ok, startFakeCloudWatch, testClient } from "./helpers/aws.js";
import { startFakeLogs } from "./helpers/fake-logs.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups.splice(0).reverse()) await fn();
});

async function setup(override?: Parameters<typeof startFakeLogs>[0]) {
  const fake = await startFakeLogs(override);
  const client = testClient(fake);
  cleanups.push(
    () => fake.close(),
    () => client.close(),
  );
  return { fake, client, state: fake.state };
}

async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of it) out.push(x);
  return out;
}

describe("ensureLogGroup", () => {
  it("creates with tags, KMS key and retention, then reconciles an existing group", async () => {
    const { client, state, fake } = await setup();
    const first = await client.ensureLogGroup("app/one", {
      retentionDays: 7,
      kmsKeyId: "arn:aws:kms:us-east-1:123456789012:key/k1",
      tags: { team: "sdk" },
    });
    expect(first.created).toBe(true);
    expect(first.arn).toBe("arn:aws:logs:us-east-1:123456789012:log-group:app/one");
    expect(state.group("app/one")).toMatchObject({ retentionInDays: 7, tags: { team: "sdk" } });
    expect(fake.ops()).toEqual(["CreateLogGroup", "DescribeLogGroups", "PutRetentionPolicy"]);

    const again = await client.ensureLogGroup("app/one", {
      retentionDays: 14,
      kmsKeyId: "k1",
      tags: { env: "test" },
    });
    expect(again.created).toBe(false);
    expect(state.group("app/one")?.tags).toEqual({ team: "sdk", env: "test" });
    expect(state.group("app/one")?.retentionInDays).toBe(14);
    await expect(client.ensureLogGroup("app/one", { kmsKeyId: "other" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
    // null retention = never expire
    await client.ensureLogGroup("app/one", { retentionDays: null });
    expect(state.group("app/one")?.retentionInDays).toBeUndefined();
    // existing, no options: only create + describe
    expect((await client.ensureLogGroup("app/one")).created).toBe(false);
  });

  it("validates names and retention client-side (Moto accepts both)", async () => {
    const { client, fake } = await setup();
    await expect(client.ensureLogGroup("bad name!")).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.ensureLogGroup("g", { retentionDays: 2 })).rejects.toMatchObject({
      code: "VALIDATION",
    });
    expect(fake.requests).toHaveLength(0);
    expect(RETENTION_DAYS).toContain(3653);
  });

  it("maps access denied (fixture) and propagates other errors", async () => {
    const fake = await startFakeCloudWatch(() => fixtureResponse("ensureLogGroup.access-denied"));
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    await expect(client.ensureLogGroup("g")).rejects.toMatchObject({ code: "PERMISSION_DENIED" });
  });

  it("raises NOT_FOUND when the group vanishes after creation", async () => {
    const { client } = await setup((req) =>
      req.target === "DescribeLogGroups" ? ok({ logGroups: [] }) : undefined,
    );
    await expect(client.ensureLogGroup("g")).rejects.toMatchObject({ code: "NOT_FOUND" });
  });

  it("replays the recorded create / exists fixtures", async () => {
    const fake = await startFakeCloudWatch((req, i) => {
      if (req.target === "CreateLogGroup")
        return fixtureResponse(i === 0 ? "ensureLogGroup.create" : "ensureLogGroup.exists");
      return fixtureResponse("describeLogGroups.prefix");
    });
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    const name = (fixtureResponse("describeLogGroups.prefix").body as string).match(
      /"logGroupName": ?"([^"]+)"/,
    )?.[1] as string;
    expect((await client.ensureLogGroup(name)).created).toBe(true);
    expect((await client.ensureLogGroup(name)).created).toBe(false);
  });
});

describe("ensureLogStream / delete / retention", () => {
  it("creates once, reports existing, NOT_FOUND for a missing group unless createGroup", async () => {
    const { client, state } = await setup();
    await expect(client.ensureLogStream("missing", "s")).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
    expect(await client.ensureLogStream("auto", "s", { createGroup: true })).toEqual({
      created: true,
    });
    expect(await client.ensureLogStream("auto", "s")).toEqual({ created: false });
    expect(state.group("auto")?.streams.has("s")).toBe(true);
    await expect(client.ensureLogStream("auto", "bad:stream")).rejects.toMatchObject({
      code: "VALIDATION",
    });
  });

  it("propagates unexpected stream errors", async () => {
    const { client } = await setup(() => awsError("ServiceUnavailableException", 503));
    await expect(client.ensureLogStream("g", "s")).rejects.toMatchObject({ code: "UNAVAILABLE" });
  });

  it("putRetentionPolicy sets, deletes and validates", async () => {
    const { client, state } = await setup();
    state.addGroup("g");
    await client.putRetentionPolicy("g", 30);
    expect(state.group("g")?.retentionInDays).toBe(30);
    await client.putRetentionPolicy("g", 0);
    expect(state.group("g")?.retentionInDays).toBeUndefined();
    await client.putRetentionPolicy("g", 7);
    await client.putRetentionPolicy("g", null);
    expect(state.group("g")?.retentionInDays).toBeUndefined();
    await expect(client.putRetentionPolicy("g", 8)).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.putRetentionPolicy("nope", 7)).rejects.toMatchObject({ code: "NOT_FOUND" });
  });

  it("deleteLogGroup / deleteLogStream with ignoreMissing", async () => {
    const { client, state } = await setup();
    state.addGroup("g");
    state.put("g", "s", []);
    expect(await client.deleteLogStream("g", "s")).toBe(true);
    await expect(client.deleteLogStream("g", "s")).rejects.toMatchObject({ code: "NOT_FOUND" });
    expect(await client.deleteLogStream("g", "s", { ignoreMissing: true })).toBe(false);
    expect(await client.deleteLogGroup("g")).toBe(true);
    await expect(client.deleteLogGroup("g")).rejects.toMatchObject({ code: "NOT_FOUND" });
    expect(await client.deleteLogGroup("g", { ignoreMissing: true })).toBe(false);
  });

  it("delete functions propagate other errors even with ignoreMissing", async () => {
    const fake = await startFakeCloudWatch(() => fixtureResponse("deleteLogGroup.access-denied"));
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    await expect(client.deleteLogGroup("g", { ignoreMissing: true })).rejects.toMatchObject({
      code: "PERMISSION_DENIED",
    });
    await expect(client.deleteLogStream("g", "s", { ignoreMissing: true })).rejects.toMatchObject({
      code: "PERMISSION_DENIED",
    });
  });
});

describe("describe iterators and tags", () => {
  it("describeLogGroups pages, filters by prefix and honours limit", async () => {
    const { client, state, fake } = await setup();
    for (let i = 0; i < 5; i++) state.addGroup(`p/${i}`, { retentionInDays: 7, kmsKeyId: "k" });
    state.addGroup("other");
    const all = await collect(client.describeLogGroups({ prefix: "p/", pageSize: 2 }));
    expect(all.map((g) => g.name)).toEqual(["p/0", "p/1", "p/2", "p/3", "p/4"]);
    expect(all[0]).toMatchObject<Partial<LogGroupInfo>>({
      arn: "arn:aws:logs:us-east-1:123456789012:log-group:p/0",
      retentionDays: 7,
      kmsKeyId: "k",
      storedBytes: 0,
      metricFilterCount: 0,
    });
    expect(fake.requests.filter((r) => r.target === "DescribeLogGroups")).toHaveLength(3);
    expect(await collect(client.describeLogGroups({ limit: 2 }))).toHaveLength(2);
    await expect(collect(client.describeLogGroups({ pageSize: 51 }))).rejects.toMatchObject({
      code: "VALIDATION",
    });
  });

  it("describeLogGroups parses the recorded pages", async () => {
    const fake = await startFakeCloudWatch((req) =>
      fixtureResponse(
        req.body.nextToken === undefined ? "describeLogGroups.page1" : "describeLogGroups.page2",
      ),
    );
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    const groups = await collect(client.describeLogGroups());
    expect(groups.length).toBeGreaterThanOrEqual(2);
    expect(groups.every((g) => !g.arn.endsWith(":*"))).toBe(true);
    expect(groups[0]?.creationTime).toBeInstanceOf(Date);
  });

  it("describeLogStreams pages, orders and validates", async () => {
    const { client, state } = await setup();
    state.addGroup("g");
    for (const s of ["b", "a", "c"]) state.put("g", s, []);
    const names = (await collect(client.describeLogStreams("g", { pageSize: 2 }))).map(
      (s) => s.name,
    );
    expect(names).toEqual(["a", "b", "c"]);
    expect(await collect(client.describeLogStreams("g", { limit: 1, prefix: "c" }))).toHaveLength(
      1,
    );
    await expect(
      collect(client.describeLogStreams("g", { orderBy: "LastEventTime", prefix: "a" })),
    ).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(collect(client.describeLogStreams("missing"))).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
  });

  it("describeLogStreams parses the recorded fixture with event times", async () => {
    const fake = await startFakeCloudWatch(() =>
      fixtureResponse("describeLogStreams.last-event-desc"),
    );
    const client = testClient(fake);
    cleanups.push(
      () => fake.close(),
      () => client.close(),
    );
    const streams = await collect(
      client.describeLogStreams("g", { orderBy: "LastEventTime", descending: true, limit: 2 }),
    );
    expect(streams[0]?.arn).toContain("log-stream");
    expect(streams[0]?.lastEventTime).toBeInstanceOf(Date);
    expect(streams[0]?.firstEventTime).toBeInstanceOf(Date);
    expect(fake.requests[0]?.body).toMatchObject({ orderBy: "LastEventTime", descending: true });
  });

  it("tagLogGroup / untagLogGroup / listLogGroupTags use the ARN without :*", async () => {
    const { client, state, fake } = await setup();
    state.addGroup("g", { tags: { a: "1" } });
    await client.tagLogGroup("g", { b: "2" });
    expect(await client.listLogGroupTags("g")).toEqual({ a: "1", b: "2" });
    await client.untagLogGroup("g", ["a"]);
    expect(await client.listLogGroupTags("g")).toEqual({ b: "2" });
    expect(fake.requests.find((r) => r.target === "TagResource")?.body.resourceArn).toBe(
      "arn:aws:logs:us-east-1:123456789012:log-group:g",
    );
    await expect(client.listLogGroupTags("missing")).rejects.toMatchObject({ code: "NOT_FOUND" });
    expect(await client.getLogGroup("missing")).toBeUndefined();
  });

  it("getLogGroup pages until the exact name", async () => {
    const { client, state } = await setup();
    for (let i = 0; i < 60; i++) state.addGroup(`g${String(i).padStart(2, "0")}`);
    state.addGroup("g");
    expect((await client.getLogGroup("g59"))?.name).toBe("g59");
  });

  it("is a CloudWatchLogsClient", async () => {
    const { client } = await setup();
    expect(client).toBeInstanceOf(CloudWatchLogsClient);
  });
});
