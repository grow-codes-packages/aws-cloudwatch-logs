// Stateful in-memory CloudWatch Logs for unit tests (Moto-like semantics where it matters:
// quoted-phrase / term filter patterns, nextToken paging, ResourceNotFound / AlreadyExists).

import type { MockResponse } from "./server.js";
import { type AwsHandler, type AwsRequest, awsError, ok, startFakeCloudWatch } from "./aws.js";

interface Event {
  timestamp: number;
  message: string;
  ingestionTime: number;
  eventId: string;
  stream: string;
}

interface Group {
  name: string;
  arn: string;
  creationTime: number;
  retentionInDays?: number;
  kmsKeyId?: string;
  tags: Record<string, string>;
  streams: Map<string, Event[]>;
}

/** The fake's state. */
export class FakeLogsState {
  readonly groups = new Map<string, Group>();
  readonly queries: { queryId: string; queryString: string; logGroupName: string }[] = [];
  #nextId = 1;

  group(name: string): Group | undefined {
    return this.groups.get(name);
  }

  addGroup(name: string, extra: Partial<Group> = {}): Group {
    const g: Group = {
      name,
      arn: `arn:aws:logs:us-east-1:123456789012:log-group:${name}`,
      creationTime: Date.now(),
      tags: {},
      streams: new Map(),
      ...extra,
    };
    this.groups.set(name, g);
    return g;
  }

  put(group: string, stream: string, events: { timestamp: number; message: string }[]): void {
    const g = this.groups.get(group) ?? this.addGroup(group);
    const list = g.streams.get(stream) ?? [];
    for (const e of events) {
      list.push({
        ...e,
        ingestionTime: Date.now(),
        eventId: String(this.#nextId++).padStart(8, "0"),
        stream,
      });
    }
    g.streams.set(stream, list);
  }

  handle(req: AwsRequest): MockResponse {
    const b = req.body;
    const name = (b.logGroupName ?? b.logGroupIdentifier) as string | undefined;
    const g = name === undefined ? undefined : this.groups.get(name);
    const notFound = (): MockResponse => awsError("ResourceNotFoundException");
    switch (req.target) {
      case "CreateLogGroup": {
        if (g !== undefined) return awsError("ResourceAlreadyExistsException");
        this.addGroup(name as string, {
          tags: (b.tags as Record<string, string> | undefined) ?? {},
          ...(typeof b.kmsKeyId === "string" ? { kmsKeyId: b.kmsKeyId } : {}),
        });
        return ok();
      }
      case "DeleteLogGroup":
        if (g === undefined) return notFound();
        this.groups.delete(g.name);
        return ok();
      case "PutRetentionPolicy":
        if (g === undefined) return notFound();
        g.retentionInDays = b.retentionInDays as number;
        return ok();
      case "DeleteRetentionPolicy":
        if (g === undefined) return notFound();
        delete g.retentionInDays;
        return ok();
      case "CreateLogStream": {
        if (g === undefined) return notFound();
        const s = b.logStreamName as string;
        if (g.streams.has(s)) return awsError("ResourceAlreadyExistsException");
        g.streams.set(s, []);
        return ok();
      }
      case "DeleteLogStream":
        if (g === undefined || !g.streams.has(b.logStreamName as string)) return notFound();
        g.streams.delete(b.logStreamName as string);
        return ok();
      case "DescribeLogGroups": {
        const prefix = (b.logGroupNamePrefix as string | undefined) ?? "";
        const all = [...this.groups.values()]
          .filter((x) => x.name.startsWith(prefix))
          .sort((x, y) => x.name.localeCompare(y.name));
        const from = b.nextToken === undefined ? 0 : Number(b.nextToken);
        const limit = (b.limit as number | undefined) ?? 50;
        const page = all.slice(from, from + limit);
        return ok({
          logGroups: page.map((x) => ({
            logGroupName: x.name,
            arn: `${x.arn}:*`,
            logGroupArn: x.arn,
            creationTime: x.creationTime,
            storedBytes: 0,
            metricFilterCount: 0,
            ...(x.retentionInDays === undefined ? {} : { retentionInDays: x.retentionInDays }),
            ...(x.kmsKeyId === undefined ? {} : { kmsKeyId: x.kmsKeyId }),
          })),
          nextToken: from + limit < all.length ? String(from + limit) : null,
        });
      }
      case "DescribeLogStreams": {
        if (g === undefined) return notFound();
        const prefix = (b.logStreamNamePrefix as string | undefined) ?? "";
        const all = [...g.streams.keys()].filter((s) => s.startsWith(prefix)).sort();
        const from = b.nextToken === undefined ? 0 : Number(b.nextToken);
        const limit = (b.limit as number | undefined) ?? 50;
        return ok({
          logStreams: all.slice(from, from + limit).map((s) => ({
            logStreamName: s,
            arn: `${g.arn}:log-stream:${s}`,
            creationTime: g.creationTime,
          })),
          ...(from + limit < all.length ? { nextToken: String(from + limit) } : {}),
        });
      }
      case "PutLogEvents": {
        if (g === undefined || !g.streams.has(b.logStreamName as string)) return notFound();
        this.put(g.name, b.logStreamName as string, b.logEvents as Event[]);
        return ok({ nextSequenceToken: "1" });
      }
      case "FilterLogEvents": {
        if (g === undefined) return notFound();
        const pattern = (b.filterPattern as string | undefined) ?? "";
        const terms = pattern.startsWith('"')
          ? [pattern.slice(1, -1).replaceAll('\\"', '"')]
          : pattern.split(/\s+/).filter((t) => t !== "");
        const streams = b.logStreamNames as string[] | undefined;
        const all = [...g.streams.values()]
          .flat()
          .filter((e) => streams === undefined || streams.includes(e.stream))
          .filter((e) => b.startTime === undefined || e.timestamp >= (b.startTime as number))
          .filter((e) => b.endTime === undefined || e.timestamp <= (b.endTime as number))
          .filter((e) => terms.every((t) => e.message.includes(t)))
          .sort((x, y) => x.timestamp - y.timestamp);
        const from = b.nextToken === undefined ? 0 : Number(b.nextToken);
        const limit = (b.limit as number | undefined) ?? 10_000;
        return ok({
          events: all.slice(from, from + limit).map((e) => ({
            eventId: e.eventId,
            timestamp: e.timestamp,
            ingestionTime: e.ingestionTime,
            message: e.message,
            logStreamName: e.stream,
          })),
          ...(from + limit < all.length ? { nextToken: String(from + limit) } : { nextToken: null }),
          searchedLogStreams: [],
        });
      }
      case "GetLogEvents": {
        if (g === undefined) return notFound();
        const events = g.streams.get(b.logStreamName as string);
        if (events === undefined) return notFound();
        return ok({
          events: events.map((e) => ({
            timestamp: e.timestamp,
            message: e.message,
            ingestionTime: e.ingestionTime,
          })),
          nextForwardToken: "f/1",
          nextBackwardToken: "b/1",
        });
      }
      case "TagResource":
      case "UntagResource":
      case "ListTagsForResource": {
        const arn = b.resourceArn as string;
        const tg = [...this.groups.values()].find((x) => x.arn === arn);
        if (tg === undefined) return notFound();
        if (req.target === "TagResource") Object.assign(tg.tags, b.tags);
        if (req.target === "UntagResource") {
          for (const k of b.tagKeys as string[]) delete tg.tags[k];
        }
        return ok(req.target === "ListTagsForResource" ? { tags: tg.tags } : {});
      }
      case "StartQuery": {
        const groups = (b.logGroupNames as string[] | undefined) ?? [];
        for (const n of groups) if (!this.groups.has(n)) return notFound();
        const queryId = `q-${this.queries.length + 1}`;
        this.queries.push({
          queryId,
          queryString: b.queryString as string,
          logGroupName: groups[0] ?? "",
        });
        return ok({ queryId });
      }
      case "GetQueryResults":
        return ok({
          results: [
            [
              { field: "@timestamp", value: "1795033642000" },
              { field: "@message", value: "hello" },
            ],
          ],
          status: "Complete",
        });
      case "DescribeQueries":
        return ok({
          queries: this.queries.map((q) => ({
            ...q,
            status: "Complete",
            createTime: 1795033644934.42,
          })),
        });
      case "StopQuery":
        return ok({ success: true });
      default:
        return { status: 500, body: "<!doctype html><title>not implemented</title>" };
    }
  }
}

/** Starts a stateful fake; `override` may answer an operation first (return undefined to pass). */
export async function startFakeLogs(
  override?: (req: AwsRequest, index: number) => MockResponse | undefined,
): Promise<Awaited<ReturnType<typeof startFakeCloudWatch>> & { state: FakeLogsState }> {
  const state = new FakeLogsState();
  const handler: AwsHandler = (req, i) => override?.(req, i) ?? state.handle(req);
  const fake = await startFakeCloudWatch(handler);
  return Object.assign(fake, { state });
}
