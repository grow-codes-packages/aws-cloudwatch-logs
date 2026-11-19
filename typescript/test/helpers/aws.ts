// Fake CloudWatch Logs endpoint for unit tests: answers AWS JSON 1.1 requests by X-Amz-Target with
// canned responses (usually the fixtures recorded from Moto 5.2.4 in test/fixtures/).

import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { EventStreamCodec } from "@smithy/eventstream-codec";
import type { MessageHeaders } from "@smithy/types";
import { fromUtf8, toUtf8 } from "@smithy/util-utf8";
import { Agent } from "undici";
import { CloudWatchLogsClient, type CloudWatchLogsConfigOptions } from "../../src/index.js";
import { type MockResponse, type MockServer, type RecordedRequest, startServer } from "./server.js";

const fixturesDir = fileURLToPath(new URL("../fixtures/", import.meta.url));

/** A recorded fixture (see API.md §7). */
export interface Fixture {
  function: string;
  case: string;
  operation: string;
  request: { body: unknown };
  response: {
    status: number;
    headers: Record<string, string>;
    body?: unknown;
    bodyBase64?: string;
    eventStream?: { headers: Record<string, string>; payload: unknown }[];
  };
}

const cache = new Map<string, Fixture>();

/** Loads `test/fixtures/<name>.json`. */
export function fixture(name: string): Fixture {
  let f = cache.get(name);
  if (f === undefined) {
    f = JSON.parse(readFileSync(`${fixturesDir}${name}.json`, "utf8")) as Fixture;
    cache.set(name, f);
  }
  return f;
}

/** Every fixture name. */
export function fixtureNames(): string[] {
  return readdirSync(fixturesDir)
    .filter((n) => n.endsWith(".json"))
    .map((n) => n.slice(0, -5));
}

/** Converts a fixture's response to a mock response (status, headers, raw body). */
export function fixtureResponse(name: string): MockResponse {
  const r = fixture(name).response;
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(r.headers)) {
    if (k === "content-length" || k === "status") continue;
    headers[k] = v;
  }
  let body: string | Buffer;
  if (r.bodyBase64 !== undefined) body = Buffer.from(r.bodyBase64, "base64");
  else if (r.body === null || r.body === undefined) body = "";
  else if (typeof r.body === "string") body = r.body;
  else body = JSON.stringify(r.body);
  return { status: r.status, headers, body };
}

/** One request the fake service saw, decoded. */
export interface AwsRequest {
  target: string;
  body: Record<string, unknown>;
  headers: RecordedRequest["headers"];
  host: string;
}

/** Handler: operation name (e.g. `PutLogEvents`) + decoded body → response. */
export type AwsHandler = (req: AwsRequest, index: number) => MockResponse | Promise<MockResponse>;

/** The fake service and helpers. */
export interface FakeCloudWatch {
  server: MockServer;
  url: string;
  requests: AwsRequest[];
  /** Operations of the requests seen, in order. */
  ops(): string[];
  close(): Promise<void>;
}

/** JSON success response like Moto's (Moto sends text/html as content type). */
export function ok(body: unknown = null, headers: Record<string, string> = {}): MockResponse {
  return {
    status: 200,
    headers: { "content-type": "application/x-amz-json-1.1", "x-amzn-requestid": "req-1", ...headers },
    body: body === null ? "" : JSON.stringify(body),
  };
}

/** AWS JSON error response. */
export function awsError(code: string, status = 400, message = "boom"): MockResponse {
  return {
    status,
    headers: {
      "content-type": "application/x-amz-json-1.1",
      "x-amzn-requestid": "req-err",
      "x-amzn-errortype": code,
    },
    body: JSON.stringify({ __type: code, message }),
  };
}

/** Starts the fake service. */
export async function startFakeCloudWatch(handler: AwsHandler): Promise<FakeCloudWatch> {
  const requests: AwsRequest[] = [];
  const server = await startServer(async (raw, i) => {
    const target = String(raw.headers["x-amz-target"] ?? "").replace(/^Logs_20140328\./, "");
    const body = raw.body === "" ? {} : (JSON.parse(raw.body) as Record<string, unknown>);
    const req: AwsRequest = { target, body, headers: raw.headers, host: String(raw.headers.host) };
    requests.push(req);
    return handler(req, i);
  });
  return {
    server,
    url: server.url,
    requests,
    ops: () => requests.map((r) => r.target),
    close: () => server.close(),
  };
}

/** A dispatcher that connects every host name (e.g. `stream-127.0.0.1`) to 127.0.0.1. */
export function loopbackDispatcher(): Agent {
  return new Agent({
    connect: {
      lookup: (
        _host: string,
        opts: { all?: boolean },
        cb: (e: Error | null, a: unknown, f?: number) => void,
      ) => {
        if (opts.all === true) cb(null, [{ address: "127.0.0.1", family: 4 }]);
        else cb(null, "127.0.0.1", 4);
      },
    } as never,
  });
}

/** Test credentials (never real). */
export const TEST_CREDENTIALS = {
  region: "us-east-1",
  accessKeyId: "AKIDTESTTESTTEST",
  secretAccessKey: "test-secret-key-value",
};

/**
 * The fake's URL under a host name (the loopback dispatcher resolves it), so the SDK's `stream-`
 * host prefix for StartLiveTail yields a valid URL.
 */
export function fakeHostUrl(fake: FakeCloudWatch): string {
  return `http://fake-cwl:${fake.server.port}`;
}

/** A client wired to the fake service, with fast retries. */
export function testClient(
  fake: FakeCloudWatch,
  options: CloudWatchLogsConfigOptions = {},
  internals: ConstructorParameters<typeof CloudWatchLogsClient>[1] = {},
): CloudWatchLogsClient {
  return new CloudWatchLogsClient(
    { ...TEST_CREDENTIALS, url: fakeHostUrl(fake), retry: { maxRetries: 0 }, ...options },
    { dispatcher: loopbackDispatcher(), ...internals },
  );
}

const codec = new EventStreamCodec(toUtf8, fromUtf8);

/** Encodes event-stream frames (`:event-type` + JSON payload, or exception frames). */
export function eventStream(
  frames: { type: string; payload?: unknown; exception?: boolean }[],
): Buffer {
  const parts = frames.map((f) => {
    const headers: MessageHeaders = {
      ":message-type": { type: "string", value: f.exception === true ? "exception" : "event" },
      [f.exception === true ? ":exception-type" : ":event-type"]: { type: "string", value: f.type },
      ":content-type": { type: "string", value: "application/json" },
    };
    const body = fromUtf8(f.payload === undefined ? "" : JSON.stringify(f.payload));
    return Buffer.from(codec.encode({ headers, body }));
  });
  return Buffer.concat(parts);
}

/** Event-stream response. */
export function streamResponse(body: Buffer, delayMs?: number): MockResponse {
  return {
    status: 200,
    headers: { "content-type": "application/vnd.amazon.eventstream", "x-amzn-requestid": "tail" },
    body,
    ...(delayMs === undefined ? {} : { delayMs }),
  };
}
