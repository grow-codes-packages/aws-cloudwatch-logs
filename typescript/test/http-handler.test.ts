// Service-owned: the undici-based HTTP handler used by the AWS SDK client.

import { afterEach, describe, expect, it } from "vitest";
import { CloudWatchLogsConfig, GROW_USER_AGENT, GrowHttpHandler } from "../src/index.js";
import { parseAttemptHeader } from "../src/http-handler.js";
import { type MockServer, startServer } from "./helpers/server.js";

const servers: MockServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

describe("GrowHttpHandler", () => {
  it("joins query parameters, adds the User-Agent and returns the body", async () => {
    const s = await startServer(() => ({
      status: 200,
      headers: { "x-amz-request-id": "r2" },
      body: "ok",
    }));
    servers.push(s);
    const handler = new GrowHttpHandler({ config: new CloudWatchLogsConfig({}) });
    expect(handler.userAgent).toBe(GROW_USER_AGENT);
    const { response } = await handler.handle({
      method: "get",
      protocol: "http:",
      hostname: "127.0.0.1",
      port: s.port,
      path: "x",
      query: { a: "1", b: ["2", "3"], c: null as never },
      headers: {},
    } as never);
    expect(response.statusCode).toBe(200);
    expect(Buffer.from(response.body as Uint8Array).toString()).toBe("ok");
    expect(s.requests[0]?.url).toBe("/x?a=1&b=2&b=3");
    expect(s.requests[0]?.headers["user-agent"]).toBe(GROW_USER_AGENT);
    expect(handler.httpHandlerConfigs()).toEqual({});
    handler.updateHttpClientConfig();
    handler.destroy();
    handler.destroy();
  });

  it("aborts with AbortError when the caller signal is already aborted", async () => {
    const s = await startServer(() => ({ status: 200, body: "ok" }));
    servers.push(s);
    const handler = new GrowHttpHandler({ config: new CloudWatchLogsConfig({}) });
    const ac = new AbortController();
    ac.abort();
    await expect(
      handler.handle(
        {
          method: "POST",
          protocol: "http:",
          hostname: "127.0.0.1",
          port: s.port,
          path: "/",
          headers: {},
          body: "{}",
        } as never,
        { abortSignal: ac.signal as never },
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    handler.destroy();
  });

  it("parseAttemptHeader", () => {
    expect(parseAttemptHeader("attempt=2; max=4")).toEqual({ attempt: 2, max: 4 });
    expect(parseAttemptHeader(undefined)).toEqual({ attempt: 1, max: 1 });
  });

  it("wraps non-Error failures", async () => {
    const handler = new GrowHttpHandler({
      config: new CloudWatchLogsConfig({}),
      dispatcher: {
        dispatch: () => {
          throw "boom";
        },
      } as never,
    });
    await expect(
      handler.handle({
        method: "GET",
        protocol: "http:",
        hostname: "h",
        path: "/",
        headers: {},
      } as never),
    ).rejects.toBeInstanceOf(Error);
  });
});
