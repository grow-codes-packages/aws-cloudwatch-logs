// Service-owned: runs the shared conformance scenarios (core + logging) against the stateful fake
// CloudWatch Logs (the Docker suite runs the same scenarios against Moto).

import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { CloudWatchLogsClient } from "../../src/client.js";
import type { BatchOptions } from "../../src/logging/index.js";
import {
  type ConformanceOptions,
  conformanceCases,
  formatReport,
  runConformance,
} from "../../src/testing/index.js";
import { testClient } from "../helpers/aws.js";
import { startFakeLogs } from "../helpers/fake-logs.js";

const scenariosDir = fileURLToPath(new URL("../../../conformance/scenarios/", import.meta.url));
let fake: Awaited<ReturnType<typeof startFakeLogs>>;
const clients: CloudWatchLogsClient[] = [];

beforeAll(async () => {
  fake = await startFakeLogs();
});

afterAll(async () => {
  await Promise.all(clients.map((c) => c.close()));
  await fake.close();
});

function client(group?: string): CloudWatchLogsClient {
  const c = testClient(fake, group === undefined ? {} : { logGroup: group });
  clients.push(c);
  return c;
}

function options(): ConformanceOptions {
  return {
    scenariosDir,
    categories: ["core", "logging"],
    factories: {
      core: () => client(),
      logging: (ctx) => {
        const group = `conformance/${ctx.run}`;
        fake.state.addGroup(group);
        const c = client(group);
        return {
          client: c.logClient({ batch: ctx.batchOptions as BatchOptions }),
          query: c.logQueryClient(),
        };
      },
    },
    capabilities: { core: [], logging: ["query"] },
  };
}

describe("conformance (core + logging) against the fake CloudWatch Logs", () => {
  it("every scenario passes; serverVersion is skipped (no version)", async () => {
    const report = await runConformance(options());
    expect(report.failed, formatReport(report)).toBe(0);
    expect(report.results.map((r) => r.id)).toEqual(
      expect.arrayContaining(["core.health", "logging.roundtrip", "logging.batch", "logging.redaction"]),
    );
  });

  for (const c of conformanceCases(options())) {
    it.skipIf(c.skip !== undefined)(`case ${c.id}`, async () => {
      expect((await c.run()).status).toBe("passed");
    });
  }
});
