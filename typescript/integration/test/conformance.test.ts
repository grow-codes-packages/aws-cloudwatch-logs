// Runs the shared conformance scenarios (conformance/scenarios) against the real service.
// Capabilities: core — none ("version" is not declared: CloudWatch Logs exposes no server version,
// so the serverVersion step is skipped); logging — "query" (search = FilterLogEvents).
// Skips are justified in docs/compatibility.md.

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { CloudWatchLogsClient, type BatchOptions } from "@grow-codes/aws-cloudwatch-logs";
import { formatReport, runConformance } from "@grow-codes/aws-cloudwatch-logs/testing";
import { loadBootstrapEnv, runId, scenariosDir } from "./env.ts";

const clients: CloudWatchLogsClient[] = [];
const groups: string[] = [];

before(() => {
  loadBootstrapEnv();
});

after(async () => {
  const admin = CloudWatchLogsClient.fromEnv();
  for (const g of groups) await admin.deleteLogGroup(g, { ignoreMissing: true });
  await admin.close();
  await Promise.all(clients.map((c) => c.close()));
});

test("conformance: core + logging", async () => {
  const report = await runConformance({
    scenariosDir,
    categories: ["core", "logging"],
    factories: {
      core: () => {
        const c = CloudWatchLogsClient.fromEnv();
        clients.push(c);
        return c;
      },
      logging: async (ctx) => {
        const group = `sdk-ts-conformance/${runId}/${ctx.run}`.replace(/[^.\-_/#A-Za-z0-9]/g, "_");
        groups.push(group);
        const c = CloudWatchLogsClient.fromEnv({ logGroup: group });
        clients.push(c);
        await c.ensureLogGroup(group);
        return {
          client: c.logClient({ batch: ctx.batchOptions as BatchOptions }),
          query: c.logQueryClient(),
        };
      },
    },
    capabilities: { core: [], logging: ["query"] },
  });
  console.log(formatReport(report));
  assert.equal(report.failed, 0, formatReport(report));
  assert.ok(report.passed >= 4);
});
