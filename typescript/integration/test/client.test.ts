// Integration tests of the core client against the real service (Moto, docker/compose.yml):
// health, ping (good / wrong secret / unknown key), serverVersion, close, and a permission error
// with the read-only bootstrap user.

import assert from "node:assert/strict";
import { before, test } from "node:test";
import { CloudWatchLogsClient, CloudWatchLogsError } from "@grow-codes/aws-cloudwatch-logs";
import { loadBootstrapEnv } from "./env.ts";

before(() => {
  loadBootstrapEnv();
  assert.ok(process.env.GROW_AWS_CLOUDWATCH_LOGS_URL, "GROW_AWS_CLOUDWATCH_LOGS_URL must be set");
  assert.ok(process.env.AWS_ACCESS_KEY_ID, "bootstrap credentials must be loaded");
});

test("health reports a healthy service", async () => {
  const client = CloudWatchLogsClient.fromEnv();
  try {
    const health = await client.health();
    assert.deepEqual(health, { healthy: true, status: "ok", details: {} });
  } finally {
    await client.close();
  }
});

test("ping succeeds with the bootstrap credentials", async () => {
  const client = CloudWatchLogsClient.fromEnv();
  try {
    const latency = await client.ping();
    assert.equal(typeof latency, "number");
    assert.ok(latency >= 0);
  } finally {
    await client.close();
  }
});

test("ping with a wrong secret or an unknown key raises AUTHENTICATION and never leaks it", async () => {
  const wrong = "wrong-secret-for-integration-test";
  for (const overrides of [
    { accessKeyId: process.env.AWS_ACCESS_KEY_ID as string, secretAccessKey: wrong },
    { accessKeyId: "AKIAUNKNOWNKEY000000", secretAccessKey: wrong },
  ]) {
    const client = CloudWatchLogsClient.fromEnv(overrides);
    try {
      await assert.rejects(client.ping(), (err: unknown) => {
        assert.ok(err instanceof CloudWatchLogsError);
        assert.equal(err.code, "AUTHENTICATION");
        assert.equal(err.httpStatus, 403);
        assert.ok(!err.message.includes(wrong));
        assert.ok(!JSON.stringify(err).includes(wrong));
        return true;
      });
      // the service answered, so health still reports it healthy (auth failed)
      const health = await client.health();
      assert.equal(health.healthy, true);
      assert.equal(health.details.auth, "failed");
    } finally {
      await client.close();
    }
  }
});

test("the read-only user gets PERMISSION_DENIED for writes but may read", async () => {
  const client = CloudWatchLogsClient.fromEnv({
    accessKeyId: process.env.GROW_TEST_READONLY_ACCESS_KEY_ID as string,
    secretAccessKey: process.env.GROW_TEST_READONLY_SECRET_ACCESS_KEY as string,
  });
  try {
    await assert.rejects(client.ensureLogGroup("grow-readonly/denied"), {
      code: "PERMISSION_DENIED",
    });
    assert.ok((await client.ping()) >= 0);
  } finally {
    await client.close();
  }
});

test("serverVersion is UNSUPPORTED and close is idempotent", async () => {
  const client = CloudWatchLogsClient.fromEnv();
  try {
    await assert.rejects(client.serverVersion(), { code: "UNSUPPORTED" });
  } finally {
    await client.close();
    await client.close();
  }
  await assert.rejects(client.ping(), { code: "CANCELLED" });
});
