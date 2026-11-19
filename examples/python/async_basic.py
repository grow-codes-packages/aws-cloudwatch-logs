"""The async client: a log client with batching, a search and a Live Tail session."""

from __future__ import annotations

import asyncio
import secrets

import grow_aws_cloudwatch_logs as sdk


async def main() -> None:
    group = f"grow-example/{secrets.token_hex(3)}"
    async with sdk.AsyncCloudWatchLogsClient.from_env(log_group=group) as client:
        await client.ensure_log_group(group)
        try:
            logs = client.log_client(resource={"service.name": "example"})
            logger = logs.logger("checkout")
            logger.info("payment accepted", {"order": 42, "password": "never-sent"})
            logger.error("payment provider timeout", {"order": 43})
            await logs.flush(10)
            print(logs.stats())

            query = client.log_query_client()
            record = await query.wait_for_log("payment accepted", timeout_s=20)
            print(f"found: {record.severity_text} {record.body} {record.attributes}")
            page = await query.search("payment", limit=10)  # newest first
            print([r.body for r in page.records])

            # Live Tail: on AWS new events stream for up to 3 hours; Moto replays and ends.
            session = await client.start_live_tail([group], lambda e: print("tail:", e.message))
            await session.wait(5)
            await session.close()
        finally:
            await client.delete_log_group(group, ignore_missing=True)


if __name__ == "__main__":
    asyncio.run(main())
