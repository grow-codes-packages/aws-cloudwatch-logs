# grow-aws-cloudwatch-logs (Python)

Python SDK for [Amazon CloudWatch Logs](https://grow-codes-packages.github.io/aws-cloudwatch-logs/) by Grow Codes,
built on the official AWS SDK for Python (boto3, aioboto3).
Unofficial and community maintained; "Amazon CloudWatch Logs" is a trademark of Amazon.com, Inc. or its affiliates.

```bash
pip install grow-aws-cloudwatch-logs
```

```python
from grow_aws_cloudwatch_logs import CloudWatchLogsClient, InputLogEvent

# Standard AWS environment (AWS_REGION, credentials / profile) plus GROW_AWS_CLOUDWATCH_LOGS_*.
with CloudWatchLogsClient.from_env() as client:
    client.ensure_log_group("my-app/prod", retention_days=30, tags={"team": "api"})
    client.put_log_events("my-app/prod", "web-1", [InputLogEvent("hello")])
    for event in client.filter_log_events(group="my-app/prod", pattern="ERROR"):
        print(event.timestamp, event.message)
    print(client.query("fields @timestamp, @message | limit 5", groups=["my-app/prod"]).rows)
```

The async client (aioboto3) has the same methods; iterators are async iterators:

```python
import asyncio
from grow_aws_cloudwatch_logs import AsyncCloudWatchLogsClient

async def main() -> None:
    async with AsyncCloudWatchLogsClient.from_env(log_group="my-app/prod") as client:
        logs = client.log_client()               # batching LogClient (SPEC logging category)
        logs.logger("checkout").info("paid", {"order": 42})
        await logs.flush(10)
        page = await client.log_query_client().search("paid")  # newest first
        print([r.body for r in page.records])

asyncio.run(main())
```

- Python 3.10 or newer, typed (`py.typed`). Signing, retries (standard mode, `max_retries + 1`
  attempts) and the Live Tail event stream come from botocore; the Grow timeouts, TLS, proxy,
  headers and User-Agent are mapped onto the SDK client; hooks fire from botocore events.
- `put_log_events` sorts, size-limits (256 KiB per event by default, truncate / drop / error) and
  splits batches to the service limits (10,000 events, 1 MiB, 24 h).
- Errors are mapped by AWS error code to `CloudWatchLogsError` subclasses with `request_id`.
- Credentials are wrapped in `Secret` and never appear in `repr()`, logs or error messages.
- `grow_aws_cloudwatch_logs.testing` ships in-memory fakes and the conformance runner.

Documentation: https://grow-codes-packages.github.io/aws-cloudwatch-logs/ ·
Source: https://github.com/grow-codes-packages/aws-cloudwatch-logs · Licence: Apache-2.0
