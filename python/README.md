# grow-aws-cloudwatch-logs (Python)

Python SDK for [Amazon CloudWatch Logs](https://grow-codes-packages.github.io/aws-cloudwatch-logs/) by Grow Codes.
Unofficial and community maintained; "Amazon CloudWatch Logs" is a trademark of Amazon.com, Inc. or its affiliates.

```bash
pip install grow-aws-cloudwatch-logs
```

```python
from grow_aws_cloudwatch_logs import CloudWatchLogsClient, CloudWatchLogsConfig

# Reads GROW_AWS_CLOUDWATCH_LOGS_URL, GROW_AWS_CLOUDWATCH_LOGS_TOKEN, ... from the environment.
with CloudWatchLogsClient.from_env() as client:
    print(client.health())
    print(client.server_version())
```

The async client has the same methods:

```python
import asyncio
from grow_aws_cloudwatch_logs import AsyncCloudWatchLogsClient

async def main() -> None:
    async with AsyncCloudWatchLogsClient.from_env() as client:
        print(await client.health())

asyncio.run(main())
```

- Python 3.10 or newer, typed (`py.typed`), `httpx` based, environment proxies ignored unless
  configured.
- Retries with full-jitter backoff and `Retry-After`, connect/request/total timeouts,
  cancellation, hooks and a silent-by-default `logging` logger named `grow_aws_cloudwatch_logs`.
- Credentials are wrapped in `Secret` and never appear in `repr()`, logs or error messages.
- `grow_aws_cloudwatch_logs.testing` ships in-memory fakes and the conformance runner.

Documentation: https://grow-codes-packages.github.io/aws-cloudwatch-logs/ ·
Source: https://github.com/grow-codes-packages/aws-cloudwatch-logs · Licence: Apache-2.0
