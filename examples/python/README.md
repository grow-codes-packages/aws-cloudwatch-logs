# Python examples

Install the package, point it at a running Amazon CloudWatch Logs and run an example:

```bash
pip install grow-aws-cloudwatch-logs
export GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000
export GROW_AWS_CLOUDWATCH_LOGS_TOKEN=...   # when the service needs one
python examples/python/basic.py
```

| File | Shows |
| - | - |
| `basic.py` | sync client: health, ping, server version, error handling |
| `async_basic.py` | async client, cancellation by task |
| `hooks_and_logging.py` | request hooks, the `grow_aws_cloudwatch_logs` logger, retries and timeouts |
