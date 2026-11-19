# Python examples

Install the package, point it at Amazon CloudWatch Logs (or the Moto emulator) and run an example:

```bash
pip install grow-aws-cloudwatch-logs
export AWS_REGION=us-east-1                               # plus credentials or AWS_PROFILE
export GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000  # only for an emulator
python examples/python/basic.py
```

| File | Shows |
| - | - |
| `basic.py` | sync client: health, ping, `ensure_log_group`, `put_log_events`, `get_log_events`, `filter_log_events`, `emf`, Logs Insights `query`, error handling |
| `async_basic.py` | async client: batching log client + `Logger`, `wait_for_log`, `search`, Live Tail |
| `hooks_and_logging.py` | request hooks, the `grow_aws_cloudwatch_logs` logger, retries and timeouts, a stdlib `logging.Handler` that ships records to CloudWatch Logs |
