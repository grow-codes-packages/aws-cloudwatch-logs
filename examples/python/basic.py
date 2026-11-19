"""Create a log group, send events, read them back and run a Logs Insights query (sync client).

Environment: the standard AWS variables (AWS_REGION, credentials or a profile) and, for an
emulator, GROW_AWS_CLOUDWATCH_LOGS_URL (e.g. http://127.0.0.1:5000).
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone

import grow_aws_cloudwatch_logs as sdk


def main() -> None:
    group = f"grow-example/{secrets.token_hex(3)}"
    with sdk.CloudWatchLogsClient.from_env() as client:
        status = client.health()
        print(f"healthy={status.healthy} status={status.status} ping={client.ping():.1f} ms")
        try:
            created = client.ensure_log_group(group, retention_days=7, tags={"team": "sdk"})
            print(f"log group {created.arn} created={created.created}")
            now = datetime.now(timezone.utc)
            result = client.put_log_events(
                group,
                "app",
                [
                    sdk.InputLogEvent("user signed in", now - timedelta(seconds=2)),
                    sdk.InputLogEvent("order placed ERROR", now - timedelta(seconds=1)),
                    sdk.InputLogEvent("order shipped", now),
                ],
            )
            print(f"accepted={result.accepted} rejected={result.rejected} calls={result.batches}")
            for event in client.get_log_events(group, "app", start_from_head=True):
                print(f"  {event.timestamp:%H:%M:%S} {event.message}")
            for event in client.filter_log_events(group=group, pattern="ERROR"):
                print(f"  match in {event.log_stream_name}: {event.message}")
            client.emf(
                "orders", 1, {"service": "shop"}, group=group, stream="metrics", unit="Count"
            )
            insights = client.query(
                "fields @timestamp, @message | sort @timestamp desc | limit 2",
                groups=[group],
                start=now - timedelta(minutes=5),
            )
            print(f"insights: {insights.status}, {len(insights.rows)} rows")
        except sdk.AuthenticationError as err:
            print(f"credentials rejected: {err}")
        except sdk.CloudWatchLogsError as err:
            print(f"failed with {err.code}: {err} (retryable={err.retryable})")
        finally:
            client.delete_log_group(group, ignore_missing=True)


if __name__ == "__main__":
    main()
