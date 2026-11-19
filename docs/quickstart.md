# Quickstart

Connect to Amazon CloudWatch Logs, create a log group, write two events and search them, then close
the client. The client reads `GROW_AWS_CLOUDWATCH_LOGS_*` first and then the standard AWS variables
and credential chain (see [Functions → Service configuration](functions.md#service-configuration)).

```sh
export AWS_REGION=eu-central-1
export AWS_PROFILE=dev                # or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, SSO, IAM role …
export CLOUDWATCH_LOG_GROUP=my-app/api   # optional default group (emf, logClient, logQueryClient)
# Emulator (Moto) instead of AWS:
# export GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000
```

The IAM principal needs the `logs:` actions you call (for example `logs:CreateLogGroup`,
`logs:CreateLogStream`, `logs:PutLogEvents`, `logs:DescribeLogGroups`, `logs:FilterLogEvents`;
`logs:StartLiveTail`, `logs:StartQuery` / `logs:GetQueryResults` for live tail and Insights).

=== "TypeScript"

    ```ts
    import { CloudWatchLogsClient } from "@grow-codes/aws-cloudwatch-logs";

    const client = CloudWatchLogsClient.fromEnv();
    try {
      await client.ensureLogGroup("my-app/api", { retentionDays: 7, tags: { team: "platform" } });
      const res = await client.putLogEvents("my-app/api", "web-1", [
        { timestamp: Date.now(), message: "service started" },
        { timestamp: Date.now(), message: "ERROR something failed" },
      ]);
      console.log(res.accepted, res.rejected);
      for await (const e of client.filterLogEvents({ group: "my-app/api", pattern: "ERROR" })) {
        console.log(e.timestamp, e.message);
      }
    } finally {
      await client.close();
    }
    ```

=== "Rust"

    ```rust
    use grow_aws_cloudwatch_logs::{
        CloudWatchLogsClient, EnsureLogGroupOptions, FilterLogEventsOptions, LogEvent, PutLogEventsOptions,
    };

    #[tokio::main]
    async fn main() -> Result<(), grow_aws_cloudwatch_logs::Error> {
        let client = CloudWatchLogsClient::from_env()?;
        client
            .ensure_log_group("my-app/api", EnsureLogGroupOptions::default().retention_days(7).tag("team", "platform"))
            .await?;
        let res = client
            .put_log_events(
                "my-app/api",
                "web-1",
                vec![LogEvent::new("service started"), LogEvent::new("ERROR something failed")],
                PutLogEventsOptions::default(),
            )
            .await?;
        println!("{} {}", res.accepted, res.rejected);
        let errors = client
            .filter_log_events(FilterLogEventsOptions {
                group: Some("my-app/api".into()),
                pattern: Some("ERROR".into()),
                ..Default::default()
            })
            .try_collect()
            .await?;
        println!("{errors:?}");
        client.close().await;
        Ok(())
    }
    ```

=== "Go"

    ```go
    package main

    import (
    	"context"
    	"fmt"
    	"log"

    	cloudwatchlogs "github.com/grow-codes-packages/aws-cloudwatch-logs/go"
    )

    func main() {
    	ctx := context.Background()
    	client, err := cloudwatchlogs.NewClientFromEnv()
    	if err != nil {
    		log.Fatal(err)
    	}
    	defer client.Close()

    	if _, err := client.EnsureLogGroup(ctx, "my-app/api", cloudwatchlogs.EnsureLogGroupOptions{
    		RetentionDays: cloudwatchlogs.Days(7), Tags: map[string]string{"team": "platform"},
    	}); err != nil {
    		log.Fatal(err)
    	}
    	res, err := client.PutLogEvents(ctx, "my-app/api", "web-1", []cloudwatchlogs.LogEvent{
    		{Message: "service started"}, {Message: "ERROR something failed"},
    	}, cloudwatchlogs.PutLogEventsOptions{})
    	if err != nil {
    		log.Fatal(err)
    	}
    	fmt.Println(res.Accepted, res.Rejected)
    	for e, err := range client.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: "my-app/api", Pattern: "ERROR"}) {
    		if err != nil {
    			log.Fatal(err)
    		}
    		fmt.Println(e.Timestamp, e.Message)
    	}
    }
    ```

=== "Python"

    ```python
    import grow_aws_cloudwatch_logs as sdk

    with sdk.CloudWatchLogsClient.from_env() as client:
        client.ensure_log_group("my-app/api", retention_days=7, tags={"team": "platform"})
        res = client.put_log_events(
            "my-app/api",
            "web-1",
            [sdk.InputLogEvent("service started"), sdk.InputLogEvent("ERROR something failed")],
        )
        print(res.accepted, res.rejected)
        for e in client.filter_log_events(group="my-app/api", pattern="ERROR"):
            print(e.timestamp, e.message)
    ```

    Async code uses `AsyncCloudWatchLogsClient` (install `grow-aws-cloudwatch-logs[async]`):

    ```python
    import asyncio
    import grow_aws_cloudwatch_logs as sdk

    async def main() -> None:
        async with sdk.AsyncCloudWatchLogsClient.from_env() as client:
            await client.ensure_log_group("my-app/api")
            await client.put_log_events("my-app/api", "web-1", [sdk.InputLogEvent("hello")])
            async for e in client.filter_log_events(group="my-app/api", pattern="hello"):
                print(e.message)

    asyncio.run(main())
    ```

=== "Java"

    ```java
    import codes.grow.packages.cloudwatchlogs.*;
    import java.util.List;
    import java.util.Map;

    try (CloudWatchLogsClient client = CloudWatchLogsClient.fromEnv()) {
        client.ensureLogGroup("my-app/api",
            EnsureLogGroupOptions.builder().retentionDays(7).tags(Map.of("team", "platform")).build());
        PutLogEventsResult res = client.putLogEvents("my-app/api", "web-1",
            List.of(LogEvent.of("service started"), LogEvent.of("ERROR something failed")));
        System.out.println(res.accepted() + " " + res.rejected());
        for (FilteredLogEvent e : client.filterLogEvents(
                FilterLogEventsOptions.builder().group("my-app/api").pattern("ERROR").build())) {
            System.out.println(e.timestamp() + " " + e.message());
        }
    }
    ```

`health()` checks reachability with a signed `DescribeLogGroups` call (it never throws),
`ping()` returns the round-trip time of an authenticated call and `serverVersion()` raises
`UNSUPPORTED` (AWS has no version endpoint).

## Next steps

- [Configuration](configuration.md): timeouts, retries, TLS, proxy, environment variables.
- [Functions](functions.md): the Amazon CloudWatch Logs API of this SDK.
- [Logging](logging.md): the batching `LogClient`, logger facade and framework adapters.
- [Error handling](errors.md): error codes and retries.
