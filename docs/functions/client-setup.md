# Client setup

Build a client from the environment, from an explicit configuration or around an existing AWS SDK
client, check it and close it.

| Function | AWS operation | Notes |
| - | - | - |
| `fromEnv()` / `new(config)` / `builder()` (Java, Rust config builder) | — | builds the AWS SDK client on the first call: region, credential chain or static keys, profile, optional assume role (`roleArn`, standard STS endpoint resolution, `AWS_ENDPOINT_URL_STS` honoured), endpoint override `url`. A missing region → `CONFIG` |
| `withSdkClient(sdk)` / `sdkClient` config | — | wraps an existing AWS SDK CloudWatch Logs client; Grow TLS, proxy, hooks, headers and User-Agent do not apply to it; `close()` leaves it usable |
| `health()` | `DescribeLogGroups` (limit 1) | never throws: `{healthy, status, latency, details}`. Probe timeout `min(requestTimeout, 5 s)`, no retries. Authentication and permission failures still count as reachable (`healthy: true`, `details.auth`) |
| `ping()` | `DescribeLogGroups` (limit 1) | round-trip time of an authenticated call; raises the mapped error |
| `serverVersion()` | — | always `UNSUPPORTED` (AWS has no version endpoint) |
| `close()` | — | idempotent; flushes the log clients created by this client, stops open live-tail sessions and releases connections. Every later call raises `CANCELLED` |

=== "TypeScript"

    ```ts
    import { CloudWatchLogsClient } from "@grow-codes/aws-cloudwatch-logs";
    import { CloudWatchLogsClient as AwsLogs } from "@aws-sdk/client-cloudwatch-logs";

    const client = CloudWatchLogsClient.fromEnv({ logGroup: "my-app/api" }); // env + overrides
    const explicit = new CloudWatchLogsClient({
      region: "eu-central-1",
      roleArn: "arn:aws:iam::123456789012:role/log-writer",
      url: "http://127.0.0.1:5000", // emulator; leave unset for AWS
    });
    const wrapped = new CloudWatchLogsClient({ sdkClient: new AwsLogs({ region: "eu-central-1" }) });

    const health = await client.health();
    console.log(health.healthy, health.details);
    console.log(await client.ping(), "ms");
    await client.close();
    ```

=== "Rust"

    ```rust
    use grow_aws_cloudwatch_logs::{CloudWatchLogsClient, CloudWatchLogsConfig};

    let client = CloudWatchLogsClient::from_env()?;
    let explicit = CloudWatchLogsClient::new(
        CloudWatchLogsConfig::builder()
            .region("eu-central-1")
            .role_arn("arn:aws:iam::123456789012:role/log-writer")
            .log_group("my-app/api")
            .build()?,
    )?;
    let sdk = aws_sdk_cloudwatchlogs::Client::new(&aws_config::load_from_env().await);
    let wrapped = CloudWatchLogsClient::with_sdk_client(sdk)?;

    let health = client.health().await?;
    println!("{} {:?}", health.healthy, health.details);
    println!("{:?}", client.ping().await?);
    client.close().await;
    ```

=== "Go"

    ```go
    client, err := cloudwatchlogs.NewClientFromEnv()
    explicit, err := cloudwatchlogs.NewClient(cloudwatchlogs.Config{
    	Region:   "eu-central-1",
    	RoleARN:  "arn:aws:iam::123456789012:role/log-writer",
    	LogGroup: "my-app/api",
    })
    awsCfg, err := config.LoadDefaultConfig(ctx)
    wrapped, err := cloudwatchlogs.NewClient(cloudwatchlogs.Config{SDKClient: awscwl.NewFromConfig(awsCfg)})

    health, err := client.Health(ctx)
    fmt.Println(health.Healthy, health.Details)
    latency, err := client.Ping(ctx)
    _ = client.Close()
    ```

=== "Python"

    ```python
    import boto3
    import grow_aws_cloudwatch_logs as sdk

    client = sdk.CloudWatchLogsClient.from_env()
    explicit = sdk.CloudWatchLogsClient(
        sdk.CloudWatchLogsConfig(
            region="eu-central-1",
            role_arn="arn:aws:iam::123456789012:role/log-writer",
            log_group="my-app/api",
        )
    )
    wrapped = sdk.CloudWatchLogsClient(sdk.CloudWatchLogsConfig(sdk_client=boto3.client("logs")))

    health = client.health()
    print(health.healthy, health.details)
    print(client.ping(), "ms")
    client.close()
    ```

=== "Java"

    ```java
    CloudWatchLogsClient client = CloudWatchLogsClient.fromEnv();
    CloudWatchLogsClient explicit = CloudWatchLogsClient.create(
        CloudWatchLogsConfig.builder()
            .region("eu-central-1")
            .roleArn("arn:aws:iam::123456789012:role/log-writer")
            .logGroup("my-app/api")
            .build());
    CloudWatchLogsClient wrapped = CloudWatchLogsClient.create(
        CloudWatchLogsConfig.builder().sdkClient(CloudWatchLogsAsyncClient.create()).build());

    HealthStatus health = client.health();
    System.out.println(health.healthy() + " " + health.details());
    System.out.println(client.ping().toMillis() + " ms");
    client.close();
    ```
