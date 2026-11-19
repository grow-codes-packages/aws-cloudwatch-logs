# Logs Insights

Run [CloudWatch Logs Insights](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/AnalyzingLogData.html)
queries.

| Function | AWS operation | Notes |
| - | - | - |
| `startQuery(query, {groups?, start?, end?, limit?})` | `StartQuery` | returns the query id; `groups` default to `config.logGroup`; `start` defaults to one hour ago, `end` to now |
| `getQueryResults(queryId)` | `GetQueryResults` | `{queryId, status, rows, statistics}`; rows are maps of field → value |
| `query(query, {groups?, start?, end?, limit?, timeout?, pollInterval?})` | `StartQuery`, `GetQueryResults`, `StopQuery` | polls until `Complete` (default timeout 60 s, poll interval 1 s); `LimitExceededException` (too many concurrent queries) is retried; `Failed` → `UNAVAILABLE`, `Cancelled` → `CANCELLED`, `Timeout` → `TIMEOUT`; on timeout or cancellation the query is stopped |
| `stopQuery(queryId)` | `StopQuery` | `true` when the query was running |
| `describeQueries({group?, status?, maxResults?})` | `DescribeQueries` | recent queries of the account |
| `parseQueryTimestamp(value)` (Go, Rust, Java `parseInsightsTimestamp`) | — | parses Insights `@timestamp` values (`2026-11-19 07:00:00.000` on AWS, epoch ms on Moto) |

=== "TypeScript"

    ```ts
    const r = await client.query("fields @timestamp, @message | filter @message like /ERROR/ | limit 20", {
      groups: ["my-app/api"], start: new Date(Date.now() - 3_600_000), end: new Date(), timeoutMs: 30_000,
    });
    for (const row of r.rows) console.log(row["@timestamp"], row["@message"]);
    const id = await client.startQuery("stats count(*) by bin(5m)", { groups: ["my-app/api"] });
    const partial = await client.getQueryResults(id);
    await client.stopQuery(id);
    ```

=== "Rust"

    ```rust
    let r = client.query("fields @timestamp, @message | limit 20", QueryOptions::default().groups(["my-app/api"]).range(Some(SystemTime::now() - Duration::from_secs(3600)), None)).await?;
    let id = client.start_query("stats count(*) by bin(5m)", QueryOptions::default().groups(["my-app/api"])).await?;
    let partial = client.get_query_results(&id).await?;
    client.stop_query(&id).await?;
    ```

=== "Go"

    ```go
    r, err := client.Query(ctx, "fields @timestamp, @message | limit 20", cloudwatchlogs.QueryOptions{Groups: []string{"my-app/api"}, Start: time.Now().Add(-time.Hour)})
    for _, row := range r.Rows {
    	fmt.Println(row["@timestamp"], row["@message"])
    }
    id, err := client.StartQuery(ctx, "stats count(*) by bin(5m)", cloudwatchlogs.QueryOptions{Groups: []string{"my-app/api"}})
    partial, err := client.GetQueryResults(ctx, id)
    _, err = client.StopQuery(ctx, id)
    ```

=== "Python"

    ```python
    r = client.query("fields @timestamp, @message | limit 20", groups=["my-app/api"], start=datetime.now(timezone.utc) - timedelta(hours=1))
    qid = client.start_query("stats count(*) by bin(5m)", groups=["my-app/api"])
    partial = client.get_query_results(qid)
    client.stop_query(qid)
    ```

=== "Java"

    ```java
    QueryResults r = client.query("fields @timestamp, @message | limit 20",
        QueryOptions.builder().groups(List.of("my-app/api")).start(Instant.now().minusSeconds(3600)).build());
    String id = client.startQuery("stats count(*) by bin(5m)", QueryOptions.builder().groups(List.of("my-app/api")).build());
    QueryResults partial = client.getQueryResults(id);
    client.stopQuery(id);
    ```
