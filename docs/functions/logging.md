# Logging (LogClient)

The shared Grow Codes logging interface on top of CloudWatch Logs. The generic parts (batch
processor, logger facade, redaction, framework adapters, `LogRecord` model) are described on the
[Logging](../logging.md) page; this page covers what is specific to CloudWatch Logs.

| Function | AWS operation | Notes |
| - | - | - |
| `logClient({group?, stream?, createGroup?, batch?, resource?, redaction?})` | `PutLogEvents` (via `exporter`) | a `BatchingLogClient` that writes each `LogRecord` as **normalized JSON** (timestamp, severity, body, attributes, resource, trace ids) to `group`/`stream` (defaults `config.logGroup` / `config.logStream`; the stream is created on first use, the group too with `createGroup`). Oversize records keep valid JSON (the body is truncated). `client.close()` flushes every log client it created |
| `exporter({group?, stream?, createGroup?})` | `PutLogEvents` | the `LogExporter` behind `logClient`, for your own batch processor; events the service rejects are reported as `rejected` with a `VALIDATION` error |
| `logQueryClient(group?)` | — | a `LogQueryClient` over one group |
| `search(query, {start?, end?, limit?, order?, cursor?})` | `FilterLogEvents` | `query` is a CloudWatch filter pattern. `order: "asc"` reads forward natively; `"desc"` (default) walks the time window **backwards in slices**, newest first (no Insights dependency). Results are normalized `LogRecord`s (JSON written by `logClient` is parsed back; plain lines become the body) with the stream in the `aws.log.stream` attribute; `nextCursor` continues the search |
| `tail(query, onRecord)` | `StartLiveTail` | live tail of the group, records normalized like `search`; returns a handle with `stop()` |
| `listStreams()` | `DescribeLogStreams` | all stream names of the group |
| `listLabels()` | — | `UNSUPPORTED` (CloudWatch Logs has no label index) |
| `waitForLog(query, {timeout?, interval?})` | `FilterLogEvents` | test helper: polls `search` until a record matches |

=== "TypeScript"

    ```ts
    const logs = client.logClient({ group: "my-app/api", resource: { "service.name": "api" } });
    logs.logger("http").info("request served", { status: 200 });
    await logs.flush();
    const q = client.logQueryClient({ group: "my-app/api" });
    const { records, nextCursor } = await q.search("request", { limit: 20 }); // newest first
    ```

=== "Rust"

    ```rust
    let logs = client.log_client(LogClientOptions { exporter: ExporterOptions { group: Some("my-app/api".into()), ..Default::default() }, ..Default::default() })?;
    logs.logger(Some("http"), attrs! {}).info("request served", attrs! { "status" => 200 });
    logs.flush(None).await?;
    let q = client.log_query_client(Some("my-app/api"))?;
    let page = q.search("request", SearchOptions::default().limit(20)).await?;
    ```

=== "Go"

    ```go
    logs, err := client.LogClient(cloudwatchlogs.LogClientConfig{Group: "my-app/api"})
    logs.Logger("http", nil).Info(ctx, "request served", "status", 200)
    err = logs.Flush(ctx)
    q, err := client.LogQueryClient("my-app/api")
    page, err := q.Search(ctx, "request", cloudwatchlogs.SearchOptions{Limit: 20})
    ```

=== "Python"

    ```python
    logs = client.log_client(group="my-app/api", resource={"service.name": "api"})
    logs.logger("http").info("request served", {"status": 200})
    logs.flush()
    q = client.log_query_client("my-app/api")
    page = q.search("request", limit=20)
    ```

=== "Java"

    ```java
    BatchingLogClient logs = client.logClient(CloudWatchLogClientOptions.defaults().withTarget("my-app/api", null));
    logs.logger("http").info("request served", Map.of("status", 200));
    logs.flush(Duration.ofSeconds(5));
    LogSearchResult page = client.logQueryClient("my-app/api").search("request", SearchOptions.builder().limit(20).build());
    ```
