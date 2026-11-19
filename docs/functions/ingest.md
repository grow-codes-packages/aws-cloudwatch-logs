# Ingest

Write log events and Embedded Metric Format (EMF) documents.

| Function | AWS operation | Notes |
| - | - | - |
| `putLogEvents(group, stream, events, {autoCreate?, createGroup?, maxEventBytes?, oversizePolicy?})` | `PutLogEvents` | events are **stably sorted** by timestamp (missing timestamp = now) and split client side into calls within the service limits: at most 10,000 events and 1,048,576 bytes per call (UTF-8 message bytes + 26 bytes per event) and a 24-hour span. Events larger than `maxEventBytes` (default 256 KiB) follow `oversizePolicy`: `truncate` (UTF-8 safe, appends ` …[truncated N bytes]`), `drop` or `error` (`VALIDATION`). A missing stream is created once when `autoCreate` is on (default). Returns `{accepted, rejected, truncated, dropped, batches, rejectedInfo}` |
| `emf(metric, value, dimensions, {namespace?, unit?, timestamp?, properties?, group?, stream?})` | `PutLogEvents` | builds an EMF document and writes it as one event (default group/stream from the config); returns the **document sent**; an oversize document or an event the service rejects → `VALIDATION` |
| `buildEmf(metric, value, dimensions, options)` | — | the EMF document only (value: a finite number or 1–100 numbers; at most 30 dimensions; units as in CloudWatch metrics; default namespace `GrowSdk`) |
| `truncateMessage(message, maxBytes)` (Go, Rust) | — | the truncation helper used by `putLogEvents` |

`rejectedLogEventsInfo` is reported per batch. As returned by AWS, `tooOldLogEventEndIndex` and
`expiredLogEventEndIndex` are **exclusive** end indexes (events `0 … index-1` were rejected) and
`tooNewLogEventStartIndex` is inclusive (events `index …` were rejected); the indexes refer to the
timestamp-sorted batch. Rejected events are never retried. Moto 5.2.4 returns the end indexes one
lower than AWS (see [Compatibility](../compatibility.md)).

Throttling and transient 5xx answers are retried by the AWS SDK. A response lost after the service
stored a batch can duplicate those events.

=== "TypeScript"

    ```ts
    const res = await client.putLogEvents("my-app/api", "web-1", events, { oversizePolicy: "drop" });
    console.log(res.accepted, res.rejected, res.truncated, res.dropped, res.batches, res.rejectedInfo);
    const doc = await client.emf("latency", 42, { service: "api" }, { unit: "Milliseconds", namespace: "MyApp" });
    ```

=== "Rust"

    ```rust
    let res = client.put_log_events("my-app/api", "web-1", events, PutLogEventsOptions::default()).await?;
    let dims: BTreeMap<String, String> = [("service".to_owned(), "api".to_owned())].into();
    let doc: String = client.emf("latency", 42.0, &dims, EmfOptions::default().unit("Milliseconds").namespace("MyApp")).await?;
    ```

=== "Go"

    ```go
    res, err := client.PutLogEvents(ctx, "my-app/api", "web-1", events, cloudwatchlogs.PutLogEventsOptions{OversizePolicy: cloudwatchlogs.OversizeDrop})
    doc, err := client.EMF(ctx, "latency", 42, map[string]string{"service": "api"}, cloudwatchlogs.EMFOptions{Unit: "Milliseconds", Namespace: "MyApp"})
    ```

=== "Python"

    ```python
    res = client.put_log_events("my-app/api", "web-1", events, oversize_policy="drop")
    doc = client.emf("latency", 42, {"service": "api"}, unit="Milliseconds", namespace="MyApp")
    ```

=== "Java"

    ```java
    PutLogEventsResult res = client.putLogEvents("my-app/api", "web-1", events,
        PutLogEventsOptions.builder().oversizePolicy(OversizePolicy.DROP).build());
    Map<String, Object> doc = client.emf("latency", 42, Map.of("service", "api"),
        Emf.Options.defaults().withUnit("Milliseconds").withNamespace("MyApp"));
    ```
