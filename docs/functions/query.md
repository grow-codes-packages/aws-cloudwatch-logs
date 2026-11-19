# Query

Read events back: a pattern search over a group or one stream in order.

| Function | AWS operation | Notes |
| - | - | - |
| `filterLogEvents({group?, streams?, streamPrefix?, pattern?, start?, end?, pageSize?, limit?})` | `FilterLogEvents` | lazy iterator over all pages, ordered by timestamp then event id (`group` defaults to `config.logGroup`). `streams` (1–100 names) and `streamPrefix` are mutually exclusive. `pattern` uses the [CloudWatch filter pattern syntax](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html) (terms, `"phrases"`, JSON and space-delimited patterns) |
| `filterLogEventsPage(options, nextToken?)` | `FilterLogEvents` | one page `{events, nextToken}`; AWS may return empty pages that still carry a token |
| `getLogEvents(group, stream, {startFromHead?, start?, end?, pageSize?, limit?})` | `GetLogEvents` | lazy iterator; forward from the head (`startFromHead`) or backward from the newest event; ends when the token repeats |

=== "TypeScript"

    ```ts
    for await (const e of client.filterLogEvents({ group: "my-app/api", pattern: '"timed out"', start: new Date(Date.now() - 3_600_000) })) {
      console.log(e.logStreamName, e.message);
    }
    const page = await client.filterLogEventsPage({ group: "my-app/api", pageSize: 100 });
    const head = client.getLogEvents("my-app/api", "web-1", { startFromHead: true, limit: 50 });
    ```

=== "Rust"

    ```rust
    let hits = client.filter_log_events(FilterLogEventsOptions { group: Some("my-app/api".into()), pattern: Some("\"timed out\"".into()), ..Default::default() }).try_collect().await?;
    let page = client.filter_log_events_page(FilterLogEventsOptions { group: Some("my-app/api".into()), ..Default::default() }, None).await?;
    let head = client.get_log_events("my-app/api", "web-1", GetLogEventsOptions { start_from_head: true, limit: Some(50), ..Default::default() }).try_collect().await?;
    ```

=== "Go"

    ```go
    for e, err := range client.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: "my-app/api", Pattern: `"timed out"`, Start: time.Now().Add(-time.Hour)}) {
    	if err != nil {
    		return err
    	}
    	fmt.Println(e.LogStreamName, e.Message)
    }
    page, err := client.FilterLogEventsPage(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: "my-app/api", PageSize: 100}, "")
    head := client.GetLogEvents(ctx, "my-app/api", "web-1", cloudwatchlogs.GetLogEventsOptions{StartFromHead: true, Limit: 50})
    ```

=== "Python"

    ```python
    for e in client.filter_log_events(group="my-app/api", pattern='"timed out"', start=datetime.now(timezone.utc) - timedelta(hours=1)):
        print(e.log_stream_name, e.message)
    page = client.filter_log_events_page(group="my-app/api", page_size=100)
    head = list(client.get_log_events("my-app/api", "web-1", start_from_head=True, limit=50))
    ```

=== "Java"

    ```java
    for (FilteredLogEvent e : client.filterLogEvents(FilterLogEventsOptions.builder()
            .group("my-app/api").pattern("\"timed out\"").start(Instant.now().minusSeconds(3600)).build())) {
        System.out.println(e.logStreamName().orElse("") + " " + e.message());
    }
    FilterLogEventsPage page = client.filterLogEventsPage(FilterLogEventsOptions.builder().group("my-app/api").pageSize(100).build(), null);
    var head = client.getLogEvents("my-app/api", "web-1", GetLogEventsOptions.builder().startFromHead(true).limit(50).build());
    ```
