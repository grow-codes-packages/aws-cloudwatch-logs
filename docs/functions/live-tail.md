# Live tail

Stream new events as they arrive with [Live Tail](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatchLogs_LiveTail.html).

| Function | AWS operation | Notes |
| - | - | - |
| `startLiveTail({groups, streams?, streamPrefixes?, pattern?}, onEvent, {onUpdate?, onError?})` | `StartLiveTail` (event stream) | 1–10 groups (names are turned into ARNs, see `logGroupArn`); `streams` / `streamPrefixes` need exactly one group. Returns after the `sessionStart` frame with a handle (`sessionId`, `close()` / `stop()`, done signal, last error). `onUpdate` gets each `sessionUpdate` (count, sampled flag); stream exceptions (`SessionStreamingException`, `SessionTimeoutException`) go to `onError` and end the session |

On AWS a session lasts up to 3 hours and sends keep-alive updates; the per-attempt
`requestTimeout` does not apply to it. `client.close()` stops every open session. The AWS SDKs use
the endpoint host prefix `streaming-` (botocore, aws-sdk-rust) or `stream-`; set `url` to a host
that resolves with either prefix when you use an emulator.

=== "TypeScript"

    ```ts
    const handle = await client.startLiveTail({ groups: ["my-app/api"], pattern: "ERROR" }, (e) => console.log(e.message), {
      onUpdate: (u) => console.log("batch of", u.count),
    });
    await handle.close();
    ```

=== "Rust"

    ```rust
    let session = client.start_live_tail(LiveTailOptions::default().groups(["my-app/api"]).pattern("ERROR"), Arc::new(|e| println!("{}", e.message))).await?;
    session.close();
    ```

=== "Go"

    ```go
    s, err := client.StartLiveTail(ctx, cloudwatchlogs.LiveTailOptions{Groups: []string{"my-app/api"}, Pattern: "ERROR"}, func(e cloudwatchlogs.LiveTailEvent) {
    	fmt.Println(e.Message)
    })
    defer s.Close()
    ```

=== "Python"

    ```python
    session = client.start_live_tail(["my-app/api"], lambda e: print(e.message), pattern="ERROR")
    session.close()
    ```

=== "Java"

    ```java
    LiveTailHandle h = client.startLiveTail(
        LiveTailRequest.builder().groups(List.of("my-app/api")).pattern("ERROR").build(),
        e -> System.out.println(e.message()));
    h.close();
    ```
