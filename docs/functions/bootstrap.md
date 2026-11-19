# Bootstrap

Create, inspect and remove log groups and streams. All `ensure…` functions are idempotent and safe
to call at every start of an application. Group arguments of the tag functions and `logGroupArn`
accept a name or an ARN; everything else sends `logGroupName`.

| Function | AWS operation | Notes |
| - | - | - |
| `ensureLogGroup(name, {retentionDays?, kmsKeyId?, tags?})` | `CreateLogGroup`, `DescribeLogGroups`, `TagResource`, `PutRetentionPolicy` / `DeleteRetentionPolicy` | returns `{created, arn}` (ARN without `:*`). On an existing group the requested tags are **added** (never removed) and the retention is re-applied. `retentionDays` unset leaves the retention unchanged, `0` removes it (never expire). A different KMS key on an existing group → `CONFLICT` (keys are never re-associated). Invalid names or retention values → `VALIDATION` |
| `ensureLogStream(group, stream, {createGroup?})` | `CreateLogStream` | returns `{created}`; a missing group → `NOT_FOUND` unless `createGroup` |
| `putRetentionPolicy(group, days)` | `PutRetentionPolicy` / `DeleteRetentionPolicy` | `days` one of 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653; `0` (or `null` / `None`) removes the policy |
| `deleteLogGroup(name, {ignoreMissing?})` / `deleteLogStream(group, stream, {ignoreMissing?})` | `DeleteLogGroup` / `DeleteLogStream` | `true` when something was deleted; missing → `NOT_FOUND`, or `false` with `ignoreMissing` |
| `describeLogGroups({prefix?, pageSize?, limit?})` | `DescribeLogGroups` | lazy iterator over all pages (`pageSize` ≤ 50) |
| `describeLogStreams(group, {prefix?, orderBy?, descending?, pageSize?, limit?})` | `DescribeLogStreams` | lazy iterator; order by name or last event time |
| `getLogGroup(name)` | `DescribeLogGroups` | the group named exactly `name`, or none |
| `tagLogGroup(group, tags)` / `untagLogGroup(group, keys)` / `listLogGroupTags(group)` | `TagResource` / `UntagResource` / `ListTagsForResource` | tags as maps |
| `logGroupArn(group)` | — / `DescribeLogGroups` | built from region and `accountId` when configured, otherwise read with `DescribeLogGroups` (no STS permission needed) |

=== "TypeScript"

    ```ts
    const { created, arn } = await client.ensureLogGroup("my-app/api", { retentionDays: 30, tags: { team: "platform" } });
    await client.ensureLogStream("my-app/api", "web-1");
    await client.putRetentionPolicy("my-app/api", 0); // 0 or null: never expire
    for await (const g of client.describeLogGroups({ prefix: "my-app/" })) console.log(g.name, g.retentionDays);
    for await (const s of client.describeLogStreams("my-app/api")) console.log(s.name);
    await client.tagLogGroup("my-app/api", { env: "prod" });
    console.log(await client.listLogGroupTags("my-app/api"));
    await client.deleteLogStream("my-app/api", "web-1", { ignoreMissing: true });
    ```

=== "Rust"

    ```rust
    let r = client.ensure_log_group("my-app/api", EnsureLogGroupOptions::default().retention_days(30).tag("team", "platform")).await?;
    client.ensure_log_stream("my-app/api", "web-1", EnsureLogStreamOptions::default()).await?;
    client.put_retention_policy("my-app/api", None).await?; // None or Some(0): never expire
    let groups = client.describe_log_groups(DescribeLogGroupsOptions { prefix: Some("my-app/".into()), ..Default::default() }).try_collect().await?;
    let streams = client.describe_log_streams("my-app/api", DescribeLogStreamsOptions::default()).try_collect().await?;
    client.tag_log_group("my-app/api", [("env".to_owned(), "prod".to_owned())].into()).await?;
    client.delete_log_stream("my-app/api", "web-1", DeleteOptions { ignore_missing: true }).await?;
    ```

=== "Go"

    ```go
    r, err := client.EnsureLogGroup(ctx, "my-app/api", cloudwatchlogs.EnsureLogGroupOptions{RetentionDays: cloudwatchlogs.Days(30), Tags: map[string]string{"team": "platform"}})
    _, err = client.EnsureLogStream(ctx, "my-app/api", "web-1", cloudwatchlogs.EnsureLogStreamOptions{})
    err = client.PutRetentionPolicy(ctx, "my-app/api", 0) // 0: never expire
    for g, err := range client.DescribeLogGroups(ctx, cloudwatchlogs.DescribeLogGroupsOptions{Prefix: "my-app/"}) {
    	if err != nil {
    		return err
    	}
    	fmt.Println(g.Name, g.RetentionDays)
    }
    err = client.TagLogGroup(ctx, "my-app/api", map[string]string{"env": "prod"})
    _, err = client.DeleteLogStream(ctx, "my-app/api", "web-1", cloudwatchlogs.DeleteOptions{IgnoreMissing: true})
    ```

=== "Python"

    ```python
    r = client.ensure_log_group("my-app/api", retention_days=30, tags={"team": "platform"})
    client.ensure_log_stream("my-app/api", "web-1")
    client.put_retention_policy("my-app/api", None)  # None or 0: never expire
    names = [g.name for g in client.describe_log_groups(prefix="my-app/")]
    streams = [s.name for s in client.describe_log_streams("my-app/api")]
    client.tag_log_group("my-app/api", {"env": "prod"})
    client.delete_log_stream("my-app/api", "web-1", ignore_missing=True)
    ```

=== "Java"

    ```java
    EnsureLogGroupResult r = client.ensureLogGroup("my-app/api",
        EnsureLogGroupOptions.builder().retentionDays(30).tags(Map.of("team", "platform")).build());
    client.ensureLogStream("my-app/api", "web-1");
    client.putRetentionPolicy("my-app/api", 0); // 0: never expire
    for (LogGroupInfo g : client.describeLogGroups(DescribeLogGroupsOptions.builder().prefix("my-app/").build())) {
        System.out.println(g.name());
    }
    client.tagLogGroup("my-app/api", Map.of("env", "prod"));
    client.deleteLogStream("my-app/api", "web-1", true);
    ```
