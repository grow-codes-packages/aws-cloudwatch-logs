package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cloudwatchlogs "github.com/grow-codes-packages/aws-cloudwatch-logs/go"
)

// setup loads /bootstrap/env and returns an admin client plus a unique prefix.
func setup(t *testing.T) (*cloudwatchlogs.Client, string) {
	t.Helper()
	bootstrap := os.Getenv("GROW_BOOTSTRAP_ENV")
	if bootstrap == "" {
		bootstrap = "/bootstrap/env"
	}
	loadBootstrapEnv(t, bootstrap)
	if os.Getenv("GROW_AWS_CLOUDWATCH_LOGS_URL") == "" {
		t.Skip("GROW_AWS_CLOUDWATCH_LOGS_URL is not set; run through docker/compose.yml")
	}
	c, err := cloudwatchlogs.NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	var b [4]byte
	_, _ = rand.Read(b[:])
	return c, "it-go-" + hex.EncodeToString(b[:])
}

// must returns v or panics with err (the test fails with the error and stack).
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func collect[T any](t *testing.T, seq func(func(T, error) bool)) []T {
	t.Helper()
	var out []T
	for v, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func wantCode(t *testing.T, err error, code cloudwatchlogs.ErrorCode) {
	t.Helper()
	if cloudwatchlogs.CodeOf(err) != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestIntegration(t *testing.T) {
	c, prefix := setup(t)
	ctx := t.Context()
	group := prefix + "/app"
	other := prefix + "/other"
	t.Cleanup(func() {
		for _, g := range []string{group, other, prefix + "/tail"} {
			_, _ = c.DeleteLogGroup(context.Background(), g, cloudwatchlogs.DeleteOptions{IgnoreMissing: true})
		}
	})

	t.Run("core", func(t *testing.T) {
		hs := must(c.Health(ctx))
		if !hs.Healthy || hs.Status != "ok" {
			t.Fatalf("health %+v", hs)
		}
		if must(c.Ping(ctx)) <= 0 {
			t.Fatal("ping latency")
		}
		_, err := c.ServerVersion(ctx)
		wantCode(t, err, cloudwatchlogs.ErrorCodeUnsupported)
		if c.Region() == "" || c.SDK() == nil || c.Config().LogGroup == "" {
			t.Fatal("accessors")
		}
		bad, err := cloudwatchlogs.NewClient(cloudwatchlogs.Config{
			CommonConfig: cloudwatchlogs.CommonConfig{URL: c.Config().URL}, Region: c.Region(),
			AccessKeyID: "AKIANOTAREALKEY00000", SecretAccessKey: cloudwatchlogs.NewSecret("wrong-secret"),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = bad.Close() }()
		_, err = bad.Ping(ctx)
		wantCode(t, err, cloudwatchlogs.ErrorCodeAuthentication)
		if hs := must(bad.Health(ctx)); !hs.Healthy || hs.Details["auth"] != "failed" {
			t.Fatalf("health with bad credentials %+v", hs)
		}
	})

	t.Run("bootstrap functions", func(t *testing.T) {
		res := must(c.EnsureLogGroup(ctx, group, cloudwatchlogs.EnsureLogGroupOptions{RetentionDays: 7, Tags: map[string]string{"team": "sdk"}}))
		if !res.Created || !strings.HasSuffix(res.ARN, ":log-group:"+group) {
			t.Fatalf("create %+v", res)
		}
		res = must(c.EnsureLogGroup(ctx, group, cloudwatchlogs.EnsureLogGroupOptions{RetentionDays: 14, Tags: map[string]string{"env": "it"}}))
		if res.Created {
			t.Fatal("second ensure must not create")
		}
		tags := must(c.ListLogGroupTags(ctx, group))
		if tags["team"] != "sdk" || tags["env"] != "it" {
			t.Fatalf("tags %v", tags)
		}
		if err := c.TagLogGroup(ctx, group, map[string]string{"x": "1"}); err != nil {
			t.Fatal(err)
		}
		if err := c.UntagLogGroup(ctx, res.ARN, []string{"team"}); err != nil {
			t.Fatal(err)
		}
		tags = must(c.ListLogGroupTags(ctx, group))
		if tags["x"] != "1" || tags["team"] != "" {
			t.Fatalf("tags after tag/untag %v", tags)
		}
		groups := collect(t, c.DescribeLogGroups(ctx, cloudwatchlogs.DescribeLogGroupsOptions{Prefix: prefix + "/", PageSize: 1}))
		if len(groups) != 1 || groups[0].Name != group || groups[0].RetentionDays != 14 {
			t.Fatalf("groups %+v", groups)
		}
		if g, ok, err := c.GetLogGroup(ctx, group); err != nil || !ok || g.RetentionDays != 14 || g.ARN != res.ARN {
			t.Fatalf("getLogGroup %+v %t %v", g, ok, err)
		}
		if _, ok, err := c.GetLogGroup(ctx, prefix+"/missing"); ok || err != nil {
			t.Fatalf("getLogGroup missing %t %v", ok, err)
		}
		if arn := must(c.LogGroupARN(ctx, group)); arn != res.ARN {
			t.Fatalf("logGroupArn %s != %s", arn, res.ARN)
		}
		if err := c.PutRetentionPolicy(ctx, group, 30); err != nil {
			t.Fatal(err)
		}
		if g := collect(t, c.DescribeLogGroups(ctx, cloudwatchlogs.DescribeLogGroupsOptions{Prefix: group})); g[0].RetentionDays != 30 {
			t.Fatalf("retention %+v", g[0])
		}
		if err := c.PutRetentionPolicy(ctx, group, 0); err != nil {
			t.Fatal(err)
		}
		if g := collect(t, c.DescribeLogGroups(ctx, cloudwatchlogs.DescribeLogGroupsOptions{Prefix: group})); g[0].RetentionDays != 0 {
			t.Fatalf("retention removed %+v", g[0])
		}
		for _, s := range []string{"s1", "s2", "s3"} {
			if r := must(c.EnsureLogStream(ctx, group, s, cloudwatchlogs.EnsureLogStreamOptions{})); !r.Created {
				t.Fatalf("stream %s not created", s)
			}
		}
		if r := must(c.EnsureLogStream(ctx, group, "s1", cloudwatchlogs.EnsureLogStreamOptions{})); r.Created {
			t.Fatal("existing stream")
		}
		streams := collect(t, c.DescribeLogStreams(ctx, group, cloudwatchlogs.DescribeLogStreamsOptions{PageSize: 2}))
		if len(streams) != 3 || streams[0].Name != "s1" {
			t.Fatalf("streams %+v", streams)
		}
		if l := collect(t, c.DescribeLogStreams(ctx, group, cloudwatchlogs.DescribeLogStreamsOptions{Limit: 2})); len(l) != 2 {
			t.Fatalf("stream limit %d", len(l))
		}
		desc := collect(t, c.DescribeLogStreams(ctx, group, cloudwatchlogs.DescribeLogStreamsOptions{Descending: true}))
		if desc[0].Name != "s3" {
			t.Fatalf("descending %+v", desc)
		}
		if !must(c.DeleteLogStream(ctx, group, "s3", cloudwatchlogs.DeleteOptions{})) {
			t.Fatal("delete stream")
		}
		if must(c.DeleteLogStream(ctx, group, "s3", cloudwatchlogs.DeleteOptions{IgnoreMissing: true})) {
			t.Fatal("second delete")
		}
		_, err := c.DeleteLogStream(ctx, group, "s3", cloudwatchlogs.DeleteOptions{})
		wantCode(t, err, cloudwatchlogs.ErrorCodeNotFound)
		// a stream with createGroup, then the group is deleted
		if r := must(c.EnsureLogStream(ctx, other, "x", cloudwatchlogs.EnsureLogStreamOptions{CreateGroup: true})); !r.Created {
			t.Fatal("createGroup")
		}
		if !must(c.DeleteLogGroup(ctx, other, cloudwatchlogs.DeleteOptions{})) {
			t.Fatal("delete group")
		}
		if len(collect(t, c.DescribeLogGroups(ctx, cloudwatchlogs.DescribeLogGroupsOptions{Prefix: other}))) != 0 {
			t.Fatal("group still listed")
		}
		_, err = c.DeleteLogGroup(ctx, other, cloudwatchlogs.DeleteOptions{})
		wantCode(t, err, cloudwatchlogs.ErrorCodeNotFound)
		// KMS mismatch on an existing group
		kms := prefix + "/kms"
		must(c.EnsureLogGroup(ctx, kms, cloudwatchlogs.EnsureLogGroupOptions{KMSKeyID: "arn:aws:kms:us-east-1:123456789012:key/k1"}))
		_, err = c.EnsureLogGroup(ctx, kms, cloudwatchlogs.EnsureLogGroupOptions{KMSKeyID: "k2"})
		wantCode(t, err, cloudwatchlogs.ErrorCodeConflict)
		must(c.DeleteLogGroup(ctx, kms, cloudwatchlogs.DeleteOptions{}))
		_, err = c.EnsureLogGroup(ctx, "bad name!", cloudwatchlogs.EnsureLogGroupOptions{})
		wantCode(t, err, cloudwatchlogs.ErrorCodeValidation)
	})

	marker := "marker-" + prefix
	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	t.Run("ingest functions", func(t *testing.T) {
		events := []cloudwatchlogs.LogEvent{
			{Timestamp: base.Add(2 * time.Second), Message: "third " + marker + " ERROR"},
			{Timestamp: base, Message: "first " + marker},
			{Timestamp: base.Add(time.Second), Message: "second " + marker + " ünïcode ✓"},
		}
		res := must(c.PutLogEvents(ctx, group, "s1", events, cloudwatchlogs.PutLogEventsOptions{}))
		if res.Accepted != 3 || res.Batches != 1 {
			t.Fatalf("put %+v", res)
		}
		// auto-created stream
		res = must(c.PutLogEvents(ctx, group, "auto", []cloudwatchlogs.LogEvent{{Timestamp: base, Message: "auto " + marker}}, cloudwatchlogs.PutLogEventsOptions{}))
		if res.Accepted != 1 {
			t.Fatalf("auto %+v", res)
		}
		// oversize truncation round trip
		big := strings.Repeat("x", 300_000) + marker
		res = must(c.PutLogEvents(ctx, group, "s2", []cloudwatchlogs.LogEvent{{Timestamp: base, Message: big}}, cloudwatchlogs.PutLogEventsOptions{}))
		if res.Truncated != 1 || res.Accepted != 1 {
			t.Fatalf("truncate %+v", res)
		}
		got := collect(t, c.GetLogEvents(ctx, group, "s2", cloudwatchlogs.GetLogEventsOptions{StartFromHead: true}))
		if len(got) != 1 || len(got[0].Message) > cloudwatchlogs.DefaultMaxEventBytes || !strings.Contains(got[0].Message, "truncated") {
			t.Fatalf("truncated read back %d", len(got))
		}
		// 10,000-event and > 1 MiB inputs are split client side
		many := make([]cloudwatchlogs.LogEvent, 10_001)
		for i := range many {
			many[i] = cloudwatchlogs.LogEvent{Timestamp: base, Message: "bulk"}
		}
		if res := must(c.PutLogEvents(ctx, group, "bulk", many, cloudwatchlogs.PutLogEventsOptions{})); res.Batches != 2 || res.Accepted != 10_001 {
			t.Fatalf("bulk %+v", res)
		}
		// EMF
		doc := must(cloudwatchlogs.BuildEMF("latency", 42, map[string]string{"service": "api"}, cloudwatchlogs.EMFOptions{Namespace: "GrowSdkIT", Unit: "Milliseconds"}))
		if !strings.Contains(doc, `"CloudWatchMetrics"`) {
			t.Fatal(doc)
		}
		emfRes := must(c.EMF(ctx, "latency", 42, map[string]string{"service": "api"}, cloudwatchlogs.EMFOptions{Group: group, Stream: "emf", Properties: map[string]any{"marker": marker}}))
		if emfRes.Accepted != 1 {
			t.Fatalf("emf %+v", emfRes)
		}
		emf := collect(t, c.GetLogEvents(ctx, group, "emf", cloudwatchlogs.GetLogEventsOptions{StartFromHead: true}))
		var parsed map[string]any
		if len(emf) != 1 || json.Unmarshal([]byte(emf[0].Message), &parsed) != nil || parsed["latency"] != float64(42) || parsed["marker"] != marker {
			t.Fatalf("emf read back %+v", emf)
		}
	})

	t.Run("query functions", func(t *testing.T) {
		head := collect(t, c.GetLogEvents(ctx, group, "s1", cloudwatchlogs.GetLogEventsOptions{StartFromHead: true, PageSize: 1}))
		if len(head) != 3 || !strings.HasPrefix(head[0].Message, "first") || !strings.HasPrefix(head[2].Message, "third") || !head[0].Timestamp.Equal(base) {
			t.Fatalf("getLogEvents %+v", head)
		}
		latest := collect(t, c.GetLogEvents(ctx, group, "s1", cloudwatchlogs.GetLogEventsOptions{Limit: 1}))
		if len(latest) != 1 {
			t.Fatalf("limit %+v", latest)
		}
		filtered := collect(t, c.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: group, Streams: []string{"s1"}, Pattern: "ERROR"}))
		if len(filtered) != 1 || !strings.HasPrefix(filtered[0].Message, "third") || filtered[0].LogStreamName != "s1" {
			t.Fatalf("filter %+v", filtered)
		}
		paged := collect(t, c.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: group, Pattern: `"` + marker + `"`, PageSize: 1, Start: base.Add(-time.Second), End: time.Now().Add(time.Minute)}))
		if len(paged) < 5 {
			t.Fatalf("filter pages %d", len(paged))
		}
		if l := collect(t, c.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: group, Pattern: marker, Limit: 2})); len(l) != 2 {
			t.Fatalf("filter limit %d", len(l))
		}
		page := must(c.FilterLogEventsPage(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: group, Streams: []string{"s1"}, PageSize: 2}, ""))
		if len(page.Events) != 2 || page.NextToken == "" {
			t.Fatalf("page 1 %+v", page)
		}
		page = must(c.FilterLogEventsPage(ctx, cloudwatchlogs.FilterLogEventsOptions{Group: group, Streams: []string{"s1"}, PageSize: 2}, page.NextToken))
		if len(page.Events) != 1 || !strings.HasPrefix(page.Events[0].Message, "third") {
			t.Fatalf("page 2 %+v", page)
		}

		id := must(c.StartQuery(ctx, "fields @timestamp, @message | sort @timestamp asc | limit 3", cloudwatchlogs.QueryOptions{Groups: []string{group}, Start: base.Add(-time.Minute), End: time.Now().Add(time.Minute)}))
		if id == "" {
			t.Fatal("query id")
		}
		qr := must(c.GetQueryResults(ctx, id))
		if !qr.Status.Terminal() && qr.Status != cloudwatchlogs.QueryRunning && qr.Status != cloudwatchlogs.QueryScheduled {
			t.Fatalf("status %s", qr.Status)
		}
		res := must(c.Query(ctx, "fields @timestamp, @message | sort @timestamp desc | limit 2", cloudwatchlogs.QueryOptions{Groups: []string{group}, Start: base.Add(-time.Minute), End: time.Now().Add(time.Minute)}))
		if res.Status != cloudwatchlogs.QueryComplete || len(res.Rows) != 2 || res.Rows[0]["@message"] == "" {
			t.Fatalf("query %+v", res)
		}
		if _, err := cloudwatchlogs.ParseQueryTimestamp(res.Rows[0]["@timestamp"]); err != nil {
			t.Fatal(err)
		}
		qs := must(c.DescribeQueries(ctx, cloudwatchlogs.DescribeQueriesOptions{Group: group}))
		found := slices.ContainsFunc(qs, func(q cloudwatchlogs.QueryInfo) bool { return q.QueryID == id })
		if !found {
			t.Fatalf("describeQueries %+v", qs)
		}
		// StopQuery is not implemented by Moto 5.2.4 (HTTP 500); unit tests cover it.
		if os.Getenv("GROW_TEST_EMULATOR") == "moto" {
			_, err := c.StopQuery(ctx, id)
			wantCode(t, err, cloudwatchlogs.ErrorCodeUnavailable)
		} else if _, err := c.StopQuery(ctx, id); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("log client, exporter and search", func(t *testing.T) {
		cfg := must(cloudwatchlogs.ConfigFromEnvWith(cloudwatchlogs.Config{LogGroup: group, LogStream: "logclient"}, nil))
		lc := must(cloudwatchlogs.NewClient(cfg))
		defer func() { _ = lc.Close() }()
		logs := must(lc.LogClient(cloudwatchlogs.LogClientConfig{LogClientOptions: cloudwatchlogs.LogClientOptions{Resource: map[string]cloudwatchlogs.AttrValue{"service.name": "it-go"}}}))
		q := must(lc.LogQueryClient(""))
		if q.Group() != group {
			t.Fatal(q.Group())
		}
		logger := logs.Logger("it", map[string]cloudwatchlogs.AttrValue{"run": prefix})
		logger.Info(ctx, "logger hello "+marker, "password", "hunter2-"+prefix)
		logs.Send(ctx, cloudwatchlogs.LogInput{Message: "send hello " + marker, Severity: cloudwatchlogs.SeverityWarn,
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7"})
		if err := logs.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if st := logs.Stats(); st.Sent != 2 || st.Failed != 0 {
			t.Fatalf("stats %+v", st)
		}
		rec := must(q.WaitForLog(ctx, "send hello "+marker, cloudwatchlogs.WaitForLogOptions{Timeout: 20 * time.Second}))
		if rec.Severity != cloudwatchlogs.SeverityWarn || rec.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || rec.Resource["service.name"] != "it-go" ||
			rec.Attributes[cloudwatchlogs.AttrLogStream] != "logclient" {
			t.Fatalf("record %+v", rec)
		}
		lrec := must(q.WaitForLog(ctx, "logger hello "+marker, cloudwatchlogs.WaitForLogOptions{Timeout: 20 * time.Second}))
		if s, _ := json.Marshal(lrec); strings.Contains(string(s), "hunter2-"+prefix) {
			t.Fatal("password not redacted")
		}
		// exporter directly
		exp := must(lc.Exporter(cloudwatchlogs.ExporterOptions{Stream: "exporter"}))
		er := must(exp.Export(ctx, []cloudwatchlogs.LogRecord{{Timestamp: time.Now(), Severity: cloudwatchlogs.SeverityError, Body: "exported " + marker}}, cloudwatchlogs.ExportOptions{Gzip: true}))
		if er.Accepted != 1 || er.Rejected != 0 {
			t.Fatalf("export %+v", er)
		}
		// search: desc (default) newest first, asc oldest first, cursors
		desc := must(q.Search(ctx, marker, cloudwatchlogs.SearchOptions{Limit: 3}))
		if len(desc.Records) != 3 || desc.NextCursor == "" || desc.Records[0].Timestamp.Before(desc.Records[2].Timestamp) {
			t.Fatalf("desc %+v", desc)
		}
		all := must(cloudwatchlogs.SearchAll(ctx, q, marker, cloudwatchlogs.SearchOptions{Limit: 4}, 0))
		asc := must(cloudwatchlogs.SearchAll(ctx, q, marker, cloudwatchlogs.SearchOptions{Limit: 4, Order: cloudwatchlogs.OrderAsc}, 0))
		if len(all) != len(asc) || len(all) < 8 || asc[0].Timestamp.After(asc[len(asc)-1].Timestamp) {
			t.Fatalf("search all desc=%d asc=%d", len(all), len(asc))
		}
		streams := must(q.ListStreams(ctx))
		if !slices.Contains(streams, "logclient") || !slices.Contains(streams, "exporter") || !slices.Contains(streams, "s1") {
			t.Fatalf("streams %v", streams)
		}
		_, err := q.ListLabels(ctx)
		wantCode(t, err, cloudwatchlogs.ErrorCodeUnsupported)
	})

	t.Run("live tail", func(t *testing.T) {
		// Moto resolves every logGroupIdentifier to the most recently created
		// group and replays existing events once, so create the group last and
		// put the events before tailing.
		tail := prefix + "/tail"
		must(c.EnsureLogGroup(ctx, tail, cloudwatchlogs.EnsureLogGroupOptions{}))
		must(c.PutLogEvents(ctx, tail, "t1", []cloudwatchlogs.LogEvent{{Message: "tail one " + marker}, {Message: "tail two " + marker}}, cloudwatchlogs.PutLogEventsOptions{}))
		var mu sync.Mutex
		var got []cloudwatchlogs.LiveTailEvent
		var updates []cloudwatchlogs.LiveTailUpdate
		s := must(c.StartLiveTail(ctx, cloudwatchlogs.LiveTailOptions{Groups: []string{tail}, Pattern: "tail", OnUpdate: func(u cloudwatchlogs.LiveTailUpdate) {
			mu.Lock()
			updates = append(updates, u)
			mu.Unlock()
		}}, func(e cloudwatchlogs.LiveTailEvent) {
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		}))
		waitEvents(t, &mu, func() int { return len(got) }, 2, s.Done())
		_ = s.Close()
		mu.Lock()
		if len(got) < 2 || !strings.Contains(got[0].Message, "tail one") || got[0].LogStreamName != "t1" || s.SessionID() == "" || len(updates) == 0 {
			t.Fatalf("live tail %+v", got)
		}
		mu.Unlock()
		if err := s.Err(); err != nil {
			t.Fatal(err)
		}
		// Tail (LogQueryClient) on the same group
		tq := must(c.LogQueryClient(tail))
		var recs []cloudwatchlogs.LogRecord
		h := must(tq.Tail(ctx, "tail two", func(r cloudwatchlogs.LogRecord) {
			mu.Lock()
			recs = append(recs, r)
			mu.Unlock()
		}, cloudwatchlogs.TailOptions{}))
		waitEvents(t, &mu, func() int { return len(recs) }, 1, h.(*cloudwatchlogs.LiveTailSession).Done())
		h.Stop()
		mu.Lock()
		defer mu.Unlock()
		if len(recs) != 1 || !strings.Contains(recs[0].Body, "tail two") {
			t.Fatalf("tail %+v", recs)
		}
	})

	t.Run("permission denied with the read-only user", func(t *testing.T) {
		ak, sk := os.Getenv("GROW_TEST_READONLY_ACCESS_KEY_ID"), os.Getenv("GROW_TEST_READONLY_SECRET_ACCESS_KEY")
		if ak == "" {
			t.Skip("no read-only user in /bootstrap/env")
		}
		ro, err := cloudwatchlogs.NewClient(cloudwatchlogs.Config{
			CommonConfig: cloudwatchlogs.CommonConfig{URL: c.Config().URL}, Region: c.Region(),
			AccessKeyID: ak, SecretAccessKey: cloudwatchlogs.NewSecret(sk),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ro.Close() }()
		if _, err := ro.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		_, err = ro.EnsureLogGroup(ctx, prefix+"/ro", cloudwatchlogs.EnsureLogGroupOptions{})
		wantCode(t, err, cloudwatchlogs.ErrorCodePermissionDenied)
		if !errors.Is(err, cloudwatchlogs.ErrPermissionDenied) {
			t.Fatal("errors.Is")
		}
		_, err = ro.PutLogEvents(ctx, group, "s1", []cloudwatchlogs.LogEvent{{Message: "x"}}, cloudwatchlogs.PutLogEventsOptions{})
		wantCode(t, err, cloudwatchlogs.ErrorCodePermissionDenied)
	})

	t.Run("close", func(t *testing.T) {
		cc := must(cloudwatchlogs.NewClientFromEnv())
		if err := cc.Close(); err != nil || cc.Close() != nil {
			t.Fatal("close must be idempotent")
		}
		_, err := cc.Ping(ctx)
		wantCode(t, err, cloudwatchlogs.ErrorCodeCancelled)
	})
}

// waitEvents waits until count() >= n, the session ended or 15 s passed.
func waitEvents(t *testing.T, mu *sync.Mutex, count func() int, n int, done <-chan struct{}) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		got := count()
		mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-done:
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			got = count()
			mu.Unlock()
			if got < n {
				t.Fatalf("session ended with %d of %d events", got, n)
			}
			return
		case <-deadline:
			t.Fatalf("timed out with %d of %d events", got, n)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
