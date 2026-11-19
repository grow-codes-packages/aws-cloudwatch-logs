package cloudwatchlogs

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// eventStore is an in-memory FilterLogEvents backend (substring match on the
// quoted phrase, inclusive time bounds, pages of `limit`).
type eventStore struct {
	mu     sync.Mutex
	events []map[string]any
}

func (s *eventStore) add(ts int64, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, map[string]any{"eventId": fmt.Sprint(len(s.events) + 1), "timestamp": ts, "ingestionTime": ts, "message": msg, "logStreamName": "s1"})
}

func (s *eventStore) handler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		FilterPattern string `json:"filterPattern"`
		StartTime     *int64 `json:"startTime"`
		EndTime       *int64 `json:"endTime"`
		Limit         int    `json:"limit"`
		NextToken     string `json:"nextToken"`
	}
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &body)
	phrase := strings.Trim(body.FilterPattern, `"`)
	s.mu.Lock()
	var match []map[string]any
	for _, e := range s.events {
		ts := e["timestamp"].(int64)
		if (body.StartTime != nil && ts < *body.StartTime) || (body.EndTime != nil && ts > *body.EndTime) {
			continue
		}
		if !strings.Contains(e["message"].(string), phrase) {
			continue
		}
		match = append(match, e)
	}
	s.mu.Unlock()
	offset := 0
	_, _ = fmt.Sscan(body.NextToken, &offset)
	limit := body.Limit
	if limit == 0 {
		limit = 10000
	}
	end := min(offset+limit, len(match))
	out := map[string]any{"events": match[offset:end]}
	if end < len(match) {
		out["nextToken"] = fmt.Sprint(end)
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	_ = json.NewEncoder(w).Encode(out)
}

func TestExporterAndLogClient(t *testing.T) {
	m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.ok"))
	c := newMockClient(t, m, nil)
	lc, err := c.LogClient(LogClientConfig{LogClientOptions: LogClientOptions{Resource: map[string]AttrValue{"service.name": "api"}}})
	if err != nil {
		t.Fatal(err)
	}
	lc.Send(t.Context(), LogInput{Message: "hello export", Attributes: map[string]AttrValue{"password": "hunter2", "n": 1}})
	lc.Logger("test", nil).Error(t.Context(), "boom", "k", "v")
	if err := lc.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st := lc.Stats(); st.Sent != 2 || st.Failed != 0 {
		t.Fatalf("stats %+v", st)
	}
	body := m.callsOf("PutLogEvents")[0].Body
	evs := body["logEvents"].([]any)
	msg := evs[0].(map[string]any)["message"].(string)
	var rec LogRecord
	if err := json.Unmarshal([]byte(msg), &rec); err != nil || rec.Body != "hello export" || rec.Attributes["password"] != Redacted || rec.Resource["service.name"] != "api" {
		t.Fatalf("message %s (%v)", msg, err)
	}
	if body["logGroupName"] != "app/web" || body["logStreamName"] != "s1" {
		t.Fatalf("%v", body)
	}
	lc.Send(t.Context(), LogInput{Message: "flushed on close"})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if len(m.callsOf("PutLogEvents")) != 2 {
		t.Fatal("Close must flush log clients")
	}

	noGroup := newMockClient(t, m, func(cfg *Config) { cfg.LogGroup = "" })
	if _, err := noGroup.LogClient(LogClientConfig{}); CodeOf(err) != ErrorCodeConfig {
		t.Fatal(err)
	}
	if _, err := c.LogClient(LogClientConfig{}); CodeOf(err) != ErrorCodeCancelled {
		t.Fatal(err)
	}
	if _, err := newMockClient(t, m, nil).LogClient(LogClientConfig{LogClientOptions: LogClientOptions{Batch: BatchOptions{MaxQueueSize: -1}}}); CodeOf(err) != ErrorCodeConfig {
		t.Fatal(err)
	}
}

func TestExporterResults(t *testing.T) {
	now := time.Now()
	recs := []LogRecord{{Timestamp: now, Body: "a"}, {Body: "b"}, {Timestamp: now, Body: "c"}}
	t.Run("rejected", func(t *testing.T) {
		m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.rejected-too-old-too-new"))
		exp, _ := newMockClient(t, m, nil).Exporter(ExporterOptions{})
		res, err := exp.Export(t.Context(), recs, ExportOptions{Gzip: true})
		if err != nil || res.Accepted != 2 || res.Rejected != 1 || CodeOf(res.Error) != ErrorCodeValidation {
			t.Fatalf("%+v %v", res, err)
		}
		if exp.Shutdown(t.Context()) != nil {
			t.Fatal("shutdown")
		}
	})
	t.Run("failure", func(t *testing.T) {
		m := newAWSMock(t).on("PutLogEvents", awsError(400, "ServiceUnavailableException", "down"))
		exp, _ := newMockClient(t, m, nil).Exporter(ExporterOptions{})
		_, err := exp.Export(t.Context(), recs, ExportOptions{})
		wantCode(t, err, ErrorCodeUnavailable)
	})
	t.Run("partial failure", func(t *testing.T) {
		big := strings.Repeat("x", 200_000)
		many := make([]LogRecord, 8) // 8 × ~200 KB > 1 MiB → two PutLogEvents calls
		for i := range many {
			many[i] = LogRecord{Timestamp: now, Body: big}
		}
		m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.ok"), awsError(400, "ServiceUnavailableException", "down"))
		exp, _ := newMockClient(t, m, nil).Exporter(ExporterOptions{})
		res, err := exp.Export(t.Context(), many, ExportOptions{})
		if err != nil || res.Accepted == 0 || CodeOf(res.Error) != ErrorCodeUnavailable {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("oversize records", func(t *testing.T) {
		big := LogRecord{Timestamp: now, Body: strings.Repeat("ü\"", 300), Attributes: map[string]AttrValue{"k": "v"}}
		msg, ok := encodeRecord(big, 200, OversizeTruncate)
		var rec LogRecord
		if !ok || len(msg) > 200 || json.Unmarshal([]byte(msg), &rec) != nil || !strings.Contains(rec.Body, "truncated") {
			t.Fatalf("%d %q", len(msg), msg)
		}
		if _, ok := encodeRecord(big, 200, OversizeDrop); ok {
			t.Fatal("drop")
		}
		hugeAttrs := LogRecord{Timestamp: now, Body: "b", Attributes: map[string]AttrValue{"k": strings.Repeat("v", 500)}}
		if msg, ok := encodeRecord(hugeAttrs, 100, OversizeTruncate); !ok || len(msg) > 100 {
			t.Fatalf("attrs %d", len(msg))
		}
		m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.ok"))
		exp, _ := newMockClient(t, m, func(cfg *Config) { cfg.MaxEventBytes = 250; cfg.OversizePolicy = OversizeError }).Exporter(ExporterOptions{})
		res, err := exp.Export(t.Context(), []LogRecord{big, {Timestamp: now, Body: "ok"}}, ExportOptions{})
		if err != nil || res.Rejected != 1 || res.Accepted != 1 {
			t.Fatalf("%+v %v", res, err)
		}
	})
}

func TestSearch(t *testing.T) {
	base := time.Now().Add(-time.Hour).UnixMilli()
	store := &eventStore{}
	for i := range 25 {
		store.add(base+int64(i/2)*1000, fmt.Sprintf(`{"timestamp":"%s","severity":9,"severityText":"INFO","body":"needle %d","attributes":{"i":%d},"resource":{}}`,
			time.UnixMilli(base+int64(i/2)*1000).UTC().Format(time.RFC3339Nano), i, i))
	}
	store.add(base-40*24*3600*1000, "needle very old plain line") // outside the default window
	store.add(base+100_000, "haystack")
	newClient := func(t *testing.T) (*QueryClient, *awsMock) {
		m := newAWSMock(t).on("FilterLogEvents", store.handler).on("DescribeLogGroups", jsonReply(`{"logGroups":[]}`))
		return queryOf(t, newMockClient(t, m, nil)), m
	}
	t.Run("desc pages with cursor", func(t *testing.T) {
		c, _ := newClient(t)
		var bodies []string
		cursor := ""
		for range 10 {
			res, err := c.Search(t.Context(), "needle", SearchOptions{Limit: 4, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range res.Records {
				bodies = append(bodies, r.Body)
				if r.Attributes[AttrLogStream] != "s1" || r.Attributes[AttrEventID] == nil {
					t.Fatalf("attrs %v", r.Attributes)
				}
			}
			if res.NextCursor == "" {
				break
			}
			cursor = res.NextCursor
		}
		if len(bodies) != 25 || bodies[0] != "needle 24" || bodies[24] != "needle 0" {
			t.Fatalf("%d %v", len(bodies), bodies)
		}
		seen := map[string]bool{}
		for _, b := range bodies {
			if seen[b] {
				t.Fatalf("duplicate %s", b)
			}
			seen[b] = true
		}
	})
	t.Run("desc with explicit window and SearchAll", func(t *testing.T) {
		c, _ := newClient(t)
		recs, err := SearchAll(t.Context(), c, "needle", SearchOptions{Start: time.UnixMilli(base - 50*24*3600*1000), End: time.Now(), Limit: 7}, 0)
		if err != nil || len(recs) != 26 || recs[25].Body != "needle very old plain line" || recs[25].Severity != SeverityInfo {
			t.Fatalf("%d %v", len(recs), err)
		}
	})
	t.Run("desc uses group creation time", func(t *testing.T) {
		m := newAWSMock(t).on("FilterLogEvents", store.handler).
			on("DescribeLogGroups", jsonReply(fmt.Sprintf(`{"logGroups":[{"logGroupName":"app/web","creationTime":%d}]}`, base-2*3600*1000)))
		res, err := queryOf(t, newMockClient(t, m, nil)).Search(t.Context(), "needle", SearchOptions{Limit: 100})
		if err != nil || len(res.Records) != 25 || res.NextCursor != "" {
			t.Fatalf("%d %v", len(res.Records), err)
		}
		m2 := newAWSMock(t).on("DescribeLogGroups", awsError(400, "AccessDeniedException", "no"))
		_, err = queryOf(t, newMockClient(t, m2, nil)).Search(t.Context(), "needle", SearchOptions{})
		wantCode(t, err, ErrorCodePermissionDenied)
	})
	t.Run("asc pages", func(t *testing.T) {
		c, m := newClient(t)
		var all []LogRecord
		cursor := ""
		for {
			res, err := c.Search(t.Context(), "needle", SearchOptions{Order: OrderAsc, Limit: 10, Cursor: cursor, Start: time.UnixMilli(base)})
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, res.Records...)
			if res.NextCursor == "" {
				break
			}
			cursor = res.NextCursor
		}
		if len(all) != 25 || all[0].Body != "needle 0" {
			t.Fatalf("%d", len(all))
		}
		if m.callsOf("FilterLogEvents")[0].Body["filterPattern"] != `"needle"` {
			t.Fatal("pattern")
		}
	})
	t.Run("errors", func(t *testing.T) {
		c, _ := newClient(t)
		now := time.Now()
		_, err := c.Search(t.Context(), "x", SearchOptions{Start: now, End: now.Add(-time.Second)})
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.Search(t.Context(), "x", SearchOptions{Order: "sideways"})
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.Search(t.Context(), "x", SearchOptions{Cursor: "bogus"})
		wantCode(t, err, ErrorCodeValidation)
		noGroup := newMockClient(t, newAWSMock(t), func(cfg *Config) { cfg.LogGroup = "" })
		_, err = noGroup.LogQueryClient("")
		wantCode(t, err, ErrorCodeConfig)
		_, err = noGroup.LogQueryClient("bad name!")
		wantCode(t, err, ErrorCodeValidation)
		m := newAWSMock(t).on("FilterLogEvents", replay(t, "filterLogEvents.not-found")).on("DescribeLogGroups", jsonReply(`{"logGroups":[]}`))
		bad := queryOf(t, newMockClient(t, m, nil))
		_, err = bad.Search(t.Context(), "x", SearchOptions{Order: OrderAsc})
		wantCode(t, err, ErrorCodeNotFound)
		_, err = bad.Search(t.Context(), "x", SearchOptions{})
		wantCode(t, err, ErrorCodeNotFound)
		closedC := newMockClient(t, m, nil)
		closed := queryOf(t, closedC)
		_ = closedC.Close()
		_, err = closed.Search(t.Context(), "x", SearchOptions{Order: OrderAsc})
		wantCode(t, err, ErrorCodeCancelled)
		_, err = closed.Search(t.Context(), "x", SearchOptions{})
		wantCode(t, err, ErrorCodeCancelled)
	})
	t.Run("wait for log", func(t *testing.T) {
		c, _ := newClient(t)
		rec, err := c.WaitForLog(t.Context(), "needle 7", WaitForLogOptions{Timeout: 5 * time.Second})
		if err != nil || rec.Body != "needle 7" {
			t.Fatalf("%+v %v", rec, err)
		}
	})
}

func TestSearchHelpers(t *testing.T) {
	for q, want := range map[string]string{"": "", "hello world": `"hello world"`, `say "hi"`: `"say \"hi\""`, `{ $.a = 1 }`: `{ $.a = 1 }`, "?a ?b": "?a ?b", `"exact"`: `"exact"`, `-x`: `-x`, `[w1]`: `[w1]`} {
		if got := FilterPattern(q); got != want {
			t.Errorf("%q: %q", q, got)
		}
	}
	if compareEventIDs("9", "10") >= 0 || compareEventIDs("b", "a") <= 0 || compareEventIDs("ab", "b") >= 0 {
		t.Fatal("compareEventIDs")
	}
	if c, ok := decodeDescCursor(encodeDescCursor(descCursor{End: 5, Skip: 2})); !ok || c.End != 5 || c.Skip != 2 {
		t.Fatal("cursor round trip")
	}
	for _, bad := range []string{"d1.!!!", "x", "d1." + "e30", encodeDescCursor(descCursor{Skip: -1})} {
		if c, ok := decodeDescCursor(bad); ok && bad != "d1.e30" {
			t.Errorf("%q accepted: %+v", bad, c)
		}
	}
	ts := time.UnixMilli(1000).UTC()
	r := MessageToRecord("plain", ts, ts)
	if r.Body != "plain" || r.Severity != SeverityInfo || !r.Timestamp.Equal(ts) || r.Attributes == nil {
		t.Fatalf("%+v", r)
	}
	r = MessageToRecord(`{"level":"info","msg":"x"}`, ts, time.Time{})
	if r.Body != `{"level":"info","msg":"x"}` {
		t.Fatal("non-record JSON must stay the body")
	}
	r = MessageToRecord(`{"timestamp":"","body":"b","severity":17,"severityText":"ERROR"}`, ts, time.Time{})
	if r.Body != "b" || r.Severity != SeverityError || !r.Timestamp.Equal(ts) {
		t.Fatalf("%+v", r)
	}
	r = MessageToRecord(`{"timestamp":"not a time","body":"b"}`, ts, time.Time{})
	if !strings.Contains(r.Body, "not a time") {
		t.Fatal("bad record stays raw")
	}
}

func TestQueryClientMisc(t *testing.T) {
	m := newAWSMock(t).
		on("DescribeLogStreams", replay(t, "describeLogStreams.all")).
		on("StartLiveTail", replay(t, "startLiveTail.seed"))
	c := newMockClient(t, m, nil)
	q := queryOf(t, c)
	if q.Group() != "app/web" {
		t.Fatal(q.Group())
	}
	names, err := q.ListStreams(t.Context())
	if err != nil || len(names) == 0 {
		t.Fatalf("%v %v", names, err)
	}
	if _, err := q.ListLabels(t.Context()); CodeOf(err) != ErrorCodeUnsupported {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got []LogRecord
	h, err := q.Tail(t.Context(), "grow-seed-event", func(r LogRecord) { mu.Lock(); got = append(got, r); mu.Unlock() }, TailOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, h.(*LiveTailSession))
	h.Stop()
	mu.Lock()
	if len(got) != 3 || got[0].Attributes[AttrLogStream] != "seed-stream" {
		t.Fatalf("%+v", got)
	}
	mu.Unlock()
	if m.callsOf("StartLiveTail")[0].Body["logEventFilterPattern"] != `"grow-seed-event"` {
		t.Fatal("pattern")
	}
	if _, err := q.Tail(t.Context(), "x", nil, TailOptions{}); CodeOf(err) != ErrorCodeValidation {
		t.Fatal(err)
	}
	noGroup := newMockClient(t, m, func(cfg *Config) { cfg.LogGroup = "" })
	if _, err := noGroup.Exporter(ExporterOptions{Group: "g", Stream: "bad:stream"}); CodeOf(err) != ErrorCodeValidation {
		t.Fatal(err)
	}
	if _, err := noGroup.Exporter(ExporterOptions{}); CodeOf(err) != ErrorCodeConfig {
		t.Fatal(err)
	}
	m2 := newAWSMock(t).on("DescribeLogStreams", replay(t, "describeLogStreams.not-found"))
	if _, err := queryOf(t, newMockClient(t, m2, nil)).ListStreams(t.Context()); CodeOf(err) != ErrorCodeNotFound {
		t.Fatal(err)
	}
}

func queryOf(t *testing.T, c *Client) *QueryClient {
	t.Helper()
	q, err := c.LogQueryClient("")
	if err != nil {
		t.Fatal(err)
	}
	return q
}
