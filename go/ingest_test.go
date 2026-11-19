package cloudwatchlogs

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestPlanBatches(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	t.Run("sorted and stable", func(t *testing.T) {
		evs := []LogEvent{{Timestamp: now.Add(2 * time.Second), Message: "c"}, {Timestamp: now, Message: "a"}, {Timestamp: now, Message: "b"}, {Message: "zero"}}
		p, err := planBatches("op", evs, 100, OversizeTruncate, now.Add(time.Hour))
		if err != nil || len(p.batches) != 1 {
			t.Fatal(err)
		}
		var got []string
		for _, e := range p.batches[0] {
			got = append(got, *e.Message)
		}
		if strings.Join(got, ",") != "a,b,c,zero" || *p.batches[0][3].Timestamp != now.Add(time.Hour).UnixMilli() {
			t.Fatalf("order %v", got)
		}
	})
	t.Run("10000 events", func(t *testing.T) {
		evs := make([]LogEvent, MaxBatchEvents+1)
		for i := range evs {
			evs[i] = LogEvent{Timestamp: now, Message: "x"}
		}
		p, _ := planBatches("op", evs, 100, OversizeTruncate, now)
		if len(p.batches) != 2 || len(p.batches[0]) != MaxBatchEvents || len(p.batches[1]) != 1 {
			t.Fatalf("batches %d", len(p.batches))
		}
	})
	t.Run("1 MiB including overhead", func(t *testing.T) {
		msg := strings.Repeat("y", 262_144-EventOverheadBytes) // 4 events = exactly 1 MiB
		evs := []LogEvent{{now, msg}, {now, msg}, {now, msg}, {now, msg}, {now, "z"}}
		p, _ := planBatches("op", evs, DefaultMaxEventBytes, OversizeTruncate, now)
		if len(p.batches) != 2 || len(p.batches[0]) != 4 {
			t.Fatalf("batches %d/%d", len(p.batches), len(p.batches[0]))
		}
	})
	t.Run("24 h span", func(t *testing.T) {
		evs := []LogEvent{{now, "a"}, {now.Add(MaxBatchSpan), "b"}, {now.Add(MaxBatchSpan + time.Millisecond), "c"}}
		p, _ := planBatches("op", evs, 100, OversizeTruncate, now)
		if len(p.batches) != 2 || len(p.batches[0]) != 2 {
			t.Fatalf("span batches %d", len(p.batches))
		}
	})
	t.Run("oversize policies", func(t *testing.T) {
		big := strings.Repeat("é", 100) // 200 bytes
		p, err := planBatches("op", []LogEvent{{now, big}, {now, "ok"}}, 64, OversizeTruncate, now)
		if err != nil || p.truncated != 1 {
			t.Fatal(err)
		}
		m := *p.batches[0][0].Message
		if len(m) > 64 || !utf8.ValidString(m) || !strings.Contains(m, "…[truncated ") {
			t.Fatalf("truncated %q (%d)", m, len(m))
		}
		p, _ = planBatches("op", []LogEvent{{now, big}, {now, "ok"}}, 64, OversizeDrop, now)
		if p.dropped != 1 || len(p.batches[0]) != 1 {
			t.Fatal("drop")
		}
		_, err = planBatches("op", []LogEvent{{now, big}}, 64, OversizeError, now)
		wantCode(t, err, ErrorCodeValidation)
		p, _ = planBatches("op", nil, 64, OversizeError, now)
		if len(p.batches) != 0 {
			t.Fatal("empty")
		}
	})
}

func TestTruncateMessage(t *testing.T) {
	if TruncateMessage("short", 10) != "short" {
		t.Fatal("fits")
	}
	long := strings.Repeat("a", 1000)
	got := TruncateMessage(long, 100)
	if len(got) > 100 || !strings.HasSuffix(got, "…[truncated 929 bytes]") && !strings.Contains(got, "truncated") {
		t.Fatalf("%q", got)
	}
	if got := TruncateMessage(long, 5); got != "aaaaa" {
		t.Fatalf("tiny %q", got)
	}
	if got := TruncateMessage("ééé", 3); got != "é" {
		t.Fatalf("rune %q", got)
	}
}

func TestRejectedCount(t *testing.T) {
	cases := []struct {
		r    RejectedInfo
		n    int
		want int
	}{
		{RejectedInfo{TooOldLogEventEndIndex: -1, TooNewLogEventStartIndex: -1, ExpiredLogEventEndIndex: -1}, 5, 0},
		{RejectedInfo{TooOldLogEventEndIndex: 2, TooNewLogEventStartIndex: -1, ExpiredLogEventEndIndex: -1}, 5, 2},
		{RejectedInfo{TooOldLogEventEndIndex: -1, TooNewLogEventStartIndex: 3, ExpiredLogEventEndIndex: 1}, 5, 3},
		{RejectedInfo{TooOldLogEventEndIndex: 9, TooNewLogEventStartIndex: 0, ExpiredLogEventEndIndex: -1}, 5, 5},
	}
	for i, tc := range cases {
		if got := tc.r.count(tc.n); got != tc.want {
			t.Errorf("case %d: %d", i, got)
		}
	}
}

func TestPutLogEvents(t *testing.T) {
	now := time.Now()
	t.Run("ok", func(t *testing.T) {
		m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.ok"))
		c := newMockClient(t, m, nil)
		res, err := c.PutLogEvents(t.Context(), "app/web", "s1", []LogEvent{{now, "hello"}, {now.Add(-time.Second), "first"}}, PutLogEventsOptions{})
		if err != nil || res.Accepted != 2 || res.Batches != 1 || res.Rejected != 0 {
			t.Fatalf("%+v %v", res, err)
		}
		body := m.callsOf("PutLogEvents")[0].Body
		evs := body["logEvents"].([]any)
		if evs[0].(map[string]any)["message"] != "first" || body["sequenceToken"] != nil {
			t.Fatalf("body %v", body)
		}
	})
	t.Run("rejected", func(t *testing.T) {
		m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.rejected-too-old-too-new"))
		res, err := newMockClient(t, m, nil).PutLogEvents(t.Context(), "app/web", "s1", []LogEvent{{now, "a"}, {now, "b"}, {now, "c"}}, PutLogEventsOptions{})
		if err != nil || res.Rejected != 1 || res.Accepted != 2 || len(res.RejectedInfo) != 1 || res.RejectedInfo[0].TooNewLogEventStartIndex != 2 || res.RejectedInfo[0].Batch != 0 {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("auto create stream", func(t *testing.T) {
		m := newAWSMock(t).
			on("PutLogEvents", replay(t, "putLogEvents.stream-not-found"), replay(t, "putLogEvents.ok")).
			on("CreateLogStream", replay(t, "ensureLogStream.create"))
		res, err := newMockClient(t, m, nil).PutLogEvents(t.Context(), "app/web", "s1", []LogEvent{{now, "a"}}, PutLogEventsOptions{})
		if err != nil || res.Accepted != 1 || len(m.callsOf("CreateLogStream")) != 1 {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("auto create fails on missing group", func(t *testing.T) {
		m := newAWSMock(t).
			on("PutLogEvents", replay(t, "putLogEvents.group-not-found")).
			on("CreateLogStream", replay(t, "ensureLogStream.group-not-found"))
		_, err := newMockClient(t, m, nil).PutLogEvents(t.Context(), "app/web", "s1", []LogEvent{{now, "a"}}, PutLogEventsOptions{})
		wantCode(t, err, ErrorCodeNotFound)
	})
	t.Run("auto create off", func(t *testing.T) {
		m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.stream-not-found"))
		off := false
		_, err := newMockClient(t, m, nil).PutLogEvents(t.Context(), "app/web", "s1", []LogEvent{{now, "a"}}, PutLogEventsOptions{AutoCreate: &off})
		wantCode(t, err, ErrorCodeNotFound)
		c := newMockClient(t, m, func(cfg *Config) { cfg.DisableAutoCreate = true })
		_, err = c.PutLogEvents(t.Context(), "app/web", "s1", []LogEvent{{now, "a"}}, PutLogEventsOptions{})
		wantCode(t, err, ErrorCodeNotFound)
		if len(m.callsOf("CreateLogStream")) != 0 {
			t.Fatal("created")
		}
	})
	t.Run("errors", func(t *testing.T) {
		m := newAWSMock(t).on("PutLogEvents", replay(t, "putLogEvents.access-denied"))
		c := newMockClient(t, m, nil)
		_, err := c.PutLogEvents(t.Context(), "app/web", "s1", []LogEvent{{now, "a"}}, PutLogEventsOptions{})
		wantCode(t, err, ErrorCodePermissionDenied)
		_, err = c.PutLogEvents(t.Context(), "", "s1", nil, PutLogEventsOptions{})
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.PutLogEvents(t.Context(), "a", "", nil, PutLogEventsOptions{})
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.PutLogEvents(t.Context(), "a", "s", nil, PutLogEventsOptions{MaxEventBytes: -5})
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.PutLogEvents(t.Context(), "a", "s", []LogEvent{{now, "abcdef"}}, PutLogEventsOptions{MaxEventBytes: 2, OversizePolicy: OversizeError})
		wantCode(t, err, ErrorCodeValidation)
		res, err := c.PutLogEvents(t.Context(), "a", "s", nil, PutLogEventsOptions{})
		if err != nil || res.Batches != 0 {
			t.Fatal("empty input must not call the service")
		}
		closed := newMockClient(t, m, nil)
		_ = closed.Close()
		_, err = closed.PutLogEvents(t.Context(), "a", "s", []LogEvent{{now, "a"}}, PutLogEventsOptions{})
		wantCode(t, err, ErrorCodeCancelled)
	})
}

func TestEMF(t *testing.T) {
	ts := time.UnixMilli(1_795_000_000_000)
	doc, err := BuildEMF("latency", 42, map[string]string{"service": "api", "env": "dev"}, EMFOptions{
		Namespace: "MyApp", Unit: "Milliseconds", Timestamp: ts, Properties: map[string]any{"requestId": "r1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(doc), &got); err != nil {
		t.Fatal(err)
	}
	aws := got["_aws"].(map[string]any)
	cwm := aws["CloudWatchMetrics"].([]any)[0].(map[string]any)
	dims := cwm["Dimensions"].([]any)[0].([]any)
	if aws["Timestamp"] != float64(ts.UnixMilli()) || cwm["Namespace"] != "MyApp" || dims[0] != "env" || dims[1] != "service" ||
		got["latency"] != float64(42) || got["service"] != "api" || got["requestId"] != "r1" {
		t.Fatalf("%s", doc)
	}
	doc, _ = BuildEMF("n", 0, nil, EMFOptions{Values: []float64{1, 2}})
	if !strings.Contains(doc, `"n":[1,2]`) || !strings.Contains(doc, `"Namespace":"GrowSdk"`) || !strings.Contains(doc, `"Unit":"None"`) {
		t.Fatal(doc)
	}
	tooMany := map[string]string{}
	for i := range 31 {
		tooMany[strings.Repeat("d", i+1)] = "v"
	}
	bad := []struct {
		metric string
		value  float64
		dims   map[string]string
		opts   EMFOptions
	}{
		{"", 1, nil, EMFOptions{}},
		{"m", 1, nil, EMFOptions{Namespace: strings.Repeat("n", 1025)}},
		{"m", 1, nil, EMFOptions{Unit: "Parsecs"}},
		{"m", 1, tooMany, EMFOptions{}},
		{"m", 1, nil, EMFOptions{Values: make([]float64, 101)}},
		{"m", math.NaN(), nil, EMFOptions{}},
		{"m", 1, map[string]string{"m": "x"}, EMFOptions{}},
		{"m", 1, nil, EMFOptions{Properties: map[string]any{"_aws": 1}}},
		{"m", 1, nil, EMFOptions{Properties: map[string]any{"f": func() {}}}},
	}
	for i, b := range bad {
		if _, err := BuildEMF(b.metric, b.value, b.dims, b.opts); CodeOf(err) != ErrorCodeValidation {
			t.Errorf("case %d: %v", i, err)
		}
	}

	m := newAWSMock(t).on("PutLogEvents", replay(t, "emf.put"))
	c := newMockClient(t, m, nil)
	res, err := c.EMF(t.Context(), "latency", 42, map[string]string{"service": "api"}, EMFOptions{})
	if err != nil || res.Accepted != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	body := m.callsOf("PutLogEvents")[0].Body
	if body["logGroupName"] != "app/web" || body["logStreamName"] != "s1" {
		t.Fatalf("%v", body)
	}
	_, err = c.EMF(t.Context(), "", 1, nil, EMFOptions{Group: "x", Stream: "y"})
	wantCode(t, err, ErrorCodeValidation)
	noGroup := newMockClient(t, m, func(cfg *Config) { cfg.LogGroup = "" })
	_, err = noGroup.EMF(t.Context(), "m", 1, nil, EMFOptions{})
	wantCode(t, err, ErrorCodeConfig)
}
