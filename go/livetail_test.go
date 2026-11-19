package cloudwatchlogs

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
)

func esFrame(t *testing.T, headers map[string]string, payload any) []byte {
	t.Helper()
	var h eventstream.Headers
	for k, v := range headers {
		h.Set(k, eventstream.StringValue(v))
	}
	var body []byte
	if payload != nil {
		body, _ = json.Marshal(payload)
	}
	var buf bytes.Buffer
	if err := eventstream.NewEncoder().Encode(&buf, eventstream.Message{Headers: h, Payload: body}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func eventFrame(t *testing.T, eventType string, payload any) []byte {
	return esFrame(t, map[string]string{":message-type": "event", ":event-type": eventType, ":content-type": "application/json"}, payload)
}

func exceptionFrame(t *testing.T, code string) []byte {
	return esFrame(t, map[string]string{":message-type": "exception", ":exception-type": code, ":content-type": "application/json"},
		map[string]string{"message": "stream ended"})
}

// streamReply writes event-stream frames; when hold is true it keeps the
// connection open until the client goes away.
func streamReply(frames [][]byte, hold bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		w.WriteHeader(200)
		for _, f := range frames {
			_, _ = w.Write(f)
			w.(http.Flusher).Flush()
		}
		if hold {
			<-r.Context().Done()
		}
	}
}

var sessionStart = map[string]any{"sessionId": "sess-1", "requestId": "r", "logGroupIdentifiers": []string{"arn"}}

func update(msgs ...string) map[string]any {
	results := make([]map[string]any, len(msgs))
	for i, m := range msgs {
		results[i] = map[string]any{"logStreamName": "s1", "logGroupIdentifier": "arn", "message": m, "timestamp": 1795034969000, "ingestionTime": 1795034972331}
	}
	return map[string]any{"sessionMetadata": map[string]any{"sampled": true}, "sessionResults": results}
}

type eventSink struct {
	mu  sync.Mutex
	evs []LiveTailEvent
}

func (s *eventSink) add(e LiveTailEvent) { s.mu.Lock(); s.evs = append(s.evs, e); s.mu.Unlock() }
func (s *eventSink) all() []LiveTailEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]LiveTailEvent(nil), s.evs...)
}

func waitDone(t *testing.T, s *LiveTailSession) {
	t.Helper()
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end")
	}
}

func TestStartLiveTail(t *testing.T) {
	t.Run("seed fixture", func(t *testing.T) {
		m := newAWSMock(t).on("StartLiveTail", replay(t, "startLiveTail.seed"))
		c := newMockClient(t, m, nil)
		var sink eventSink
		s, err := c.StartLiveTail(t.Context(), LiveTailOptions{Groups: []string{"grow-bootstrap/seed"}}, sink.add)
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, s)
		evs := sink.all()
		if s.SessionID() == "" || s.Err() != nil || len(evs) != 3 || !strings.Contains(evs[0].Message, "grow-seed-event 1") ||
			evs[0].LogStreamName != "seed-stream" || evs[0].Timestamp.IsZero() {
			t.Fatalf("session %q err %v events %+v", s.SessionID(), s.Err(), evs)
		}
		call := m.callsOf("StartLiveTail")[0]
		if !strings.HasPrefix(call.Req.Host, "stream-") {
			t.Fatalf("host prefix missing: %s", call.Req.Host)
		}
		ids := call.Body["logGroupIdentifiers"].([]any)
		if ids[0] != "arn:aws:logs:us-east-1:123456789012:log-group:grow-bootstrap/seed" {
			t.Fatalf("arn %v", ids)
		}
		if first, second := s.Close(), s.Close(); first != nil || second != nil {
			t.Fatal("close")
		}
	})
	t.Run("options body and stream exception", func(t *testing.T) {
		m := newAWSMock(t).on("StartLiveTail", streamReply([][]byte{
			eventFrame(t, "initial-response", nil),
			eventFrame(t, "sessionStart", sessionStart),
			eventFrame(t, "sessionUpdate", update("a", "b")),
			exceptionFrame(t, "SessionTimeoutException"),
		}, false))
		c := newMockClient(t, m, nil)
		var sink eventSink
		s, err := c.StartLiveTail(t.Context(), LiveTailOptions{Groups: []string{"arn:aws:logs:r:1:log-group:g:*"}, StreamPrefixes: []string{"s"}, Pattern: "ERROR"}, sink.add)
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, s)
		if len(sink.all()) != 2 || !sink.all()[0].Sampled {
			t.Fatalf("events %+v", sink.all())
		}
		e := wantErr(t, s.Err(), ErrorCodeTimeout)
		if !e.Retryable {
			t.Fatal("retryable")
		}
		body := m.callsOf("StartLiveTail")[0].Body
		if body["logEventFilterPattern"] != "ERROR" || body["logStreamNamePrefixes"] == nil || body["logGroupIdentifiers"].([]any)[0] != "arn:aws:logs:r:1:log-group:g" {
			t.Fatalf("body %v", body)
		}
	})
	t.Run("update before start and streams", func(t *testing.T) {
		m := newAWSMock(t).on("StartLiveTail", streamReply([][]byte{eventFrame(t, "initial-response", nil), eventFrame(t, "sessionUpdate", update("early"))}, false))
		var sink eventSink
		s, err := newMockClient(t, m, nil).StartLiveTail(t.Context(), LiveTailOptions{Streams: []string{"s1"}}, sink.add)
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, s)
		if len(sink.all()) != 1 || sink.all()[0].Message != "early" {
			t.Fatalf("%+v", sink.all())
		}
		if m.callsOf("StartLiveTail")[0].Body["logStreamNames"] == nil {
			t.Fatal("streams")
		}
	})
	t.Run("close a running session", func(t *testing.T) {
		m := newAWSMock(t).on("StartLiveTail", streamReply([][]byte{eventFrame(t, "initial-response", nil), eventFrame(t, "sessionStart", sessionStart)}, true))
		c := newMockClient(t, m, nil)
		var once sync.Once
		s, err := c.StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) { once.Do(func() {}) })
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-s.Done():
			t.Fatalf("ended early: %v", s.Err())
		case <-time.After(50 * time.Millisecond):
		}
		s.Stop()
		waitDone(t, s)
		if s.Err() != nil {
			t.Fatalf("err after close: %v", s.Err())
		}
		// Client.Close stops open sessions
		s2, err := c.StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
		waitDone(t, s2)
		_, err = c.StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		wantCode(t, err, ErrorCodeCancelled)
	})
	t.Run("stream ends before start", func(t *testing.T) {
		m := newAWSMock(t).on("StartLiveTail", streamReply([][]byte{eventFrame(t, "initial-response", nil)}, false))
		_, err := newMockClient(t, m, nil).StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		wantCode(t, err, ErrorCodeUnavailable)
		m2 := newAWSMock(t).on("StartLiveTail", streamReply([][]byte{eventFrame(t, "initial-response", nil), exceptionFrame(t, "SessionStreamingException")}, false))
		_, err = newMockClient(t, m2, nil).StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		wantCode(t, err, ErrorCodeUnavailable)
	})
	t.Run("no first frame in time", func(t *testing.T) {
		m := newAWSMock(t).on("StartLiveTail", streamReply(nil, true))
		c := newMockClient(t, m, func(cfg *Config) { cfg.TotalTimeout = 100 * time.Millisecond })
		_, err := c.StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		wantCode(t, err, ErrorCodeTimeout)
	})
	t.Run("errors", func(t *testing.T) {
		m := newAWSMock(t).on("StartLiveTail", awsError(400, "AccessDeniedException", "no"))
		c := newMockClient(t, m, nil)
		_, err := c.StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		wantCode(t, err, ErrorCodePermissionDenied)
		eleven := make([]string, 11)
		for i := range eleven {
			eleven[i] = "g"
		}
		bad := []LiveTailOptions{
			{Groups: eleven},
			{Streams: []string{"a"}, StreamPrefixes: []string{"b"}},
			{Groups: []string{"a", "b"}, Streams: []string{"a"}},
			{Groups: []string{"bad name!"}},
		}
		for i, o := range bad {
			if _, err := c.StartLiveTail(t.Context(), o, func(LiveTailEvent) {}); CodeOf(err) != ErrorCodeValidation {
				t.Errorf("case %d: %v", i, err)
			}
		}
		_, err = c.StartLiveTail(t.Context(), LiveTailOptions{}, nil)
		wantCode(t, err, ErrorCodeValidation)
		noGroup := newMockClient(t, m, func(cfg *Config) { cfg.LogGroup = "" })
		_, err = noGroup.StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		wantCode(t, err, ErrorCodeConfig)
		noRegion := &Client{cfg: c.cfg}
		if _, err := noRegion.groupARN(t.Context(), "op", "g"); !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
		// without an account id the ARN is looked up with DescribeLogGroups (no STS call)
		lookup := newAWSMock(t).on("DescribeLogGroups", replay(t, "describeLogGroups.prefix"), replay(t, "describeLogGroups.prefix"), replay(t, "describeLogGroups.empty"))
		noAccount := newMockClient(t, lookup, func(cfg *Config) { cfg.AccountID = "" })
		arn, err := noAccount.LogGroupARN(t.Context(), "cwlv-0badc0de/app")
		if err != nil || arn != "arn:aws:logs:us-east-1:123456789012:log-group:cwlv-0badc0de/app" {
			t.Fatalf("%s %v", arn, err)
		}
		if arn, err = noAccount.LogGroupARN(t.Context(), "cwlv-0badc0de/other"); err != nil || !strings.HasSuffix(arn, ":log-group:cwlv-0badc0de/other") {
			t.Fatalf("%s %v", arn, err)
		}
		_, err = noAccount.LogGroupARN(t.Context(), "cwlv-0badc0de/missing")
		wantCode(t, err, ErrorCodeNotFound)
		if len(lookup.callsOf("GetCallerIdentity")) != 0 || len(lookup.callsOf("DescribeLogGroups")) != 3 {
			t.Fatal("unexpected calls")
		}
		down := newMockClient(t, newAWSMock(t).on("DescribeLogGroups", awsError(400, "ServiceUnavailableException", "down")), func(cfg *Config) { cfg.AccountID = "" })
		_, err = down.StartLiveTail(t.Context(), LiveTailOptions{}, func(LiveTailEvent) {})
		wantCode(t, err, ErrorCodeUnavailable)
	})
	for region, want := range map[string]string{"cn-north-1": "aws-cn", "us-gov-west-1": "aws-us-gov", "us-iso-east-1": "aws-iso",
		"us-isob-east-1": "aws-iso-b", "eusc-de-east-1": "aws-eusc", "eu-west-1": "aws"} {
		if partitionOf(region) != want {
			t.Errorf("%s", region)
		}
	}
}

func TestDetachBody(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "http://x", strings.NewReader(`{"a":1}`))
	req.ContentLength = -1
	detachBody(req)
	if req.ContentLength != 7 {
		t.Fatalf("length %d", req.ContentLength)
	}
	if b, _ := io.ReadAll(req.Body); string(b) != `{"a":1}` || req.Body.Close() != nil {
		t.Fatalf("body %q", b)
	}
	if rc, err := req.GetBody(); err != nil || rc == nil {
		t.Fatal(err)
	}
	failing, _ := http.NewRequest(http.MethodPost, "http://x", io.NopCloser(&errReader{err: errors.New("boom")}))
	detachBody(failing)
	if _, err := failing.Body.Read(make([]byte, 1)); err == nil || err.Error() != "boom" {
		t.Fatal(err)
	}
	empty, _ := http.NewRequest(http.MethodGet, "http://x", nil)
	detachBody(empty)
	if empty.Body != nil {
		t.Fatal("nil body changed")
	}
}
