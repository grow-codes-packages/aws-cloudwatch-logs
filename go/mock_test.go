package cloudwatchlogs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const (
	testAccessKey = "AKIATESTKEY1234"
	testSecretKey = "secret-SECRET-abcdef"
	testAccount   = "123456789012"
)

func init() {
	// StartLiveTail prepends "stream-" to the endpoint host; route it back to
	// the local mock server.
	dialHook = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, strings.TrimPrefix(strings.TrimPrefix(addr, "stream-"), "streaming-"))
	}
}

// fixture is a recorded request/response from testdata/fixtures.
type fixture struct {
	Function  string `json:"function"`
	Case      string `json:"case"`
	Operation string `json:"operation"`
	Request   struct {
		Body map[string]any `json:"body"`
	} `json:"request"`
	Response struct {
		Status      int               `json:"status"`
		Headers     map[string]string `json:"headers"`
		Body        json.RawMessage   `json:"body"`
		BodyBase64  string            `json:"bodyBase64"`
		EventStream []map[string]any  `json:"eventStream"`
	} `json:"response"`
}

func loadFixture(t *testing.T, name string) fixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "fixtures", name+".json"))
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return f
}

// mockCall is one request seen by the mock.
type mockCall struct {
	Op   string
	Body map[string]any
	Req  *http.Request
}

// awsMock is a local AWS JSON 1.1 endpoint; handlers are keyed by operation.
type awsMock struct {
	*httptest.Server
	t        *testing.T
	mu       sync.Mutex
	handlers map[string][]http.HandlerFunc // consumed in order; the last one repeats
	calls    []mockCall
}

func newAWSMock(t *testing.T) *awsMock {
	t.Helper()
	m := &awsMock{t: t, handlers: map[string][]http.HandlerFunc{}}
	m.Server = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.Close)
	return m
}

func (m *awsMock) serve(w http.ResponseWriter, r *http.Request) {
	op := operationOf(r.Header.Get("X-Amz-Target"))
	if op == "" {
		if err := r.ParseForm(); err == nil {
			op = r.PostForm.Get("Action") // STS query protocol
		}
	}
	var body map[string]any
	raw, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(raw))
	_ = json.Unmarshal(raw, &body)
	m.mu.Lock()
	m.calls = append(m.calls, mockCall{Op: op, Body: body, Req: r.Clone(context.Background())})
	hs := m.handlers[op]
	var h http.HandlerFunc
	if len(hs) > 0 {
		h = hs[0]
		if len(hs) > 1 {
			m.handlers[op] = hs[1:]
		}
	}
	m.mu.Unlock()
	if h == nil {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"InvalidOperationException","message":"no mock for ` + op + `"}`))
		return
	}
	h(w, r)
}

// on queues handlers for an operation (the last one keeps answering).
func (m *awsMock) on(op string, hs ...http.HandlerFunc) *awsMock {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[op] = append(m.handlers[op], hs...)
	return m
}

// callsOf returns the calls of one operation.
func (m *awsMock) callsOf(op string) []mockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []mockCall
	for _, c := range m.calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

// replay answers with a recorded fixture.
func replay(t *testing.T, name string) http.HandlerFunc {
	f := loadFixture(t, name)
	return func(w http.ResponseWriter, _ *http.Request) {
		for k, v := range f.Response.Headers {
			if strings.EqualFold(k, "content-length") || strings.EqualFold(k, "status") {
				continue
			}
			w.Header().Set(k, v)
		}
		if f.Response.BodyBase64 != "" {
			raw, _ := base64.StdEncoding.DecodeString(f.Response.BodyBase64)
			w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
			w.WriteHeader(f.Response.Status)
			_, _ = w.Write(raw)
			return
		}
		w.WriteHeader(f.Response.Status)
		switch {
		case len(f.Response.Body) == 0 || string(f.Response.Body) == "null":
		case f.Response.Body[0] == '"':
			var s string
			_ = json.Unmarshal(f.Response.Body, &s)
			_, _ = w.Write([]byte(s))
		default:
			_, _ = w.Write(f.Response.Body)
		}
	}
}

// jsonReply answers 200 with a JSON body.
func jsonReply(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.Header().Set("X-Amzn-Requestid", "req-123")
		_, _ = w.Write([]byte(body))
	}
}

// awsError answers with an AWS JSON error.
func awsError(status int, code, message string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.Header().Set("X-Amzn-Requestid", "req-err")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"__type":"` + code + `","message":"` + message + `"}`))
	}
}

// rawReply answers with a status and raw body.
func rawReply(status int, contentType, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// newMockClient builds a client against the mock with static credentials.
func newMockClient(t *testing.T, m *awsMock, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		CommonConfig:    CommonConfig{URL: m.URL, Retry: fastRetry(0)},
		Region:          "us-east-1",
		AccessKeyID:     testAccessKey,
		SecretAccessKey: NewSecret(testSecretKey),
		AccountID:       testAccount,
		LogGroup:        "app/web",
		LogStream:       "s1",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// collect drains an iterator.
func collect[T any](seq func(func(T, error) bool)) ([]T, error) {
	var out []T
	for v, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

func wantCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	_ = wantErr(t, err, code)
}

// wantErr is wantCode returning the *Error for further checks.
func wantErr(t *testing.T, err error, code ErrorCode) *Error {
	t.Helper()
	if CodeOf(err) != code {
		t.Fatalf("want %s, got %v", code, err)
	}
	e := asError(t, err)
	if strings.Contains(e.Error(), testSecretKey) {
		t.Fatalf("secret leaked: %v", e)
	}
	return e
}

// newTestClient (used by client_tls_test.go) returns a client for url with
// static test credentials and fast retries; mutate adjusts the config.
func newTestClient(t *testing.T, url string, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		CommonConfig:    CommonConfig{URL: url, Retry: fastRetry(3)},
		Region:          "us-east-1",
		AccessKeyID:     testAccessKey,
		SecretAccessKey: NewSecret(testSecretKey),
		AccountID:       testAccount,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
