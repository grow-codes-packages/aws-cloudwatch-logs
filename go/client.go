package cloudwatchlogs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// defaultHealthTimeout bounds Health when RequestTimeout is longer.
const defaultHealthTimeout = 5 * time.Second

// Client is the Amazon CloudWatch Logs client, built on the AWS SDK for Go v2.
// It is safe for concurrent use. Besides the service functions it implements
// LogQueryClient (Search, Tail, ListStreams, ListLabels, WaitForLog) and
// creates LogClients with NewLogClient.
type Client struct {
	cfg       Config
	api       API
	sts       stsAPI
	region    string
	transport *http.Transport // nil when the caller supplied the SDK client
	secrets   []string

	closed    atomic.Bool
	closeOnce sync.Once

	mu         sync.Mutex
	logClients []*BatchingLogClient
	sessions   map[*LiveTailSession]struct{}
	accountID  string
}

// NewClient validates cfg, builds the AWS SDK client (or uses cfg.SDKClient)
// and returns a client. A region that neither cfg nor the AWS chain provides
// is a CONFIG error.
func NewClient(cfg Config) (*Client, error) {
	return newClient(context.Background(), cfg)
}

// NewClientFromEnv builds a client from the environment (see ConfigFromEnv).
func NewClientFromEnv() (*Client, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return NewClient(cfg)
}

func newClient(ctx context.Context, cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.CommonConfig = cfg.withDefaults(DefaultURL)
	if cfg.LogStream == "" {
		cfg.LogStream = defaultStreamName()
	}
	parts, err := buildSDK(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c := &Client{
		cfg:       cfg,
		api:       parts.api,
		sts:       parts.sts,
		region:    parts.region,
		accountID: cfg.AccountID,
		sessions:  map[*LiveTailSession]struct{}{},
	}
	if cfg.SDKClient == nil {
		c.transport = parts.transport
	}
	for _, s := range []string{cfg.SecretAccessKey.Reveal(), cfg.SessionToken.Reveal(), cfg.ExternalID} {
		if s != "" {
			c.secrets = append(c.secrets, s)
		}
	}
	for k, v := range cfg.Headers {
		if IsSensitiveHeader(k) {
			c.secrets = append(c.secrets, v)
		}
	}
	return c, nil
}

// defaultStreamName returns "<hostname>-<pid>-<random6>".
func defaultStreamName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "host"
	}
	host = strings.NewReplacer(":", "-", "*", "-").Replace(host)
	var b [3]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

// Config returns the effective configuration (defaults applied, LogStream set).
func (c *Client) Config() Config { return c.cfg }

// SDK returns the underlying AWS SDK client for operations this package does
// not wrap.
func (c *Client) SDK() API { return c.api }

// Region returns the resolved AWS region.
func (c *Client) Region() string { return c.region }

// begin checks the client is open and applies TotalTimeout to ctx.
func (c *Client) begin(ctx context.Context, op string) (context.Context, context.CancelFunc, error) {
	if c.closed.Load() {
		return nil, nil, &Error{Code: ErrorCodeCancelled, Message: op + ": client is closed"}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, contextErr(op, err)
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.TotalTimeout)
	return cctx, cancel, nil
}

// Health checks that CloudWatch Logs answers with a signed DescribeLogGroups
// (limit 1, no retries, short timeout). AWS has no unauthenticated health
// endpoint, so an authentication or permission error still means the service
// is up: Status "ok" with Details["auth"] = "failed". Throttling gives
// "degraded" (healthy), 5xx "unhealthy" and network errors "unreachable". Only
// CANCELLED and CONFIG are returned as errors.
func (c *Client) Health(ctx context.Context) (HealthStatus, error) {
	ctx, cancel, err := c.begin(ctx, "health")
	if err != nil {
		return HealthStatus{}, err
	}
	defer cancel()
	timeout := min(c.cfg.RequestTimeout, defaultHealthTimeout)
	hctx, hcancel := context.WithTimeout(ctx, timeout)
	defer hcancel()
	_, err = c.api.DescribeLogGroups(hctx, c.pingInput(), noRetry)
	if err == nil {
		return HealthStatus{Healthy: true, Status: "ok", Details: map[string]string{}}, nil
	}
	if ctx.Err() == context.Canceled {
		return HealthStatus{}, contextErr("health", ctx.Err())
	}
	var e *Error
	_ = errors.As(c.mapError("health", err), &e) // mapError always returns an *Error
	details := map[string]string{"code": string(e.Code), "error": e.Message}
	if e.HTTPStatus > 0 {
		details["httpStatus"] = fmt.Sprint(e.HTTPStatus)
	}
	switch e.Code {
	case ErrorCodeCancelled, ErrorCodeConfig:
		return HealthStatus{}, e
	case ErrorCodeAuthentication, ErrorCodePermissionDenied:
		details["auth"] = "failed"
		return HealthStatus{Healthy: true, Status: "ok", Details: details}, nil
	case ErrorCodeRateLimited:
		return HealthStatus{Healthy: true, Status: "degraded", Details: details}, nil
	case ErrorCodeUnavailable:
		if e.HTTPStatus >= 500 {
			return HealthStatus{Healthy: false, Status: "unhealthy", Details: details}, nil
		}
		return HealthStatus{Healthy: false, Status: "unreachable", Details: details}, nil
	case ErrorCodeTimeout:
		return HealthStatus{Healthy: false, Status: "unreachable", Details: details}, nil
	}
	if e.HTTPStatus >= 400 && e.HTTPStatus < 500 {
		return HealthStatus{Healthy: true, Status: "ok", Details: details}, nil
	}
	return HealthStatus{Healthy: false, Status: "unhealthy", Details: details}, nil
}

func (c *Client) pingInput() *cwl.DescribeLogGroupsInput {
	in := &cwl.DescribeLogGroupsInput{Limit: ptr(int32(1))}
	if c.cfg.LogGroup != "" {
		in.LogGroupNamePrefix = ptr(c.cfg.LogGroup)
	}
	return in
}

// Ping makes a signed DescribeLogGroups round trip (limit 1) and returns its
// latency. Bad credentials give AUTHENTICATION, a principal without
// logs:DescribeLogGroups PERMISSION_DENIED.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	ctx, cancel, err := c.begin(ctx, "ping")
	if err != nil {
		return 0, err
	}
	defer cancel()
	start := time.Now()
	if _, err := c.api.DescribeLogGroups(ctx, c.pingInput()); err != nil {
		return 0, c.mapError("ping", err)
	}
	return max(time.Since(start), time.Nanosecond), nil
}

// ServerVersion always returns UNSUPPORTED: Amazon CloudWatch Logs exposes no
// server version (the API version is the constant APIVersion).
func (c *Client) ServerVersion(context.Context) (string, error) {
	return "", UnsupportedError("serverVersion", "Amazon CloudWatch Logs does not expose a server version")
}

// Close flushes and closes every LogClient created by NewLogClient, stops the
// live-tail sessions and releases idle connections. It is idempotent and
// never closes a caller-supplied SDK client.
func (c *Client) Close() error {
	var firstErr error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		logs := c.logClients
		c.logClients = nil
		sessions := make([]*LiveTailSession, 0, len(c.sessions))
		for s := range c.sessions {
			sessions = append(sessions, s)
		}
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.TotalTimeout)
		defer cancel()
		for _, lc := range logs {
			if err := lc.Close(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		for _, s := range sessions {
			_ = s.Close()
		}
		c.closed.Store(true)
		if c.transport != nil {
			c.transport.CloseIdleConnections()
		}
	})
	return firstErr
}

// account returns the AWS account id (config, else STS GetCallerIdentity, cached).
func (c *Client) account(ctx context.Context) (string, error) {
	c.mu.Lock()
	id := c.accountID
	c.mu.Unlock()
	if id != "" {
		return id, nil
	}
	if c.sts == nil {
		return "", ConfigError("accountId is required to build log group ARNs (GROW_AWS_CLOUDWATCH_LOGS_ACCOUNT_ID)")
	}
	out, err := c.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", c.mapError("getCallerIdentity", err)
	}
	if out.Account == nil || *out.Account == "" {
		return "", &Error{Code: ErrorCodeUnavailable, Message: "getCallerIdentity: no account in the response"}
	}
	c.mu.Lock()
	c.accountID = *out.Account
	c.mu.Unlock()
	return *out.Account, nil
}

func ptr[T any](v T) *T { return &v }

// small32 converts a validated, small non-negative count (page sizes,
// limits, retention days) to int32, saturating at math.MaxInt32.
func small32(n int) int32 {
	return int32(min(max(n, 0), math.MaxInt32)) //nolint:gosec // clamped to the int32 range above
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}
