package cloudwatchlogs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// API is the subset of the AWS SDK for Go v2 CloudWatch Logs client this
// package uses. *cloudwatchlogs.Client of the AWS SDK implements it; pass your
// own client (or a test double) in Config.SDKClient.
type API interface {
	CreateLogGroup(context.Context, *cwl.CreateLogGroupInput, ...func(*cwl.Options)) (*cwl.CreateLogGroupOutput, error)
	CreateLogStream(context.Context, *cwl.CreateLogStreamInput, ...func(*cwl.Options)) (*cwl.CreateLogStreamOutput, error)
	DeleteLogGroup(context.Context, *cwl.DeleteLogGroupInput, ...func(*cwl.Options)) (*cwl.DeleteLogGroupOutput, error)
	DeleteLogStream(context.Context, *cwl.DeleteLogStreamInput, ...func(*cwl.Options)) (*cwl.DeleteLogStreamOutput, error)
	PutRetentionPolicy(context.Context, *cwl.PutRetentionPolicyInput, ...func(*cwl.Options)) (*cwl.PutRetentionPolicyOutput, error)
	DeleteRetentionPolicy(context.Context, *cwl.DeleteRetentionPolicyInput, ...func(*cwl.Options)) (*cwl.DeleteRetentionPolicyOutput, error)
	DescribeLogGroups(context.Context, *cwl.DescribeLogGroupsInput, ...func(*cwl.Options)) (*cwl.DescribeLogGroupsOutput, error)
	DescribeLogStreams(context.Context, *cwl.DescribeLogStreamsInput, ...func(*cwl.Options)) (*cwl.DescribeLogStreamsOutput, error)
	TagResource(context.Context, *cwl.TagResourceInput, ...func(*cwl.Options)) (*cwl.TagResourceOutput, error)
	UntagResource(context.Context, *cwl.UntagResourceInput, ...func(*cwl.Options)) (*cwl.UntagResourceOutput, error)
	ListTagsForResource(context.Context, *cwl.ListTagsForResourceInput, ...func(*cwl.Options)) (*cwl.ListTagsForResourceOutput, error)
	PutLogEvents(context.Context, *cwl.PutLogEventsInput, ...func(*cwl.Options)) (*cwl.PutLogEventsOutput, error)
	FilterLogEvents(context.Context, *cwl.FilterLogEventsInput, ...func(*cwl.Options)) (*cwl.FilterLogEventsOutput, error)
	GetLogEvents(context.Context, *cwl.GetLogEventsInput, ...func(*cwl.Options)) (*cwl.GetLogEventsOutput, error)
	StartQuery(context.Context, *cwl.StartQueryInput, ...func(*cwl.Options)) (*cwl.StartQueryOutput, error)
	GetQueryResults(context.Context, *cwl.GetQueryResultsInput, ...func(*cwl.Options)) (*cwl.GetQueryResultsOutput, error)
	StopQuery(context.Context, *cwl.StopQueryInput, ...func(*cwl.Options)) (*cwl.StopQueryOutput, error)
	DescribeQueries(context.Context, *cwl.DescribeQueriesInput, ...func(*cwl.Options)) (*cwl.DescribeQueriesOutput, error)
	StartLiveTail(context.Context, *cwl.StartLiveTailInput, ...func(*cwl.Options)) (*cwl.StartLiveTailOutput, error)
}

var _ API = (*cwl.Client)(nil)

// stsAPI is the STS call used to find the account id for live-tail ARNs.
type stsAPI interface {
	GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// attemptKey carries the per-operation attempt counter through the SDK stack.
type attemptKey struct{}

// attemptCounter puts a fresh attempt counter into every operation's context;
// the HTTP client increments it once per attempt.
var attemptCounter = middleware.InitializeMiddlewareFunc("GrowAttemptCounter",
	func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
		middleware.InitializeOutput, middleware.Metadata, error,
	) {
		return next.HandleInitialize(context.WithValue(ctx, attemptKey{}, new(atomic.Int32)), in)
	})

func addAttemptCounter(stack *middleware.Stack) error {
	return stack.Initialize.Add(attemptCounter, middleware.Before)
}

// hookedHTTPClient is the SDK's HTTP client: it applies the per-attempt request
// timeout, adds the Grow User-Agent and the extra headers (after signing; none
// of them is signed), fires the hooks and logs each attempt at debug level.
type hookedHTTPClient struct {
	client         *http.Client
	userAgent      string
	headers        map[string]string
	hooks          *Hooks
	logger         *slog.Logger
	maxAttempts    int
	requestTimeout time.Duration
	now            func() time.Time
}

// Do implements the SDK HTTPClient interface.
func (h *hookedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	attempt := 1
	if c, ok := req.Context().Value(attemptKey{}).(*atomic.Int32); ok {
		attempt = int(c.Add(1))
	}
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}
	if ua := req.Header.Get("User-Agent"); ua != "" {
		req.Header.Set("User-Agent", ua+" "+h.userAgent)
	} else {
		req.Header.Set("User-Agent", h.userAgent)
	}
	op := operationOf(req.Header.Get("X-Amz-Target"))
	cancel := context.CancelFunc(func() {})
	if h.requestTimeout > 0 && op != "StartLiveTail" { // live tail sessions last up to 3 h
		var ctx context.Context
		ctx, cancel = context.WithTimeout(req.Context(), h.requestTimeout)
		req = req.WithContext(ctx)
	}
	safeURL := RedactURL(req.URL.String())
	h.hooks.request(RequestInfo{Method: req.Method, URL: safeURL, Headers: RedactHeaders(req.Header), Attempt: attempt})
	start := h.now()
	res, err := h.client.Do(req) //nolint:gosec // G704: the URL is the configured AWS endpoint, built by the SDK
	if err != nil {
		cancel()
		willRetry := attempt < h.maxAttempts && req.Context().Err() == nil
		h.hooks.error(ErrorInfo{Method: req.Method, URL: safeURL, Err: errors.New("request failed: " + innerMessage(err)), Attempt: attempt, WillRetry: willRetry})
		h.log(slog.LevelDebug, "request failed", slog.String("operation", op), slog.Int("attempt", attempt))
		return nil, err
	}
	res.Body = &cancelOnClose{ReadCloser: res.Body, cancel: cancel}
	rid := requestID(res.Header)
	h.hooks.response(ResponseInfo{Method: req.Method, URL: safeURL, Status: res.StatusCode, Duration: h.now().Sub(start), Attempt: attempt, RequestID: rid})
	h.log(slog.LevelDebug, "response", slog.String("operation", op), slog.Int("status", res.StatusCode),
		slog.Int("attempt", attempt), slog.String("requestId", rid))
	return res, nil
}

func (h *hookedHTTPClient) log(level slog.Level, msg string, attrs ...slog.Attr) {
	safeCall(func() { h.logger.LogAttrs(context.Background(), level, msg, attrs...) })
}

// cancelOnClose releases the per-attempt timeout when the body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// operationOf returns "PutLogEvents" for "Logs_20140328.PutLogEvents".
func operationOf(target string) string {
	for i := len(target) - 1; i >= 0; i-- {
		if target[i] == '.' {
			return target[i+1:]
		}
	}
	return target
}

// growBackoff applies the SPEC full-jitter backoff to the SDK standard retryer
// and honours Retry-After up to MaxRetryAfter.
type growBackoff struct {
	policy RetryPolicy
	now    func() time.Time
}

// BackoffDelay implements retry.BackoffDelayer. attempt is 1-based.
func (b growBackoff) BackoffDelay(attempt int, err error) (time.Duration, error) {
	d := b.policy.Backoff(attempt - 1)
	if ra, ok := retryAfterOf(err, b.now()); ok {
		if ra > b.policy.MaxRetryAfter {
			return 0, err // stop retrying; the mapped error carries RetryAfter
		}
		if ra > d {
			d = ra
		}
	}
	return d, nil
}

// retryAfterOf returns the Retry-After of the HTTP response behind err.
func retryAfterOf(err error, now time.Time) (time.Duration, bool) {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.Response != nil {
		return ParseRetryAfter(re.Response.Header.Get("Retry-After"), now)
	}
	return 0, false
}

// newRetryer maps the Grow retry policy onto the SDK standard retry mode
// (maxAttempts = maxRetries + 1, no client-side retry quota).
func newRetryer(p RetryPolicy, now func() time.Time) aws.Retryer {
	p = p.withDefaults()
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = p.MaxRetries + 1
		o.MaxBackoff = p.MaxBackoff
		o.Backoff = growBackoff{policy: p, now: now}
		o.RateLimiter = ratelimit.None
	})
}

// noRetry disables retries for one call (health).
func noRetry(o *cwl.Options) { o.Retryer = aws.NopRetryer{} }

// sdkParts is what buildSDK produces.
type sdkParts struct {
	api       API
	sts       stsAPI
	region    string
	transport *http.Transport
}

// dialHook replaces the dialer in unit tests (live-tail host prefix).
var dialHook func(ctx context.Context, network, addr string) (net.Conn, error)

// buildSDK creates the AWS SDK clients from a defaulted, validated config.
func buildSDK(ctx context.Context, cfg Config) (*sdkParts, error) {
	tlsCfg, err := buildTLS(cfg.TLS)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSClientConfig:       tlsCfg,
		TLSHandshakeTimeout:   cfg.ConnectTimeout,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	if dialHook != nil {
		tr.DialContext = dialHook
	}
	if cfg.Proxy != "" {
		p, err := url.Parse(cfg.Proxy)
		if err != nil {
			return nil, ConfigError("proxy is invalid")
		}
		tr.Proxy = http.ProxyURL(p)
	}
	ua := UserAgent
	if cfg.UserAgentSuffix != "" {
		ua += " " + cfg.UserAgentSuffix
	}
	policy := cfg.Retry.withDefaults()
	hc := &hookedHTTPClient{
		client:         &http.Client{Transport: tr, CheckRedirect: noCrossHostRedirect},
		userAgent:      ua,
		headers:        cfg.Headers,
		hooks:          cfg.Hooks,
		logger:         cfg.Logger,
		maxAttempts:    policy.MaxRetries + 1,
		requestTimeout: cfg.RequestTimeout,
		now:            time.Now,
	}
	if cfg.SDKClient != nil {
		region := cfg.Region
		if c, ok := cfg.SDKClient.(*cwl.Client); ok && region == "" {
			region = c.Options().Region
		}
		return &sdkParts{api: cfg.SDKClient, region: region, transport: tr}, nil
	}

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithHTTPClient(hc),
		awsconfig.WithRetryer(func() aws.Retryer { return newRetryer(policy, time.Now) }),
	}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}
	if cfg.Credentials != nil {
		opts = append(opts, awsconfig.WithCredentialsProvider(cfg.Credentials))
	}
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey.Reveal(), cfg.SessionToken.Reveal())))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, ConfigError("AWS configuration cannot be loaded (profile or shared config)")
	}
	if awsCfg.Region == "" {
		return nil, ConfigError("region is not set (GROW_AWS_CLOUDWATCH_LOGS_REGION, AWS_REGION or the profile)")
	}
	withEndpoint := func(o *sts.Options) {
		if cfg.URL != "" {
			o.BaseEndpoint = aws.String(cfg.URL)
		}
		o.APIOptions = append(o.APIOptions, addAttemptCounter)
	}
	stsClient := sts.NewFromConfig(awsCfg, withEndpoint)
	if cfg.RoleARN != "" {
		provider := stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleARN, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = DefaultRoleSessionName
			if cfg.RoleSessionName != "" {
				o.RoleSessionName = cfg.RoleSessionName
			}
			if cfg.ExternalID != "" {
				o.ExternalID = aws.String(cfg.ExternalID)
			}
		})
		awsCfg.Credentials = aws.NewCredentialsCache(provider)
		stsClient = sts.NewFromConfig(awsCfg, withEndpoint)
	}
	client := cwl.NewFromConfig(awsCfg, func(o *cwl.Options) {
		if cfg.URL != "" {
			o.BaseEndpoint = aws.String(cfg.URL)
		}
		o.APIOptions = append(o.APIOptions, addAttemptCounter)
	})
	return &sdkParts{api: client, sts: stsClient, region: awsCfg.Region, transport: tr}, nil
}

// fractionalMillis matches a JSON epoch-millisecond number with a fraction
// ("createTime": 1795034978649.759) as sent by some emulators.
var fractionalMillis = regexp.MustCompile(`("createTime"\s*:\s*-?\d+)\.\d+`)

// integerMillis is a deserialize middleware for DescribeQueries: it drops the
// fraction of createTime before the SDK parses it as an integer (Moto sends
// floats; AWS sends integers, which pass through unchanged).
var integerMillis = middleware.DeserializeMiddlewareFunc("GrowIntegerMillis",
	func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
		middleware.DeserializeOutput, middleware.Metadata, error,
	) {
		out, md, err := next.HandleDeserialize(ctx, in)
		if err != nil {
			return out, md, err
		}
		if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp.StatusCode == http.StatusOK && resp.Body != nil {
			raw, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if rerr != nil {
				return out, md, rerr
			}
			resp.Body = io.NopCloser(bytes.NewReader(fractionalMillis.ReplaceAll(raw, []byte("$1"))))
		}
		return out, md, nil
	})

// tolerantMillis installs integerMillis on one operation.
func tolerantMillis(o *cwl.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return stack.Deserialize.Add(integerMillis, middleware.After)
	})
}
