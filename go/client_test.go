package cloudwatchlogs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/smithy-go"
)

// isolateAWSEnv keeps the AWS SDK away from the developer's environment.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestConfigFromEnv(t *testing.T) {
	t.Run("core.config.env-precedence", func(t *testing.T) {
		env := map[string]string{
			"AWS_ENDPOINT_URL":                 "http://generic:1",
			"AWS_ENDPOINT_URL_CLOUDWATCH_LOGS": "http://vendor:2",
			"AWS_DEFAULT_REGION":               "eu-west-1",
			"AWS_REGION":                       "eu-central-1",
			"AWS_ACCESS_KEY_ID":                "AKIAVENDOR",
			"AWS_SECRET_ACCESS_KEY":            "vendor-secret",
			"AWS_SESSION_TOKEN":                "vendor-token",
			"AWS_PROFILE":                      "dev",
			"CLOUDWATCH_LOG_GROUP":             "vendor/group",
			"CLOUDWATCH_LOG_STREAM":            "vendor-stream",
		}
		cfg, err := ConfigFromEnvWith(Config{}, env)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.URL != "http://vendor:2" || cfg.Region != "eu-central-1" || cfg.AccessKeyID != "AKIAVENDOR" ||
			cfg.SecretAccessKey.Reveal() != "vendor-secret" || cfg.SessionToken.Reveal() != "vendor-token" ||
			cfg.Profile != "dev" || cfg.LogGroup != "vendor/group" || cfg.LogStream != "vendor-stream" {
			t.Fatalf("vendor: %s", cfg)
		}
		grow := map[string]string{
			EnvPrefix + "URL": "http://grow:3", EnvPrefix + "REGION": "us-west-2", EnvPrefix + "ACCESS_KEY_ID": "AKIAGROW",
			EnvPrefix + "SECRET_ACCESS_KEY": "grow-secret", EnvPrefix + "SESSION_TOKEN": "grow-token", EnvPrefix + "PROFILE": "p",
			EnvPrefix + "ROLE_ARN": "arn:aws:iam::1:role/r", EnvPrefix + "ROLE_SESSION_NAME": "sess", EnvPrefix + "EXTERNAL_ID": "ext-id",
			EnvPrefix + "LOG_GROUP": "grow/group", EnvPrefix + "LOG_STREAM": "grow-stream", EnvPrefix + "ACCOUNT_ID": "999",
			EnvPrefix + "MAX_EVENT_BYTES": "1000", EnvPrefix + "OVERSIZE_POLICY": "DROP", EnvPrefix + "AUTO_CREATE": "false",
		}
		for k, v := range env {
			grow[k] = v
		}
		cfg, err = ConfigFromEnvWith(Config{}, grow)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.URL != "http://grow:3" || cfg.Region != "us-west-2" || cfg.AccessKeyID != "AKIAGROW" || cfg.SecretAccessKey.Reveal() != "grow-secret" ||
			cfg.RoleARN != "arn:aws:iam::1:role/r" || cfg.RoleSessionName != "sess" || cfg.ExternalID != "ext-id" ||
			cfg.LogGroup != "grow/group" || cfg.LogStream != "grow-stream" || cfg.AccountID != "999" ||
			cfg.MaxEventBytes != 1000 || cfg.OversizePolicy != OversizeDrop || !cfg.DisableAutoCreate {
			t.Fatalf("grow: %s", cfg)
		}
		explicit := Config{CommonConfig: CommonConfig{URL: "https://explicit"}, Region: "ap-south-1", LogGroup: "x/y"}
		cfg, _ = ConfigFromEnvWith(explicit, grow)
		if cfg.URL != "https://explicit" || cfg.Region != "ap-south-1" || cfg.LogGroup != "x/y" {
			t.Fatal("explicit must win")
		}
		cfg, err = ConfigFromEnvWith(Config{}, map[string]string{})
		if err != nil || cfg.URL != DefaultURL || cfg.RequestTimeout != DefaultRequestTimeout {
			t.Fatalf("defaults: %v %s", err, cfg)
		}
	})
	t.Run("sdk client skips credential env", func(t *testing.T) {
		cfg, err := ConfigFromEnvWith(Config{SDKClient: &cwl.Client{}}, map[string]string{"AWS_ACCESS_KEY_ID": "AKIA", "AWS_SECRET_ACCESS_KEY": "x"})
		if err != nil || cfg.AccessKeyID != "" {
			t.Fatalf("%v %s", err, cfg)
		}
	})
	t.Run("invalid values", func(t *testing.T) {
		for name, env := range map[string]map[string]string{
			"duration":  {EnvPrefix + "TOTAL_TIMEOUT": "forever-xyz"},
			"int":       {EnvPrefix + "MAX_EVENT_BYTES": "lots-xyz"},
			"bool":      {EnvPrefix + "AUTO_CREATE": "maybe-xyz"},
			"url":       {EnvPrefix + "URL": "ftp://nope"},
			"policy":    {EnvPrefix + "OVERSIZE_POLICY": "explode"},
			"key-alone": {"AWS_ACCESS_KEY_ID": "AKIA"},
		} {
			_, err := ConfigFromEnvWith(Config{}, env)
			if !errors.Is(err, ErrConfig) || strings.Contains(err.Error(), "xyz") {
				t.Errorf("%s: %v", name, err)
			}
		}
	})
	t.Run("process env", func(t *testing.T) {
		t.Setenv(EnvPrefix+"LOG_GROUP", "proc/group")
		if cfg, err := ConfigFromEnv(); err != nil || cfg.LogGroup != "proc/group" {
			t.Fatalf("%v %s", err, cfg)
		}
	})
}

func TestConfigValidate(t *testing.T) {
	bad := map[string]Config{
		"url":            {CommonConfig: CommonConfig{URL: "mailto:x"}},
		"secret alone":   {SecretAccessKey: NewSecret("s")},
		"token alone":    {SessionToken: NewSecret("t")},
		"creds and keys": {AccessKeyID: "A", SecretAccessKey: NewSecret("s"), Credentials: credentials.NewStaticCredentialsProvider("a", "b", "")},
		"sdk and keys":   {AccessKeyID: "A", SecretAccessKey: NewSecret("s"), SDKClient: &cwl.Client{}},
		"sdk and role":   {RoleARN: "arn:aws:iam::1:role/r", SDKClient: &cwl.Client{}},
		"role":           {RoleARN: "role/r"},
		"max bytes":      {MaxEventBytes: MaxEventBytesLimit + 1},
		"negative bytes": {MaxEventBytes: -1},
		"policy":         {OversizePolicy: "nope"},
		"group":          {LogGroup: "bad group!"},
		"stream":         {LogStream: "bad:stream"},
	}
	for name, cfg := range bad {
		if err := cfg.Validate(); !errors.Is(err, ErrConfig) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := (Config{LogGroup: "a/b", LogStream: "s", OversizePolicy: OversizeError, MaxEventBytes: 10}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRedaction(t *testing.T) {
	t.Run("core.redaction.config", func(t *testing.T) {
		cfg := Config{
			CommonConfig:    CommonConfig{URL: "https://h", Headers: map[string]string{"X-Api-Key": "hdr-xyz"}},
			Region:          "us-east-1",
			AccessKeyID:     "AKIAABCDEFGHIJ",
			SecretAccessKey: NewSecret("sec-xyz"),
			SessionToken:    NewSecret("tok-xyz"),
			ExternalID:      "ext-xyz",
			Profile:         "p", RoleARN: "arn:aws:iam::1:role/r", RoleSessionName: "s", LogGroup: "g", LogStream: "s",
			AccountID: "1", MaxEventBytes: 5, OversizePolicy: OversizeDrop, DisableAutoCreate: true,
			Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""),
		}
		forms := fmt.Sprintf("%v|%+v|%#v|%s", cfg, cfg, cfg, cfg.String())
		for _, leak := range []string{"sec-xyz", "tok-xyz", "hdr-xyz", "ext-xyz", "ABCDEFGHIJ"} {
			if strings.Contains(forms, leak) {
				t.Fatalf("%s leaked: %s", leak, forms)
			}
		}
		if !strings.Contains(forms, "AKIA"+Redacted) || !strings.Contains(forms, "SDKClient") == strings.Contains(forms, "SDKClient:set") {
			t.Fatalf("forms: %s", forms)
		}
		if redactKeyID("abc") != Redacted || redactKeyID("") != "" {
			t.Fatal("redactKeyID")
		}
		s := Config{SDKClient: &cwl.Client{}}.String()
		if !strings.Contains(s, "SDKClient:set") {
			t.Fatal(s)
		}
	})
}

func TestNewClient(t *testing.T) {
	t.Run("missing region", func(t *testing.T) {
		isolateAWSEnv(t)
		if _, err := NewClient(Config{AccessKeyID: "A", SecretAccessKey: NewSecret("s")}); !errors.Is(err, ErrConfig) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("bad profile", func(t *testing.T) {
		isolateAWSEnv(t)
		if _, err := NewClient(Config{Region: "us-east-1", Profile: "nope"}); !errors.Is(err, ErrConfig) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("invalid config", func(t *testing.T) {
		if _, err := NewClient(Config{CommonConfig: CommonConfig{URL: "mailto:x"}}); !errors.Is(err, ErrConfig) {
			t.Fatalf("%v", err)
		}
		if _, err := NewClient(Config{CommonConfig: CommonConfig{TLS: TLSConfig{CACertPEM: "bad"}}, Region: "us-east-1"}); !errors.Is(err, ErrConfig) {
			t.Fatalf("bad TLS: %v", err)
		}
	})
	t.Run("from env", func(t *testing.T) {
		isolateAWSEnv(t)
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "ping.ok"))
		t.Setenv(EnvPrefix+"URL", m.URL)
		t.Setenv("AWS_REGION", "us-east-1")
		t.Setenv("AWS_ACCESS_KEY_ID", testAccessKey)
		t.Setenv("AWS_SECRET_ACCESS_KEY", testSecretKey)
		c, err := NewClientFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		if c.Region() != "us-east-1" || c.SDK() == nil || c.Config().LogStream == "" {
			t.Fatalf("accessors: %s %s", c.Region(), c.Config())
		}
		t.Setenv(EnvPrefix+"URL", "mailto:x")
		if _, err := NewClientFromEnv(); !errors.Is(err, ErrConfig) {
			t.Fatalf("bad env: %v", err)
		}
	})
	t.Run("proxy and headers", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "ping.ok"))
		c := newMockClient(t, m, func(cfg *Config) {
			cfg.Headers = map[string]string{"X-Extra": "1", "X-Auth-Token": "hdr-secret-1"}
			cfg.UserAgentSuffix = "app/1"
		})
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		req := m.callsOf("DescribeLogGroups")[0].Req
		if req.Header.Get("X-Extra") != "1" || !strings.Contains(req.Header.Get("User-Agent"), UserAgent+" app/1") {
			t.Fatalf("headers: %v", req.Header)
		}
		if !strings.HasPrefix(req.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
			t.Fatal("request not signed")
		}
		p := newMockClient(t, m, func(cfg *Config) { cfg.Proxy = "http://127.0.0.1:1" })
		if p == nil {
			t.Fatal("proxy client")
		}
	})
	t.Run("default stream name", func(t *testing.T) {
		n := defaultStreamName()
		if strings.ContainsAny(n, ":*") || len(strings.Split(n, "-")) < 3 {
			t.Fatal(n)
		}
	})
}

func TestClientCore(t *testing.T) {
	t.Run("ping and health ok", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "ping.ok"))
		c := newMockClient(t, m, nil)
		if d, err := c.Ping(t.Context()); err != nil || d <= 0 {
			t.Fatalf("%s %v", d, err)
		}
		hs, err := c.Health(t.Context())
		if err != nil || !hs.Healthy || hs.Status != "ok" {
			t.Fatalf("%+v %v", hs, err)
		}
		body := m.callsOf("DescribeLogGroups")[0].Body
		if body["limit"] != float64(1) || body["logGroupNamePrefix"] != "app/web" {
			t.Fatalf("body %v", body)
		}
	})
	t.Run("auth errors from fixtures", func(t *testing.T) {
		for _, name := range []string{"ping.auth-unknown-key", "ping.auth-bad-signature"} {
			m := newAWSMock(t).on("DescribeLogGroups", replay(t, name))
			c := newMockClient(t, m, nil)
			_, err := c.Ping(t.Context())
			e := wantErr(t, err, ErrorCodeAuthentication)
			if e.HTTPStatus != 403 || e.RequestID == "" || e.Retryable {
				t.Fatalf("%s: %+v", name, e)
			}
			hs, err := c.Health(t.Context())
			if err != nil || !hs.Healthy || hs.Details["auth"] != "failed" {
				t.Fatalf("health %+v %v", hs, err)
			}
		}
	})
	t.Run("permission denied", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "deleteLogGroup.access-denied"))
		_, err := newMockClient(t, m, nil).Ping(t.Context())
		wantCode(t, err, ErrorCodePermissionDenied)
	})
	t.Run("health states", func(t *testing.T) {
		cases := []struct {
			h       http.HandlerFunc
			healthy bool
			status  string
		}{
			{awsError(400, "ThrottlingException", "slow down"), true, "degraded"},
			{rawReply(500, "text/html", "<html>boom</html>"), false, "unhealthy"},
			{awsError(400, "InvalidParameterException", "bad"), true, "ok"},
			{rawReply(404, "text/html", "nope"), true, "ok"},
		}
		for i, tc := range cases {
			m := newAWSMock(t).on("DescribeLogGroups", tc.h)
			hs, err := newMockClient(t, m, nil).Health(t.Context())
			if err != nil || hs.Healthy != tc.healthy || hs.Status != tc.status {
				t.Errorf("case %d: %+v %v", i, hs, err)
			}
		}
		c := newMockClient(t, newAWSMock(t), func(cfg *Config) { cfg.URL = "http://" + closedAddr(t); cfg.ConnectTimeout = time.Second })
		if hs, err := c.Health(t.Context()); err != nil || hs.Healthy || hs.Status != "unreachable" {
			t.Fatalf("core.health.unreachable: %+v %v", hs, err)
		}
		slow := newAWSMock(t).on("DescribeLogGroups", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		})
		c = newMockClient(t, slow, func(cfg *Config) { cfg.RequestTimeout = 50 * time.Millisecond })
		if hs, err := c.Health(t.Context()); err != nil || hs.Status != "unreachable" {
			t.Fatalf("timeout: %+v %v", hs, err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := c.Health(ctx); !errors.Is(err, ErrCancelled) {
			t.Fatalf("cancelled: %v", err)
		}
		cancelSoon := newAWSMock(t).on("DescribeLogGroups", func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
		c = newMockClient(t, cancelSoon, nil)
		ctx, cancel = context.WithCancel(t.Context())
		time.AfterFunc(50*time.Millisecond, cancel)
		if _, err := c.Health(ctx); !errors.Is(err, ErrCancelled) {
			t.Fatalf("cancelled during call: %v", err)
		}
	})
	t.Run("server version", func(t *testing.T) {
		c := newMockClient(t, newAWSMock(t), nil)
		if _, err := c.ServerVersion(t.Context()); !errors.Is(err, ErrUnsupported) {
			t.Fatal(err)
		}
	})
	t.Run("core.close.idempotent", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "ping.ok"))
		c := newMockClient(t, m, nil)
		if first, second := c.Close(), c.Close(); first != nil || second != nil {
			t.Fatal("close")
		}
		if _, err := c.Ping(t.Context()); !errors.Is(err, ErrCancelled) {
			t.Fatalf("after close: %v", err)
		}
		if _, err := c.Health(t.Context()); !errors.Is(err, ErrCancelled) {
			t.Fatalf("health after close: %v", err)
		}
	})
	t.Run("hooks and retries", func(t *testing.T) {
		var reqs, resps, errs atomic.Int32
		var lastAttempt atomic.Int32
		hooks := &Hooks{
			OnRequest:  func(info RequestInfo) { reqs.Add(1); lastAttempt.Store(int32(info.Attempt)) },
			OnResponse: func(ResponseInfo) { resps.Add(1) },
			OnError:    func(ErrorInfo) { errs.Add(1) },
		}
		m := newAWSMock(t).on("DescribeLogGroups",
			awsError(400, "ThrottlingException", "slow"),
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(503)
			},
			replay(t, "ping.ok"))
		c := newMockClient(t, m, func(cfg *Config) { cfg.Hooks = hooks; cfg.Retry = fastRetry(3) })
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		if reqs.Load() != 3 || resps.Load() != 3 || lastAttempt.Load() != 3 {
			t.Fatalf("hooks: %d %d %d", reqs.Load(), resps.Load(), lastAttempt.Load())
		}
		c2 := newMockClient(t, newAWSMock(t), func(cfg *Config) {
			cfg.Hooks = hooks
			cfg.URL = "http://" + closedAddr(t)
			cfg.Retry = fastRetry(1)
		})
		_, err := c2.Ping(t.Context())
		wantCode(t, err, ErrorCodeUnavailable)
		if errs.Load() != 2 {
			t.Fatalf("error hooks: %d", errs.Load())
		}
	})
	t.Run("retry-after too long stops", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "120")
			w.Header().Set("Content-Type", "application/x-amz-json-1.1")
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"__type":"ThrottlingException","message":"slow"}`))
		})
		c := newMockClient(t, m, func(cfg *Config) { cfg.Retry = fastRetry(3) })
		_, err := c.Ping(t.Context())
		e := wantErr(t, err, ErrorCodeRateLimited)
		if e.RetryAfter != 120*time.Second || len(m.callsOf("DescribeLogGroups")) != 1 {
			t.Fatalf("%+v calls=%d", e, len(m.callsOf("DescribeLogGroups")))
		}
	})
	t.Run("backoff honours retry-after", func(t *testing.T) {
		b := growBackoff{policy: fastRetry(3).withDefaults(), now: time.Now}
		if d, err := b.BackoffDelay(1, errors.New("x")); err != nil || d > 2*time.Millisecond {
			t.Fatalf("%s %v", d, err)
		}
	})
	t.Run("sdk client", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "ping.ok"))
		sdk := cwl.New(cwl.Options{
			Region:       "eu-west-1",
			BaseEndpoint: aws.String(m.URL),
			Credentials:  credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
		})
		c, err := NewClient(Config{SDKClient: sdk})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Ping(t.Context()); err != nil || c.Region() != "eu-west-1" {
			t.Fatalf("%v %s", err, c.Region())
		}
		_ = c.Close()
		// a custom API without STS cannot build ARNs without an account id
		c2, _ := NewClient(Config{SDKClient: c.SDK(), Region: "us-east-1"})
		if _, err := c2.account(t.Context()); !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
	})
	t.Run("credentials provider", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "ping.ok"))
		c := newMockClient(t, m, func(cfg *Config) {
			cfg.AccessKeyID, cfg.SecretAccessKey = "", Secret{}
			cfg.Credentials = credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, "")
		})
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("assume role and account lookup", func(t *testing.T) {
		m := newAWSMock(t)
		m.on("AssumeRole", rawReply(200, "text/xml", `<AssumeRoleResponse><AssumeRoleResult><Credentials>
<AccessKeyId>ASIAROLE</AccessKeyId><SecretAccessKey>role-secret</SecretAccessKey><SessionToken>role-token</SessionToken>
<Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::1:assumed-role/r/s</Arn>
<AssumedRoleId>AROA:s</AssumedRoleId></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>`))
		m.on("GetCallerIdentity", rawReply(200, "text/xml", `<GetCallerIdentityResponse><GetCallerIdentityResult>
<Account>210987654321</Account><Arn>arn:aws:sts::210987654321:assumed-role/r/s</Arn><UserId>AROA:s</UserId>
</GetCallerIdentityResult></GetCallerIdentityResponse>`))
		m.on("DescribeLogGroups", replay(t, "ping.ok"))
		c := newMockClient(t, m, func(cfg *Config) {
			cfg.RoleARN, cfg.ExternalID, cfg.AccountID = "arn:aws:iam::1:role/r", "ext-1234", ""
		})
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := m.callsOf("AssumeRole"); len(got) != 1 || got[0].Req.PostForm.Get("RoleSessionName") != DefaultRoleSessionName {
			t.Fatalf("assume role calls: %d", len(got))
		}
		if !strings.Contains(m.callsOf("DescribeLogGroups")[0].Req.Header.Get("Authorization"), "ASIAROLE") {
			t.Fatal("role credentials not used")
		}
		acct, err := c.account(t.Context())
		if err != nil || acct != "210987654321" {
			t.Fatalf("%s %v", acct, err)
		}
		if acct, _ = c.account(t.Context()); acct != "210987654321" || len(m.callsOf("GetCallerIdentity")) != 1 {
			t.Fatal("account not cached")
		}
		bad := newAWSMock(t).on("GetCallerIdentity", rawReply(403, "text/xml",
			`<ErrorResponse><Error><Code>InvalidClientTokenId</Code><Message>bad</Message></Error></ErrorResponse>`))
		c3 := newMockClient(t, bad, func(cfg *Config) { cfg.AccountID = "" })
		if _, err := c3.account(t.Context()); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		empty := newAWSMock(t).on("GetCallerIdentity", rawReply(200, "text/xml",
			`<GetCallerIdentityResponse><GetCallerIdentityResult></GetCallerIdentityResult></GetCallerIdentityResponse>`))
		c4 := newMockClient(t, empty, func(cfg *Config) { cfg.AccountID = "" })
		if _, err := c4.account(t.Context()); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
	})
}

func TestErrorMapping(t *testing.T) {
	c := newMockClient(t, newAWSMock(t), nil)
	cases := map[string]ErrorCode{
		"ResourceNotFoundException":            ErrorCodeNotFound,
		"ResourceAlreadyExistsException":       ErrorCodeConflict,
		"InvalidParameterException":            ErrorCodeValidation,
		"LimitExceededException":               ErrorCodeValidation,
		"AccessDeniedException":                ErrorCodePermissionDenied,
		"UnrecognizedClientException":          ErrorCodeAuthentication,
		"ExpiredTokenException":                ErrorCodeAuthentication,
		"ThrottlingException":                  ErrorCodeRateLimited,
		"ServiceUnavailableException":          ErrorCodeUnavailable,
		"SessionTimeoutException":              ErrorCodeTimeout,
		"com.amazonaws.logs#SomethingNewThing": ErrorCodeValidation,
	}
	for code, want := range cases {
		err := c.mapError("op", &smithy.GenericAPIError{Code: code, Message: "msg " + testSecretKey})
		e := wantErr(t, err, want)
		if !strings.HasPrefix(e.Message, "op: ") || !strings.Contains(e.Message, Redacted) {
			t.Errorf("%s: %q", code, e.Message)
		}
	}
	if e := asError(t, c.mapError("op", &smithy.GenericAPIError{Code: "NewServerThing", Fault: smithy.FaultServer})); e.Code != ErrorCodeUnavailable {
		t.Fatalf("server fault: %+v", e)
	}
	if e := asError(t, c.mapError("op", &smithy.GenericAPIError{Code: "OperationAbortedException"})); !e.Retryable || e.Code != ErrorCodeConflict {
		t.Fatalf("aborted: %+v", e)
	}
	if AWSErrorCode(c.mapError("op", &smithy.GenericAPIError{Code: "ThrottlingException", Message: "ThrottlingException"})) != "ThrottlingException" {
		t.Fatal("AWSErrorCode")
	}
	if AWSErrorCode(errors.New("x")) != "" {
		t.Fatal("AWSErrorCode plain")
	}
	if err := c.mapError("op", context.Canceled); !errors.Is(err, ErrCancelled) || !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.mapError("op", context.DeadlineExceeded); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	if err := c.mapError("op", errors.New("failed to retrieve credentials")); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
	if err := c.mapError("op", &aws.MissingRegionError{}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if err := c.mapError("op", errors.New("weird "+testSecretKey)); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), testSecretKey) {
		t.Fatal(err)
	}
	if c.mapError("op", nil) != nil {
		t.Fatal("nil")
	}
	already := &Error{Code: ErrorCodeNotFound}
	if c.mapError("op", already) != already {
		t.Fatal("passthrough")
	}
	for status, want := range map[int]ErrorCode{403: ErrorCodePermissionDenied, 0: ErrorCodeUnavailable, 400: ErrorCodeValidation, 504: ErrorCodeTimeout, 502: ErrorCodeUnavailable} {
		if got := codeForStatus(status, ""); got != want {
			t.Errorf("%d: %s", status, got)
		}
	}
	if codeForStatus(403, "<Code>SignatureDoesNotMatch</Code>") != ErrorCodeAuthentication {
		t.Fatal("403 marker")
	}
}

func TestOperationOf(t *testing.T) {
	if operationOf("Logs_20140328.PutLogEvents") != "PutLogEvents" || operationOf("plain") != "plain" {
		t.Fatal("operationOf")
	}
	if ptr(3) == nil || deref[int](nil) != 0 {
		t.Fatal("helpers")
	}
}
