package cloudwatchlogs

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// DefaultURL is empty: without an endpoint override the AWS SDK resolves
// https://logs.<region>.amazonaws.com itself.
const DefaultURL = ""

// DefaultRoleSessionName names the STS session when RoleARN is set.
const DefaultRoleSessionName = "grow-sdk"

// APIVersion is the Amazon CloudWatch Logs API version this package targets.
const APIVersion = "2014-03-28"

// Per-event size limits (bytes of the UTF-8 message).
const (
	// DefaultMaxEventBytes is the default per-event size policy (256 KiB).
	DefaultMaxEventBytes = 262_144
	// MaxEventBytesLimit is the hard per-event cap: 1 MiB minus the 26 bytes of
	// per-event overhead CloudWatch Logs counts against a PutLogEvents call.
	MaxEventBytesLimit = 1_048_550
)

// OversizePolicy decides what happens to an event larger than MaxEventBytes.
type OversizePolicy string

// Oversize policies.
const (
	// OversizeTruncate cuts the message on a UTF-8 boundary and appends
	// " …[truncated N bytes]" (default). Normalized log records are truncated
	// in their body so the message stays valid JSON.
	OversizeTruncate OversizePolicy = "truncate"
	// OversizeDrop drops the event (counted in PutLogEventsResult.Dropped).
	OversizeDrop OversizePolicy = "drop"
	// OversizeError fails the call with a VALIDATION error.
	OversizeError OversizePolicy = "error"
)

// vendorEnv lists the vendor variables consulted after GROW_AWS_CLOUDWATCH_LOGS_*.
var vendorEnv = VendorEnv{URL: []string{"AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "AWS_ENDPOINT_URL"}}

// Config configures a Client. Build it with a struct literal or ConfigFromEnv;
// zero values mean "use the default". Call Validate to check it.
//
// Credentials not given here come from the standard AWS chain (environment,
// shared config/SSO, web identity, container and instance metadata).
type Config struct {
	CommonConfig

	// Region is the AWS region (env GROW_AWS_CLOUDWATCH_LOGS_REGION, AWS_REGION,
	// AWS_DEFAULT_REGION, then the shared config profile).
	Region string
	// AccessKeyID is a static access key id (env GROW_AWS_CLOUDWATCH_LOGS_ACCESS_KEY_ID,
	// AWS_ACCESS_KEY_ID). It must be given together with SecretAccessKey.
	AccessKeyID string
	// SecretAccessKey is the static secret key (env …_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY).
	SecretAccessKey Secret
	// SessionToken is the optional session token (env …_SESSION_TOKEN, AWS_SESSION_TOKEN).
	SessionToken Secret
	// Profile is the shared config profile (env …_PROFILE, AWS_PROFILE).
	Profile string
	// Credentials is an AWS SDK credentials provider used instead of the key
	// fields and the default chain.
	Credentials aws.CredentialsProvider
	// RoleARN is a role to assume with STS before calling CloudWatch Logs
	// (env GROW_AWS_CLOUDWATCH_LOGS_ROLE_ARN).
	RoleARN string
	// RoleSessionName names the assumed-role session (default "grow-sdk",
	// env …_ROLE_SESSION_NAME).
	RoleSessionName string
	// ExternalID is passed to AssumeRole (env …_EXTERNAL_ID).
	ExternalID string
	// LogGroup is the default log group of the LogClient, Search, Tail and
	// ListStreams (env …_LOG_GROUP, CLOUDWATCH_LOG_GROUP).
	LogGroup string
	// LogStream is the stream the LogClient writes to (env …_LOG_STREAM,
	// CLOUDWATCH_LOG_STREAM; default "<hostname>-<pid>-<random6>", created on demand).
	LogStream string
	// AccountID is the AWS account used to build log group ARNs for live tail
	// (env …_ACCOUNT_ID); when empty the ARN is looked up with DescribeLogGroups.
	AccountID string
	// MaxEventBytes is the per-event size policy in UTF-8 bytes (default 262,144;
	// at most MaxEventBytesLimit; env …_MAX_EVENT_BYTES).
	MaxEventBytes int
	// OversizePolicy applies to events larger than MaxEventBytes (default
	// OversizeTruncate; env …_OVERSIZE_POLICY).
	OversizePolicy OversizePolicy
	// DisableAutoCreate stops PutLogEvents and the LogClient from creating a
	// missing log stream (env …_AUTO_CREATE=false).
	DisableAutoCreate bool
	// SDKClient is an existing AWS SDK client to use instead of building one. It
	// is never closed by this package and excludes the credential fields.
	SDKClient API
}

// ConfigFromEnv reads the configuration from the process environment.
func ConfigFromEnv() (Config, error) { return ConfigFromEnvWith(Config{}, nil) }

// ConfigFromEnvWith resolves explicit values > GROW_AWS_CLOUDWATCH_LOGS_* > vendor
// variables (AWS_*, CLOUDWATCH_*) > defaults. env replaces the process
// environment when non-nil.
func ConfigFromEnvWith(explicit Config, env map[string]string) (Config, error) {
	r := NewEnvReader(EnvPrefix, env)
	c := explicit
	if err := c.applyEnv(r, vendorEnv); err != nil {
		return Config{}, err
	}
	str := func(dst *string, name string, vendor ...string) {
		if *dst == "" {
			if v, ok := r.String(name, vendor...); ok {
				*dst = v
			}
		}
	}
	secret := func(dst *Secret, name string, vendor ...string) {
		if dst.IsZero() {
			if v, ok := r.String(name, vendor...); ok {
				*dst = NewSecret(v)
			}
		}
	}
	str(&c.Region, "REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
	if c.SDKClient == nil {
		str(&c.AccessKeyID, "ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
		secret(&c.SecretAccessKey, "SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
		secret(&c.SessionToken, "SESSION_TOKEN", "AWS_SESSION_TOKEN")
		str(&c.Profile, "PROFILE", "AWS_PROFILE")
		str(&c.RoleARN, "ROLE_ARN")
		str(&c.RoleSessionName, "ROLE_SESSION_NAME")
		str(&c.ExternalID, "EXTERNAL_ID")
	}
	str(&c.LogGroup, "LOG_GROUP", "CLOUDWATCH_LOG_GROUP")
	str(&c.LogStream, "LOG_STREAM", "CLOUDWATCH_LOG_STREAM")
	str(&c.AccountID, "ACCOUNT_ID")
	if c.MaxEventBytes == 0 {
		n, ok, err := r.Int("MAX_EVENT_BYTES")
		if err != nil {
			return Config{}, err
		}
		if ok {
			c.MaxEventBytes = n
		}
	}
	if c.OversizePolicy == "" {
		if v, ok := r.String("OVERSIZE_POLICY"); ok {
			c.OversizePolicy = OversizePolicy(strings.ToLower(strings.TrimSpace(v)))
		}
	}
	if !c.DisableAutoCreate {
		v, ok, err := r.Bool("AUTO_CREATE")
		if err != nil {
			return Config{}, err
		}
		c.DisableAutoCreate = ok && !v
	}
	c.CommonConfig = c.withDefaults(DefaultURL)
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate checks the configuration and returns a CONFIG error. A missing
// region is reported by NewClient, after the AWS chain had its chance.
func (c Config) Validate() error {
	common := c.withDefaults(DefaultURL)
	if common.URL == "" {
		common.URL = "https://logs.amazonaws.com" // only to validate the other common fields
	}
	if err := common.validate(); err != nil {
		return err
	}
	hasKey, hasSecret := c.AccessKeyID != "", !c.SecretAccessKey.IsZero()
	switch {
	case hasKey != hasSecret:
		return ConfigError("accessKeyId and secretAccessKey must be given together")
	case !c.SessionToken.IsZero() && !hasKey:
		return ConfigError("sessionToken requires accessKeyId and secretAccessKey")
	case c.Credentials != nil && hasKey:
		return ConfigError("credentials cannot be combined with accessKeyId and secretAccessKey")
	case c.SDKClient != nil && (hasKey || c.Credentials != nil || c.Profile != "" || c.RoleARN != ""):
		return ConfigError("sdkClient cannot be combined with credentials, profile or roleArn")
	case c.RoleARN != "" && !strings.HasPrefix(c.RoleARN, "arn:"):
		return ConfigError("roleArn must be an IAM role ARN")
	case c.MaxEventBytes < 0 || c.MaxEventBytes > MaxEventBytesLimit:
		return ConfigError(fmt.Sprintf("maxEventBytes must be between 1 and %d", MaxEventBytesLimit))
	}
	switch c.OversizePolicy {
	case "", OversizeTruncate, OversizeDrop, OversizeError:
	default:
		return ConfigError("oversizePolicy must be truncate, drop or error")
	}
	if c.LogGroup != "" {
		if err := validateGroupName("config", c.LogGroup); err != nil {
			return ConfigError("logGroup is not a valid log group name")
		}
	}
	if c.LogStream != "" {
		if err := validateStreamName("config", c.LogStream); err != nil {
			return ConfigError("logStream is not a valid log stream name")
		}
	}
	return nil
}

func (c Config) maxEventBytes() int {
	if c.MaxEventBytes <= 0 {
		return DefaultMaxEventBytes
	}
	return c.MaxEventBytes
}

func (c Config) oversizePolicy() OversizePolicy {
	if c.OversizePolicy == "" {
		return OversizeTruncate
	}
	return c.OversizePolicy
}

// String renders the configuration with every credential redacted.
func (c Config) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Config{%s", c.describe())
	add := func(name, v string) {
		if v != "" {
			fmt.Fprintf(&b, " %s:%s", name, v)
		}
	}
	add("Region", c.Region)
	add("AccessKeyID", redactKeyID(c.AccessKeyID))
	if !c.SecretAccessKey.IsZero() {
		add("SecretAccessKey", c.SecretAccessKey.String())
	}
	if !c.SessionToken.IsZero() {
		add("SessionToken", c.SessionToken.String())
	}
	add("Profile", c.Profile)
	if c.Credentials != nil {
		add("Credentials", "provider")
	}
	add("RoleARN", c.RoleARN)
	add("RoleSessionName", c.RoleSessionName)
	if c.ExternalID != "" {
		add("ExternalID", Redacted)
	}
	add("LogGroup", c.LogGroup)
	add("LogStream", c.LogStream)
	add("AccountID", c.AccountID)
	if c.MaxEventBytes != 0 {
		add("MaxEventBytes", fmt.Sprint(c.MaxEventBytes))
	}
	add("OversizePolicy", string(c.OversizePolicy))
	if c.DisableAutoCreate {
		add("DisableAutoCreate", "true")
	}
	if c.SDKClient != nil {
		add("SDKClient", "set")
	}
	b.WriteString("}")
	return b.String()
}

// GoString renders the configuration with every credential redacted.
func (c Config) GoString() string { return c.String() }

// redactKeyID keeps the first four characters of an access key id (the key
// type, e.g. AKIA/ASIA) and redacts the rest.
func redactKeyID(id string) string {
	if id == "" {
		return ""
	}
	if len(id) <= 4 {
		return Redacted
	}
	return id[:4] + Redacted
}
