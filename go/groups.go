package cloudwatchlogs

import (
	"context"
	"fmt"
	"iter"
	"regexp"
	"slices"
	"strings"
	"time"

	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// RetentionDays lists the retention values CloudWatch Logs accepts.
var RetentionDays = []int{1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653}

// MaxDescribePageSize is the largest DescribeLogGroups / DescribeLogStreams page.
const MaxDescribePageSize = 50

var groupNameRE = regexp.MustCompile(`^[._\-/#A-Za-z0-9]+$`)

func validateGroupName(op, name string) error {
	if name == "" || len(name) > 512 || !groupNameRE.MatchString(name) {
		return ValidationError(op, "log group name must be 1-512 characters of [.-_/#A-Za-z0-9]")
	}
	return nil
}

func validateStreamName(op, name string) error {
	if name == "" || len(name) > 512 || strings.ContainsAny(name, ":*") {
		return ValidationError(op, "log stream name must be 1-512 characters without ':' or '*'")
	}
	return nil
}

func validateRetention(op string, days int) error {
	if days != 0 && !slices.Contains(RetentionDays, days) {
		return ValidationError(op, fmt.Sprintf("retention %d is not one of the allowed values", days))
	}
	return nil
}

// limitYield stops an iteration after limit items (limit <= 0 = no limit).
func limitYield[T any](limit int, yield func(T, error) bool) func(T, error) bool {
	if limit <= 0 {
		return yield
	}
	n := 0
	return func(v T, err error) bool {
		if !yield(v, err) || err != nil {
			return false
		}
		n++
		return n < limit
	}
}

func validatePageSize(op string, size, maxSize int) error {
	if size < 0 || size > maxSize {
		return ValidationError(op, fmt.Sprintf("pageSize must be between 1 and %d", maxSize))
	}
	return nil
}

// trimStarARN removes the ":*" suffix DescribeLogGroups puts on group ARNs.
func trimStarARN(arn string) string { return strings.TrimSuffix(arn, ":*") }

// LogGroup describes a log group.
type LogGroup struct {
	// Name is the log group name.
	Name string
	// ARN is the group ARN without the trailing ":*".
	ARN string
	// CreationTime is when the group was created.
	CreationTime time.Time
	// RetentionDays is the retention in days; 0 means never expire.
	RetentionDays int
	// KMSKeyID is the ARN of the KMS key that encrypts the group, if any.
	KMSKeyID string
	// StoredBytes is the stored size (approximate and delayed on AWS).
	StoredBytes int64
	// MetricFilterCount is the number of metric filters.
	MetricFilterCount int
	// Class is the log group class (STANDARD, INFREQUENT_ACCESS, …) when reported.
	Class string
}

func toLogGroup(g types.LogGroup) LogGroup {
	arn := deref(g.LogGroupArn)
	if arn == "" {
		arn = trimStarARN(deref(g.Arn))
	}
	return LogGroup{
		Name:              deref(g.LogGroupName),
		ARN:               arn,
		CreationTime:      msTime(g.CreationTime),
		RetentionDays:     int(deref(g.RetentionInDays)),
		KMSKeyID:          deref(g.KmsKeyId),
		StoredBytes:       deref(g.StoredBytes),
		MetricFilterCount: int(deref(g.MetricFilterCount)),
		Class:             string(g.LogGroupClass),
	}
}

// LogStream describes a log stream.
type LogStream struct {
	// Name is the stream name.
	Name string
	// ARN is the stream ARN.
	ARN string
	// CreationTime is when the stream was created.
	CreationTime time.Time
	// FirstEventTime and LastEventTime are the first and last event
	// timestamps (zero when empty; LastEventTime is eventually consistent).
	FirstEventTime time.Time
	LastEventTime  time.Time
	// LastIngestionTime is when the last event was ingested.
	LastIngestionTime time.Time
	// StoredBytes is the stored size (deprecated by AWS; may be 0).
	StoredBytes int64
}

func toLogStream(s types.LogStream) LogStream {
	return LogStream{
		Name:              deref(s.LogStreamName),
		ARN:               deref(s.Arn),
		CreationTime:      msTime(s.CreationTime),
		FirstEventTime:    msTime(s.FirstEventTimestamp),
		LastEventTime:     msTime(s.LastEventTimestamp),
		LastIngestionTime: msTime(s.LastIngestionTime),
		StoredBytes:       deref(s.StoredBytes), //nolint:staticcheck // still returned by AWS and Moto
	}
}

// msTime converts epoch milliseconds to a UTC time (nil or 0 → zero time).
func msTime(ms *int64) time.Time {
	if ms == nil || *ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(*ms).UTC()
}

// EnsureLogGroupOptions configure EnsureLogGroup.
type EnsureLogGroupOptions struct {
	// RetentionDays sets the retention in days (one of RetentionDays): nil
	// leaves it unchanged, 0 removes it (events never expire). Use Days(n).
	RetentionDays *int
	// KMSKeyID encrypts a new group. An existing group with a different key
	// gives a CONFLICT error (keys are never re-associated silently).
	KMSKeyID string
	// Tags are added to the group (existing tags are kept, same keys overwritten).
	Tags map[string]string
}

// EnsureLogGroupResult reports what EnsureLogGroup did.
type EnsureLogGroupResult struct {
	// Created is true when the group did not exist before.
	Created bool
	// ARN is the group ARN without ":*".
	ARN string
}

// EnsureLogGroup creates the log group when missing. On an existing group it
// adds the requested tags (never removes any), re-applies the retention (when
// opts.RetentionDays is set; 0 removes it) and checks the KMS key (CONFLICT
// when it differs).
func (c *Client) EnsureLogGroup(ctx context.Context, name string, opts EnsureLogGroupOptions) (EnsureLogGroupResult, error) {
	op := "ensureLogGroup " + name
	if err := validateGroupName(op, name); err != nil {
		return EnsureLogGroupResult{}, err
	}
	if opts.RetentionDays != nil {
		if err := validateRetention(op, *opts.RetentionDays); err != nil {
			return EnsureLogGroupResult{}, err
		}
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return EnsureLogGroupResult{}, err
	}
	defer cancel()
	in := &cwl.CreateLogGroupInput{LogGroupName: &name}
	if opts.KMSKeyID != "" {
		in.KmsKeyId = &opts.KMSKeyID
	}
	if len(opts.Tags) > 0 {
		in.Tags = opts.Tags
	}
	created := true
	if _, err := c.api.CreateLogGroup(ctx, in); err != nil {
		mapped := c.mapError(op, err)
		if CodeOf(mapped) != ErrorCodeConflict {
			return EnsureLogGroupResult{}, mapped
		}
		created = false
	}
	group, err := c.findGroup(ctx, op, name)
	if err != nil {
		return EnsureLogGroupResult{}, err
	}
	res := EnsureLogGroupResult{Created: created, ARN: group.ARN}
	if !created {
		if opts.KMSKeyID != "" && !sameKMSKey(group.KMSKeyID, opts.KMSKeyID) {
			return res, &Error{Code: ErrorCodeConflict, Message: op + ": the existing log group uses a different KMS key"}
		}
		if len(opts.Tags) > 0 {
			if _, err := c.api.TagResource(ctx, &cwl.TagResourceInput{ResourceArn: &group.ARN, Tags: opts.Tags}); err != nil {
				return res, c.mapError(op, err)
			}
		}
	}
	if opts.RetentionDays != nil {
		if err := c.applyRetention(ctx, name, *opts.RetentionDays); err != nil {
			return res, c.mapError(op, err)
		}
	}
	return res, nil
}

// sameKMSKey compares a group's key ARN with a requested key id, ARN or alias.
func sameKMSKey(existing, requested string) bool {
	if existing == requested {
		return true
	}
	return existing != "" && (strings.HasSuffix(existing, "/"+requested) || strings.HasSuffix(existing, ":"+requested))
}

// findGroup returns the group named exactly name (NOT_FOUND otherwise).
func (c *Client) findGroup(ctx context.Context, op, name string) (LogGroup, error) {
	var token *string
	for {
		out, err := c.api.DescribeLogGroups(ctx, &cwl.DescribeLogGroupsInput{LogGroupNamePrefix: &name, NextToken: token})
		if err != nil {
			return LogGroup{}, c.mapError(op, err)
		}
		for _, g := range out.LogGroups {
			if deref(g.LogGroupName) == name {
				return toLogGroup(g), nil
			}
		}
		if deref(out.NextToken) == "" || deref(out.NextToken) == deref(token) {
			return LogGroup{}, NotFoundError(op, "log group does not exist")
		}
		token = out.NextToken
	}
}

// GetLogGroup returns the group named exactly name; ok is false when it does
// not exist.
func (c *Client) GetLogGroup(ctx context.Context, name string) (group LogGroup, ok bool, err error) {
	op := "getLogGroup " + name
	if err := validateGroupName(op, name); err != nil {
		return LogGroup{}, false, err
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return LogGroup{}, false, err
	}
	defer cancel()
	g, err := c.findGroup(ctx, op, name)
	switch {
	case err == nil:
		return g, true, nil
	case CodeOf(err) == ErrorCodeNotFound:
		return LogGroup{}, false, nil
	}
	return LogGroup{}, false, err
}

// EnsureLogStreamOptions configure EnsureLogStream.
type EnsureLogStreamOptions struct {
	// CreateGroup creates the log group first (EnsureLogGroup without options).
	CreateGroup bool
}

// EnsureLogStreamResult reports what EnsureLogStream did.
type EnsureLogStreamResult struct {
	// Created is true when the stream did not exist before.
	Created bool
}

// EnsureLogStream creates the stream when missing. A missing group gives
// NOT_FOUND unless opts.CreateGroup is set.
func (c *Client) EnsureLogStream(ctx context.Context, group, stream string, opts EnsureLogStreamOptions) (EnsureLogStreamResult, error) {
	op := "ensureLogStream " + group + "/" + stream
	if err := validateGroupName(op, group); err != nil {
		return EnsureLogStreamResult{}, err
	}
	if err := validateStreamName(op, stream); err != nil {
		return EnsureLogStreamResult{}, err
	}
	if opts.CreateGroup {
		if _, err := c.EnsureLogGroup(ctx, group, EnsureLogGroupOptions{}); err != nil {
			return EnsureLogStreamResult{}, err
		}
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return EnsureLogStreamResult{}, err
	}
	defer cancel()
	if _, err := c.api.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: &group, LogStreamName: &stream}); err != nil {
		mapped := c.mapError(op, err)
		if CodeOf(mapped) == ErrorCodeConflict {
			return EnsureLogStreamResult{Created: false}, nil
		}
		return EnsureLogStreamResult{}, mapped
	}
	return EnsureLogStreamResult{Created: true}, nil
}

// PutRetentionPolicy sets the retention of a group in days (one of
// RetentionDays). days == 0 removes the policy (events never expire).
func (c *Client) PutRetentionPolicy(ctx context.Context, group string, days int) error {
	op := "putRetentionPolicy " + group
	if err := validateGroupName(op, group); err != nil {
		return err
	}
	if err := validateRetention(op, days); err != nil {
		return err
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return err
	}
	defer cancel()
	return c.mapError(op, c.applyRetention(ctx, group, days))
}

// applyRetention sets the retention of a group; days == 0 removes it.
func (c *Client) applyRetention(ctx context.Context, group string, days int) error {
	if days == 0 {
		_, err := c.api.DeleteRetentionPolicy(ctx, &cwl.DeleteRetentionPolicyInput{LogGroupName: &group})
		return err
	}
	d := small32(days)
	_, err := c.api.PutRetentionPolicy(ctx, &cwl.PutRetentionPolicyInput{LogGroupName: &group, RetentionInDays: &d})
	return err
}

// Days returns a pointer to n for EnsureLogGroupOptions.RetentionDays
// (Days(0) removes the retention).
func Days(n int) *int { return &n }

// DeleteOptions configure DeleteLogGroup and DeleteLogStream.
type DeleteOptions struct {
	// IgnoreMissing returns (false, nil) instead of NOT_FOUND for a missing resource.
	IgnoreMissing bool
}

// DeleteLogGroup deletes a group with all its streams and events. It returns
// true when something was deleted.
func (c *Client) DeleteLogGroup(ctx context.Context, name string, opts DeleteOptions) (bool, error) {
	op := "deleteLogGroup " + name
	if err := validateGroupName(op, name); err != nil {
		return false, err
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return false, err
	}
	defer cancel()
	_, err = c.api.DeleteLogGroup(ctx, &cwl.DeleteLogGroupInput{LogGroupName: &name})
	return deleteResult(c.mapError(op, err), opts)
}

// DeleteLogStream deletes a stream and its events. It returns true when
// something was deleted.
func (c *Client) DeleteLogStream(ctx context.Context, group, stream string, opts DeleteOptions) (bool, error) {
	op := "deleteLogStream " + group + "/" + stream
	if err := validateGroupName(op, group); err != nil {
		return false, err
	}
	if err := validateStreamName(op, stream); err != nil {
		return false, err
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return false, err
	}
	defer cancel()
	_, err = c.api.DeleteLogStream(ctx, &cwl.DeleteLogStreamInput{LogGroupName: &group, LogStreamName: &stream})
	return deleteResult(c.mapError(op, err), opts)
}

func deleteResult(err error, opts DeleteOptions) (bool, error) {
	switch {
	case err == nil:
		return true, nil
	case opts.IgnoreMissing && CodeOf(err) == ErrorCodeNotFound:
		return false, nil
	}
	return false, err
}

// DescribeLogGroupsOptions configure DescribeLogGroups.
type DescribeLogGroupsOptions struct {
	// Prefix limits the result to names starting with it.
	Prefix string
	// PageSize is the page size of each call (1-50; 0 = service default).
	PageSize int
	// Limit stops after this many groups (0 = all).
	Limit int
}

// DescribeLogGroups iterates over the log groups (sorted by name) across all
// pages. The iteration stops at the first error, which is yielded once.
func (c *Client) DescribeLogGroups(ctx context.Context, opts DescribeLogGroupsOptions) iter.Seq2[LogGroup, error] {
	return func(yield func(LogGroup, error) bool) {
		op := "describeLogGroups"
		if err := validatePageSize(op, opts.PageSize, MaxDescribePageSize); err != nil {
			yield(LogGroup{}, err)
			return
		}
		in := &cwl.DescribeLogGroupsInput{}
		if opts.Prefix != "" {
			in.LogGroupNamePrefix = &opts.Prefix
		}
		if opts.PageSize > 0 {
			in.Limit = ptr(small32(opts.PageSize))
		}
		paginate(ctx, c, op, limitYield(opts.Limit, yield), func(ctx context.Context, token *string) ([]LogGroup, *string, error) {
			in.NextToken = token
			out, err := c.api.DescribeLogGroups(ctx, in)
			if err != nil {
				return nil, nil, err
			}
			groups := make([]LogGroup, len(out.LogGroups))
			for i, g := range out.LogGroups {
				groups[i] = toLogGroup(g)
			}
			return groups, out.NextToken, nil
		})
	}
}

// StreamOrder orders DescribeLogStreams results.
type StreamOrder string

// Stream orders.
const (
	OrderByLogStreamName StreamOrder = "LogStreamName"
	OrderByLastEventTime StreamOrder = "LastEventTime"
)

// DescribeLogStreamsOptions configure DescribeLogStreams.
type DescribeLogStreamsOptions struct {
	// Prefix limits the result to names starting with it (not with OrderByLastEventTime).
	Prefix string
	// OrderBy is OrderByLogStreamName (default) or OrderByLastEventTime.
	OrderBy StreamOrder
	// Descending reverses the order.
	Descending bool
	// PageSize is the page size of each call (1-50; 0 = service default).
	PageSize int
	// Limit stops after this many streams (0 = all).
	Limit int
}

// DescribeLogStreams iterates over the streams of a group across all pages.
// The iteration stops at the first error, which is yielded once.
func (c *Client) DescribeLogStreams(ctx context.Context, group string, opts DescribeLogStreamsOptions) iter.Seq2[LogStream, error] {
	return func(yield func(LogStream, error) bool) {
		op := "describeLogStreams " + group
		if err := validateGroupName(op, group); err != nil {
			yield(LogStream{}, err)
			return
		}
		if err := validatePageSize(op, opts.PageSize, MaxDescribePageSize); err != nil {
			yield(LogStream{}, err)
			return
		}
		in := &cwl.DescribeLogStreamsInput{LogGroupName: &group}
		switch opts.OrderBy {
		case "", OrderByLogStreamName:
		case OrderByLastEventTime:
			if opts.Prefix != "" {
				yield(LogStream{}, ValidationError(op, "prefix cannot be combined with orderBy LastEventTime"))
				return
			}
			in.OrderBy = types.OrderByLastEventTime
		default:
			yield(LogStream{}, ValidationError(op, "orderBy must be LogStreamName or LastEventTime"))
			return
		}
		if opts.OrderBy == OrderByLogStreamName {
			in.OrderBy = types.OrderByLogStreamName
		}
		if opts.Prefix != "" {
			in.LogStreamNamePrefix = &opts.Prefix
		}
		if opts.Descending {
			in.Descending = ptr(true)
		}
		if opts.PageSize > 0 {
			in.Limit = ptr(small32(opts.PageSize))
		}
		paginate(ctx, c, op, limitYield(opts.Limit, yield), func(ctx context.Context, token *string) ([]LogStream, *string, error) {
			in.NextToken = token
			out, err := c.api.DescribeLogStreams(ctx, in)
			if err != nil {
				return nil, nil, err
			}
			streams := make([]LogStream, len(out.LogStreams))
			for i, s := range out.LogStreams {
				streams[i] = toLogStream(s)
			}
			return streams, out.NextToken, nil
		})
	}
}

// paginate drives a token-paginated call, one TotalTimeout per page.
func paginate[T any](ctx context.Context, c *Client, op string, yield func(T, error) bool,
	page func(ctx context.Context, token *string) ([]T, *string, error),
) {
	var token *string
	for {
		pctx, cancel, err := c.begin(ctx, op)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		items, next, err := page(pctx, token)
		cancel()
		if err != nil {
			var zero T
			yield(zero, c.mapError(op, err))
			return
		}
		for _, it := range items {
			if !yield(it, nil) {
				return
			}
		}
		if deref(next) == "" || deref(next) == deref(token) {
			return
		}
		token = next
	}
}

// resolveGroupARN returns the ARN of a group given by name or ARN.
func (c *Client) resolveGroupARN(ctx context.Context, op, group string) (string, error) {
	if strings.HasPrefix(group, "arn:") {
		return trimStarARN(group), nil
	}
	if err := validateGroupName(op, group); err != nil {
		return "", err
	}
	g, err := c.findGroup(ctx, op, group)
	if err != nil {
		return "", err
	}
	return g.ARN, nil
}

// TagLogGroup adds or overwrites tags on a group given by name or ARN.
func (c *Client) TagLogGroup(ctx context.Context, group string, tags map[string]string) error {
	op := "tagLogGroup " + group
	if len(tags) == 0 {
		return ValidationError(op, "at least one tag is required")
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return err
	}
	defer cancel()
	arn, err := c.resolveGroupARN(ctx, op, group)
	if err != nil {
		return err
	}
	_, err = c.api.TagResource(ctx, &cwl.TagResourceInput{ResourceArn: &arn, Tags: tags})
	return c.mapError(op, err)
}

// UntagLogGroup removes tag keys from a group given by name or ARN.
func (c *Client) UntagLogGroup(ctx context.Context, group string, keys []string) error {
	op := "untagLogGroup " + group
	if len(keys) == 0 {
		return ValidationError(op, "at least one tag key is required")
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return err
	}
	defer cancel()
	arn, err := c.resolveGroupARN(ctx, op, group)
	if err != nil {
		return err
	}
	_, err = c.api.UntagResource(ctx, &cwl.UntagResourceInput{ResourceArn: &arn, TagKeys: keys})
	return c.mapError(op, err)
}

// ListLogGroupTags returns the tags of a group given by name or ARN.
func (c *Client) ListLogGroupTags(ctx context.Context, group string) (map[string]string, error) {
	op := "listLogGroupTags " + group
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return nil, err
	}
	defer cancel()
	arn, err := c.resolveGroupARN(ctx, op, group)
	if err != nil {
		return nil, err
	}
	out, err := c.api.ListTagsForResource(ctx, &cwl.ListTagsForResourceInput{ResourceArn: &arn})
	if err != nil {
		return nil, c.mapError(op, err)
	}
	tags := make(map[string]string, len(out.Tags))
	for k, v := range out.Tags {
		tags[k] = v
	}
	return tags, nil
}
