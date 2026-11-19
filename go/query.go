package cloudwatchlogs

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// MaxEventsPageSize is the largest FilterLogEvents / GetLogEvents page.
const MaxEventsPageSize = 10_000

// Logs Insights defaults.
const (
	DefaultQueryTimeout      = 60 * time.Second
	DefaultQueryPollInterval = time.Second
	MaxQueryPollInterval     = 5 * time.Second
	DefaultQueryLimitRetries = 3
)

// FilteredLogEvent is one event returned by FilterLogEvents.
type FilteredLogEvent struct {
	// EventID is unique within the group.
	EventID string
	// LogStreamName is the stream that holds the event.
	LogStreamName string
	// Message is the event text.
	Message string
	// Timestamp is the event time; IngestionTime when CloudWatch received it.
	Timestamp     time.Time
	IngestionTime time.Time
}

// StreamLogEvent is one event returned by GetLogEvents.
type StreamLogEvent struct {
	Message       string
	Timestamp     time.Time
	IngestionTime time.Time
}

// FilterLogEventsOptions configure FilterLogEvents.
type FilterLogEventsOptions struct {
	// Group is the log group (default: the client's LogGroup).
	Group string
	// Streams limits the search to these streams (exclusive with StreamPrefix).
	Streams []string
	// StreamPrefix limits the search to streams with this prefix.
	StreamPrefix string
	// Pattern is a CloudWatch Logs filter pattern (empty = every event).
	Pattern string
	// Start and End bound the event timestamps (inclusive, zero = open).
	Start time.Time
	End   time.Time
	// PageSize is the page size of each call (1-10,000; 0 = service default).
	PageSize int
	// Limit stops after this many events in total (0 = no limit).
	Limit int
}

// FilterLogEvents iterates over the events of a group that match the filter
// pattern, across all pages (AWS may return empty pages with a token). The
// iteration stops at the first error, which is yielded once.
func (c *Client) FilterLogEvents(ctx context.Context, opts FilterLogEventsOptions) iter.Seq2[FilteredLogEvent, error] {
	return func(yield func(FilteredLogEvent, error) bool) {
		in, op, err := c.filterInput(opts)
		if err != nil {
			yield(FilteredLogEvent{}, err)
			return
		}
		count := 0
		paginate(ctx, c, op, func(e FilteredLogEvent, err error) bool {
			if err != nil {
				return yield(e, err)
			}
			if !yield(e, nil) {
				return false
			}
			count++
			return opts.Limit <= 0 || count < opts.Limit
		}, func(ctx context.Context, token *string) ([]FilteredLogEvent, *string, error) {
			in.NextToken = token
			if opts.Limit > 0 {
				in.Limit = ptr(small32(min(opts.Limit-count, pageOr(opts.PageSize))))
			}
			out, err := c.api.FilterLogEvents(ctx, in)
			if err != nil {
				return nil, nil, err
			}
			return toFiltered(out.Events), out.NextToken, nil
		})
	}
}

// FilterLogEventsPage is one FilterLogEvents response.
type FilterLogEventsPage struct {
	// Events are ordered by timestamp, then event id.
	Events []FilteredLogEvent
	// NextToken is empty at the end (AWS may return empty pages with a token).
	NextToken string
}

// FilterLogEventsPage makes one FilterLogEvents request starting at nextToken
// ("" = first page). opts.Limit is ignored; PageSize caps the page.
func (c *Client) FilterLogEventsPage(ctx context.Context, opts FilterLogEventsOptions, nextToken string) (FilterLogEventsPage, error) {
	in, op, err := c.filterInput(opts)
	if err != nil {
		return FilterLogEventsPage{}, err
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return FilterLogEventsPage{}, err
	}
	defer cancel()
	if nextToken != "" {
		in.NextToken = &nextToken
	}
	out, err := c.api.FilterLogEvents(ctx, in)
	if err != nil {
		return FilterLogEventsPage{}, c.mapError(op, err)
	}
	page := FilterLogEventsPage{Events: toFiltered(out.Events), NextToken: deref(out.NextToken)}
	sortEvents(page.Events, false)
	if page.NextToken == nextToken {
		page.NextToken = ""
	}
	return page, nil
}

func pageOr(size int) int {
	if size <= 0 {
		return MaxEventsPageSize
	}
	return size
}

func (c *Client) filterInput(opts FilterLogEventsOptions) (*cwl.FilterLogEventsInput, string, error) {
	group := opts.Group
	if group == "" {
		group = c.cfg.LogGroup
	}
	op := "filterLogEvents " + group
	if group == "" {
		return nil, op, ConfigError("filterLogEvents: no log group given and logGroup is not set")
	}
	if err := validateGroupName(op, group); err != nil {
		return nil, op, err
	}
	switch {
	case len(opts.Streams) > 0 && opts.StreamPrefix != "":
		return nil, op, ValidationError(op, "streams and streamPrefix are mutually exclusive")
	case opts.Limit < 0:
		return nil, op, ValidationError(op, "limit must not be negative")
	case !opts.Start.IsZero() && !opts.End.IsZero() && opts.End.Before(opts.Start):
		return nil, op, ValidationError(op, "end must not be before start")
	}
	if err := validatePageSize(op, opts.PageSize, MaxEventsPageSize); err != nil {
		return nil, op, err
	}
	in := &cwl.FilterLogEventsInput{LogGroupName: &group}
	if len(opts.Streams) > 0 {
		in.LogStreamNames = opts.Streams
	}
	if opts.StreamPrefix != "" {
		in.LogStreamNamePrefix = &opts.StreamPrefix
	}
	if opts.Pattern != "" {
		in.FilterPattern = &opts.Pattern
	}
	if !opts.Start.IsZero() {
		in.StartTime = ptr(opts.Start.UnixMilli())
	}
	if !opts.End.IsZero() {
		in.EndTime = ptr(opts.End.UnixMilli())
	}
	if opts.PageSize > 0 {
		in.Limit = ptr(small32(opts.PageSize))
	}
	return in, op, nil
}

func toFiltered(events []types.FilteredLogEvent) []FilteredLogEvent {
	out := make([]FilteredLogEvent, len(events))
	for i, e := range events {
		out[i] = FilteredLogEvent{
			EventID:       deref(e.EventId),
			LogStreamName: deref(e.LogStreamName),
			Message:       deref(e.Message),
			Timestamp:     msTime(e.Timestamp),
			IngestionTime: msTime(e.IngestionTime),
		}
	}
	return out
}

// GetLogEventsOptions configure GetLogEvents.
type GetLogEventsOptions struct {
	// StartFromHead reads oldest first (forward). The default reads the newest
	// page first and then walks backwards.
	StartFromHead bool
	// Start and End bound the event timestamps (zero = open).
	Start time.Time
	End   time.Time
	// PageSize is the page size of each call (1-10,000; 0 = service default).
	PageSize int
	// Limit stops after this many events in total (0 = no limit).
	Limit int
}

// GetLogEvents iterates over the events of one stream. Forward iteration
// (StartFromHead) ends when the service returns the token it was sent; on a
// live stream that is the current end. Backward iteration yields pages newest
// first (events inside a page stay in ascending order). The iteration stops
// at the first error, which is yielded once.
func (c *Client) GetLogEvents(ctx context.Context, group, stream string, opts GetLogEventsOptions) iter.Seq2[StreamLogEvent, error] {
	return func(yield func(StreamLogEvent, error) bool) {
		op := "getLogEvents " + group + "/" + stream
		if err := validateGroupName(op, group); err != nil {
			yield(StreamLogEvent{}, err)
			return
		}
		if err := validateStreamName(op, stream); err != nil {
			yield(StreamLogEvent{}, err)
			return
		}
		if err := validatePageSize(op, opts.PageSize, MaxEventsPageSize); err != nil {
			yield(StreamLogEvent{}, err)
			return
		}
		if opts.Limit < 0 {
			yield(StreamLogEvent{}, ValidationError(op, "limit must not be negative"))
			return
		}
		in := &cwl.GetLogEventsInput{LogGroupName: &group, LogStreamName: &stream, StartFromHead: ptr(opts.StartFromHead)}
		if !opts.Start.IsZero() {
			in.StartTime = ptr(opts.Start.UnixMilli())
		}
		if !opts.End.IsZero() {
			in.EndTime = ptr(opts.End.UnixMilli())
		}
		if opts.PageSize > 0 {
			in.Limit = ptr(small32(opts.PageSize))
		}
		count := 0
		var token *string
		for {
			pctx, cancel, err := c.begin(ctx, op)
			if err != nil {
				yield(StreamLogEvent{}, err)
				return
			}
			in.NextToken = token
			if opts.Limit > 0 {
				in.Limit = ptr(small32(min(opts.Limit-count, pageOr(opts.PageSize))))
			}
			out, err := c.api.GetLogEvents(pctx, in)
			cancel()
			if err != nil {
				yield(StreamLogEvent{}, c.mapError(op, err))
				return
			}
			for _, e := range out.Events {
				if !yield(StreamLogEvent{Message: deref(e.Message), Timestamp: msTime(e.Timestamp), IngestionTime: msTime(e.IngestionTime)}, nil) {
					return
				}
				count++
				if opts.Limit > 0 && count >= opts.Limit {
					return
				}
			}
			next := out.NextBackwardToken
			if opts.StartFromHead {
				next = out.NextForwardToken
			}
			if deref(next) == "" || deref(next) == deref(token) {
				return
			}
			token = next
		}
	}
}

// QueryStatus is the status of a Logs Insights query.
type QueryStatus string

// Query statuses.
const (
	QueryScheduled QueryStatus = "Scheduled"
	QueryRunning   QueryStatus = "Running"
	QueryComplete  QueryStatus = "Complete"
	QueryFailed    QueryStatus = "Failed"
	QueryCancelled QueryStatus = "Cancelled"
	QueryTimeout   QueryStatus = "Timeout"
	QueryUnknown   QueryStatus = "Unknown"
)

// Terminal reports whether the query has finished (any status but Scheduled and Running).
func (s QueryStatus) Terminal() bool { return s != QueryScheduled && s != QueryRunning }

// QueryStatistics are the scan statistics of a query (AWS only).
type QueryStatistics struct {
	RecordsMatched float64
	RecordsScanned float64
	BytesScanned   float64
}

// QueryResults is the result of GetQueryResults or Query.
type QueryResults struct {
	// QueryID identifies the query.
	QueryID string
	// Status is the query status.
	Status QueryStatus
	// Rows maps field names (@timestamp, @message, @logStream, @log, @ptr, …) to values.
	Rows []map[string]string
	// Statistics are present when the service reports them.
	Statistics *QueryStatistics
}

// QueryOptions configure StartQuery and Query.
type QueryOptions struct {
	// Groups are log group names or ARNs (default: the client's LogGroup; at most 50).
	Groups []string
	// Start and End bound the query (default: the last hour up to now). The
	// API has second precision: Start is rounded down, End up.
	Start time.Time
	End   time.Time
	// Limit caps the returned rows (1-10,000; 0 = service default).
	Limit int
	// Timeout bounds Query (default 60 s); PollInterval is the first poll
	// interval (default 1 s, growing to 5 s).
	Timeout      time.Duration
	PollInterval time.Duration
	// RetryOnQueryLimit is how often Query retries StartQuery after
	// LimitExceededException (too many concurrent queries; default 3, negative = never).
	RetryOnQueryLimit int
}

// StartQuery starts a Logs Insights query and returns its id.
func (c *Client) StartQuery(ctx context.Context, query string, opts QueryOptions) (string, error) {
	const op = "startQuery"
	in, err := c.startQueryInput(op, query, opts)
	if err != nil {
		return "", err
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return "", err
	}
	defer cancel()
	out, err := c.api.StartQuery(ctx, in)
	if err != nil {
		return "", c.mapError(op, err)
	}
	return deref(out.QueryId), nil
}

func (c *Client) startQueryInput(op, query string, opts QueryOptions) (*cwl.StartQueryInput, error) {
	if strings.TrimSpace(query) == "" {
		return nil, ValidationError(op, "query string is required")
	}
	groups := opts.Groups
	if len(groups) == 0 && c.cfg.LogGroup != "" {
		groups = []string{c.cfg.LogGroup}
	}
	switch {
	case len(groups) == 0:
		return nil, ConfigError("startQuery: no log groups given and logGroup is not set")
	case len(groups) > 50:
		return nil, ValidationError(op, "at most 50 log groups")
	case opts.Limit < 0 || opts.Limit > MaxEventsPageSize:
		return nil, ValidationError(op, "limit must be between 1 and 10000")
	}
	end := opts.End
	if end.IsZero() {
		end = time.Now()
	}
	start := opts.Start
	if start.IsZero() {
		start = end.Add(-time.Hour)
	}
	if end.Before(start) {
		return nil, ValidationError(op, "end must not be before start")
	}
	endS := end.Unix()
	if end.Truncate(time.Second).Before(end) {
		endS++
	}
	in := &cwl.StartQueryInput{QueryString: &query, StartTime: ptr(start.Unix()), EndTime: ptr(endS)}
	arns := false
	for _, g := range groups {
		if strings.HasPrefix(g, "arn:") {
			arns = true
			continue
		}
		if err := validateGroupName(op, g); err != nil {
			return nil, err
		}
	}
	if arns {
		ids := make([]string, len(groups))
		for i, g := range groups {
			ids[i] = trimStarARN(g)
		}
		in.LogGroupIdentifiers = ids
	} else {
		in.LogGroupNames = groups
	}
	if opts.Limit > 0 {
		in.Limit = ptr(small32(opts.Limit))
	}
	return in, nil
}

// GetQueryResults returns the current status and rows of a query. While the
// status is Running, AWS may return partial rows.
func (c *Client) GetQueryResults(ctx context.Context, queryID string) (QueryResults, error) {
	op := "getQueryResults " + queryID
	if queryID == "" {
		return QueryResults{}, ValidationError(op, "query id is required")
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return QueryResults{}, err
	}
	defer cancel()
	out, err := c.api.GetQueryResults(ctx, &cwl.GetQueryResultsInput{QueryId: &queryID})
	if err != nil {
		return QueryResults{}, c.mapError(op, err)
	}
	res := QueryResults{QueryID: queryID, Status: QueryStatus(out.Status), Rows: make([]map[string]string, 0, len(out.Results))}
	if res.Status == "" {
		res.Status = QueryUnknown
	}
	for _, row := range out.Results {
		m := make(map[string]string, len(row))
		for _, f := range row {
			m[deref(f.Field)] = deref(f.Value)
		}
		res.Rows = append(res.Rows, m)
	}
	if s := out.Statistics; s != nil {
		res.Statistics = &QueryStatistics{RecordsMatched: s.RecordsMatched, RecordsScanned: s.RecordsScanned, BytesScanned: s.BytesScanned}
	}
	return res, nil
}

// Query runs a Logs Insights query to completion: StartQuery (retried on
// LimitExceededException), then GetQueryResults until a terminal status.
// Failed and Unknown give UNAVAILABLE, Cancelled CANCELLED and Timeout
// TIMEOUT errors naming the query id. When opts.Timeout passes the query is
// stopped (best effort) and a TIMEOUT error returned; when ctx is cancelled it
// is stopped and a CANCELLED error returned.
func (c *Client) Query(ctx context.Context, query string, opts QueryOptions) (QueryResults, error) {
	const op = "query"
	if _, err := c.startQueryInput(op, query, opts); err != nil {
		return QueryResults{}, err
	}
	timeout, interval := opts.Timeout, opts.PollInterval
	if timeout <= 0 {
		timeout = DefaultQueryTimeout
	}
	if interval <= 0 {
		interval = DefaultQueryPollInterval
	}
	retries := opts.RetryOnQueryLimit
	if retries == 0 {
		retries = DefaultQueryLimitRetries
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var id string
	for attempt := 0; ; attempt++ {
		var err error
		id, err = c.StartQuery(qctx, query, opts)
		if err == nil {
			break
		}
		if attempt >= retries || AWSErrorCode(err) != "LimitExceededException" {
			return QueryResults{}, c.queryContextErr(ctx, qctx, op, "", err)
		}
		if serr := sleepContext(qctx, interval<<attempt); serr != nil {
			return QueryResults{}, c.queryContextErr(ctx, qctx, op, "", serr)
		}
	}
	for {
		res, err := c.GetQueryResults(qctx, id)
		if err != nil {
			return QueryResults{}, c.queryContextErr(ctx, qctx, op, id, err)
		}
		if res.Status.Terminal() {
			switch res.Status {
			case QueryComplete:
				return res, nil
			case QueryCancelled:
				return res, &Error{Code: ErrorCodeCancelled, Message: fmt.Sprintf("%s %s: the query was cancelled", op, id)}
			case QueryTimeout:
				return res, &Error{Code: ErrorCodeTimeout, Message: fmt.Sprintf("%s %s: the query timed out in the service", op, id)}
			}
			return res, &Error{Code: ErrorCodeUnavailable, Message: fmt.Sprintf("%s %s: query ended with status %s", op, id, res.Status)}
		}
		if err := sleepContext(qctx, interval); err != nil {
			return QueryResults{}, c.queryContextErr(ctx, qctx, op, id, err)
		}
		interval = min(interval*3/2, MaxQueryPollInterval)
	}
}

// queryContextErr turns a failure inside Query into the right error and stops
// the query (best effort) when the caller cancelled or the timeout passed.
func (c *Client) queryContextErr(ctx, qctx context.Context, op, id string, err error) error {
	name := op
	if id != "" {
		name += " " + id
	}
	switch {
	case ctx.Err() != nil:
		c.stopQuietly(id)
		return contextErr(name, ctx.Err())
	case qctx.Err() != nil:
		c.stopQuietly(id)
		return &Error{Code: ErrorCodeTimeout, Message: name + ": query did not complete in time", Retryable: true, Err: context.DeadlineExceeded}
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return contextErr(name, err)
}

func (c *Client) stopQuietly(id string) {
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = c.StopQuery(ctx, id)
}

// StopQuery stops a running query. It returns false when the query had
// already finished (not an error).
func (c *Client) StopQuery(ctx context.Context, queryID string) (bool, error) {
	op := "stopQuery " + queryID
	if queryID == "" {
		return false, ValidationError(op, "query id is required")
	}
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return false, err
	}
	defer cancel()
	out, err := c.api.StopQuery(ctx, &cwl.StopQueryInput{QueryId: &queryID})
	if err != nil {
		return false, c.mapError(op, err)
	}
	return out.Success, nil
}

// QueryInfo describes one Logs Insights query.
type QueryInfo struct {
	QueryID      string
	QueryString  string
	Status       QueryStatus
	CreateTime   time.Time
	LogGroupName string
}

// DescribeQueriesOptions configure DescribeQueries.
type DescribeQueriesOptions struct {
	// Group limits the result to queries of this log group.
	Group string
	// Status limits the result to queries with this status.
	Status QueryStatus
	// MaxResults caps the number of queries returned (0 = all).
	MaxResults int
}

// DescribeQueries lists recent Logs Insights queries of the account.
func (c *Client) DescribeQueries(ctx context.Context, opts DescribeQueriesOptions) ([]QueryInfo, error) {
	const op = "describeQueries"
	if opts.MaxResults < 0 {
		return nil, ValidationError(op, "maxResults must not be negative")
	}
	in := &cwl.DescribeQueriesInput{}
	if opts.Group != "" {
		in.LogGroupName = &opts.Group
	}
	if opts.Status != "" {
		in.Status = types.QueryStatus(opts.Status)
	}
	var out []QueryInfo
	var err error
	paginate(ctx, c, op, func(q QueryInfo, e error) bool {
		if e != nil {
			err = e
			return false
		}
		out = append(out, q)
		return opts.MaxResults <= 0 || len(out) < opts.MaxResults
	}, func(ctx context.Context, token *string) ([]QueryInfo, *string, error) {
		in.NextToken = token
		res, err := c.api.DescribeQueries(ctx, in, tolerantMillis)
		if err != nil {
			return nil, nil, err
		}
		qs := make([]QueryInfo, len(res.Queries))
		for i, q := range res.Queries {
			qs[i] = QueryInfo{
				QueryID:      deref(q.QueryId),
				QueryString:  deref(q.QueryString),
				Status:       QueryStatus(q.Status),
				CreateTime:   msTime(q.CreateTime),
				LogGroupName: deref(q.LogGroupName),
			}
		}
		return qs, res.NextToken, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ParseQueryTimestamp parses an Insights @timestamp value: the AWS form
// "2006-01-02 15:04:05.000" (UTC) or epoch milliseconds (Moto).
func ParseQueryTimestamp(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ms).UTC(), nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05.000", "2006-01-02 15:04:05", time.RFC3339Nano} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, ValidationError("parseQueryTimestamp", "unrecognised timestamp format")
}
