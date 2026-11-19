package cloudwatchlogs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Attribute keys added to records read back from CloudWatch Logs.
const (
	AttrLogStream = "aws.log.stream"
	AttrEventID   = "aws.event.id"
	AttrLogGroup  = "aws.log.group"
)

// Exporter is the LogExporter of the client: it writes normalized LogRecord
// JSON documents with PutLogEvents to one log group and stream. Gzip is
// ignored (PutLogEvents has no request compression).
type Exporter struct {
	client      *Client
	group       string
	stream      string
	createGroup bool
}

var _ LogExporter = (*Exporter)(nil)

// ExporterOptions select where an Exporter or LogClient writes.
type ExporterOptions struct {
	// Group is the log group (default: the client's LogGroup; required).
	Group string
	// Stream is the log stream (default: the client's LogStream). A missing
	// stream is created on first use unless DisableAutoCreate is set.
	Stream string
	// CreateGroup also creates a missing log group on the first export.
	CreateGroup bool
}

// Exporter returns a LogExporter writing to opts.Group / opts.Stream (CONFIG
// error when no group is set, VALIDATION for invalid names).
func (c *Client) Exporter(opts ExporterOptions) (*Exporter, error) {
	const op = "exporter"
	group, err := c.groupOrDefault(op, opts.Group)
	if err != nil {
		return nil, err
	}
	stream := opts.Stream
	if stream == "" {
		stream = c.cfg.LogStream
	}
	if err := validateStreamName(op, stream); err != nil {
		return nil, err
	}
	return &Exporter{client: c, group: group, stream: stream, createGroup: opts.CreateGroup}, nil
}

// groupOrDefault returns group or the configured LogGroup, validated.
func (c *Client) groupOrDefault(op, group string) (string, error) {
	if group == "" {
		group = c.cfg.LogGroup
	}
	if group == "" {
		return "", ConfigError(op + ": no log group given and logGroup is not set (GROW_AWS_CLOUDWATCH_LOGS_LOG_GROUP or CLOUDWATCH_LOG_GROUP)")
	}
	if err := validateGroupName(op, group); err != nil {
		return "", err
	}
	return group, nil
}

// Export implements LogExporter. Each record becomes one event whose message is
// the record's normalized JSON and whose timestamp is the record timestamp
// (milliseconds). Oversize records follow the client's size policy; truncation
// shortens the body so the message stays valid JSON.
func (e *Exporter) Export(ctx context.Context, records []LogRecord, _ ExportOptions) (ExportResult, error) {
	cfg := e.client.cfg
	maxBytes, policy := cfg.maxEventBytes(), cfg.oversizePolicy()
	events := make([]LogEvent, 0, len(records))
	skipped := 0
	for _, r := range records {
		msg, ok := encodeRecord(r, maxBytes, policy)
		if !ok {
			skipped++
			continue
		}
		ts := r.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		events = append(events, LogEvent{Timestamp: ts, Message: msg})
	}
	res, err := e.client.PutLogEvents(ctx, e.group, e.stream, events, PutLogEventsOptions{OversizePolicy: OversizeTruncate, CreateGroup: e.createGroup})
	out := ExportResult{Accepted: res.Accepted, Rejected: res.Rejected + skipped}
	if err != nil {
		if res.Accepted == 0 {
			return out, err
		}
		_ = errors.As(err, &out.Error)
		return out, nil
	}
	if out.Rejected > 0 {
		out.Error = &Error{Code: ErrorCodeValidation, Message: fmt.Sprintf("export: %d of %d records were rejected (oversize, too old or too new)", out.Rejected, len(records))}
	}
	return out, nil
}

// Shutdown implements LogExporter; there is nothing to release.
func (e *Exporter) Shutdown(context.Context) error { return nil }

// encodeRecord returns the JSON message of r within maxBytes, or false when the
// record must be skipped (OversizeDrop / OversizeError).
func encodeRecord(r LogRecord, maxBytes int, policy OversizePolicy) (string, bool) {
	raw, _ := json.Marshal(r) // LogRecord always encodes
	if len(raw) <= maxBytes {
		return string(raw), true
	}
	if policy != OversizeTruncate {
		return "", false
	}
	// Binary search for the longest body prefix whose record fits.
	orig := r.Body
	best := ""
	lo, hi := 0, len(orig)
	for lo <= hi {
		mid := (lo + hi) / 2
		r.Body = TruncateMessage(orig, mid)
		raw, _ = json.Marshal(r)
		if len(raw) <= maxBytes {
			best, lo = string(raw), mid+1
		} else {
			hi = mid - 1
		}
	}
	if best != "" {
		return best, true
	}
	return TruncateMessage(string(raw), maxBytes), true
}

// LogClientConfig configures Client.LogClient: where to write plus the
// batching, resource and redaction options of the generated LogClient.
type LogClientConfig struct {
	ExporterOptions
	LogClientOptions
}

// LogClient returns a batching LogClient (redaction, Logger facade) that
// exports to cfg.Group / cfg.Stream (defaults: the client's LogGroup and
// LogStream). The client flushes and closes it on Close.
func (c *Client) LogClient(cfg LogClientConfig) (*BatchingLogClient, error) {
	if c.closed.Load() {
		return nil, &Error{Code: ErrorCodeCancelled, Message: "logClient: client is closed"}
	}
	exp, err := c.Exporter(cfg.ExporterOptions)
	if err != nil {
		return nil, err
	}
	lc, err := NewBatchingLogClient(exp, cfg.LogClientOptions)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.logClients = append(c.logClients, lc)
	c.mu.Unlock()
	return lc, nil
}

// QueryClient is the LogQueryClient over one log group (see Client.LogQueryClient).
type QueryClient struct {
	client *Client
	group  string
}

var _ LogQueryClient = (*QueryClient)(nil)

// LogQueryClient returns the LogQueryClient over group (default: the client's
// LogGroup). Search uses FilterLogEvents, Tail live tail, ListStreams
// DescribeLogStreams; ListLabels is UNSUPPORTED.
func (c *Client) LogQueryClient(group string) (*QueryClient, error) {
	g, err := c.groupOrDefault("logQueryClient", group)
	if err != nil {
		return nil, err
	}
	return &QueryClient{client: c, group: g}, nil
}

// Group returns the log group searched.
func (q *QueryClient) Group() string { return q.group }

// FilterPattern turns a search query into a CloudWatch Logs filter pattern:
// empty stays empty (every event), a query that starts with '{', '?', '[', '-'
// or '"' is passed through as a native pattern, anything else becomes an exact
// quoted phrase.
func FilterPattern(query string) string {
	q := strings.TrimSpace(query)
	if q == "" {
		return ""
	}
	switch q[0] {
	case '{', '?', '[', '-', '"':
		return q
	}
	return `"` + strings.ReplaceAll(strings.ReplaceAll(q, `\`, `\\`), `"`, `\"`) + `"`
}

// Search runs FilterLogEvents on the client's LogGroup with FilterPattern(query).
// OrderAsc pages natively (the cursor is the service token). OrderDesc (the
// default) walks the time window backwards in growing slices, newest slice
// first, and sorts each slice newest first; its cursor is opaque. Messages that
// are normalized LogRecord JSON are decoded, other messages become INFO records
// with the message as body; every record gets the attributes aws.log.stream and
// aws.event.id.
func (q *QueryClient) Search(ctx context.Context, query string, opts SearchOptions) (LogSearchResult, error) {
	c := q.client
	const op = "logs.search"
	opts = opts.WithDefaults()
	if !opts.Start.IsZero() && !opts.End.IsZero() && opts.End.Before(opts.Start) {
		return LogSearchResult{}, ValidationError(op, "end must not be before start")
	}
	switch opts.Order {
	case OrderAsc:
		return q.searchAsc(ctx, c, op, query, opts)
	case OrderDesc:
		return q.searchDesc(ctx, c, op, query, opts)
	}
	return LogSearchResult{}, ValidationError(op, "order must be asc or desc")
}

func (q *QueryClient) searchAsc(ctx context.Context, c *Client, op, query string, opts SearchOptions) (LogSearchResult, error) {
	in, _, err := c.filterInput(FilterLogEventsOptions{Group: q.group, Pattern: FilterPattern(query), Start: opts.Start, End: opts.End})
	if err != nil {
		return LogSearchResult{}, err
	}
	var events []FilteredLogEvent
	token := opts.Cursor
	for len(events) < opts.Limit {
		pctx, cancel, err := c.begin(ctx, op)
		if err != nil {
			return LogSearchResult{}, err
		}
		in.NextToken = nil
		if token != "" {
			in.NextToken = &token
		}
		in.Limit = ptr(small32(min(opts.Limit-len(events), MaxEventsPageSize)))
		out, err := c.api.FilterLogEvents(pctx, in)
		cancel()
		if err != nil {
			return LogSearchResult{}, c.mapError(op, err)
		}
		page := toFiltered(out.Events)
		sortEvents(page, false)
		events = append(events, page...)
		next := deref(out.NextToken)
		if next == "" || next == token {
			token = ""
			break
		}
		token = next
	}
	return LogSearchResult{Records: toRecords(events), NextCursor: token}, nil
}

// descCursor is the decoded OrderDesc cursor: continue with events at or
// before End (ms), skipping the first Skip events stamped exactly End.
type descCursor struct {
	End  int64 `json:"e"`
	Skip int   `json:"s"`
}

const descCursorPrefix = "d1."

func encodeDescCursor(c descCursor) string {
	raw, _ := json.Marshal(c)
	return descCursorPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

func decodeDescCursor(s string) (descCursor, bool) {
	var c descCursor
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(s, descCursorPrefix))
	if !strings.HasPrefix(s, descCursorPrefix) || err != nil || json.Unmarshal(raw, &c) != nil || c.Skip < 0 {
		return descCursor{}, false
	}
	return c, true
}

// firstDescSlice is the width of the newest slice of a descending search; each
// further slice is four times wider.
const firstDescSlice = 5 * time.Minute

// maxDescSlice caps the width of one slice of a descending search.
const maxDescSlice = 30 * 24 * time.Hour

// searchScanCap is the most events read for one slice before it is narrowed.
const searchScanCap = 10_000

// defaultDescWindow bounds a descending search without Start when the group's
// creation time is unknown.
const defaultDescWindow = 30 * 24 * time.Hour

func (q *QueryClient) searchDesc(ctx context.Context, c *Client, op, query string, opts SearchOptions) (LogSearchResult, error) {
	end := opts.End
	if end.IsZero() {
		end = time.Now()
	}
	upper, skip := end.UnixMilli(), 0
	if opts.Cursor != "" {
		cur, ok := decodeDescCursor(opts.Cursor)
		if !ok {
			return LogSearchResult{}, ValidationError(op, "invalid cursor")
		}
		upper, skip = cur.End, cur.Skip
	}
	start := opts.Start
	if start.IsZero() {
		start = end.Add(-defaultDescWindow)
		if g, err := c.findGroupCtx(ctx, op, q.group); err == nil && !g.CreationTime.IsZero() {
			start = g.CreationTime.Add(-time.Hour) // events may be stamped before creation
		} else if err != nil && CodeOf(err) != ErrorCodeNotFound {
			return LogSearchResult{}, err
		}
	}
	lower := start.UnixMilli()
	pattern := FilterPattern(query)
	var collected []FilteredLogEvent
	width := firstDescSlice.Milliseconds()
	hi := upper
	for len(collected) <= opts.Limit && hi >= lower {
		lo := max(lower, hi-width+1)
		slice, err := q.readSlice(ctx, c, pattern, lo, hi)
		if err != nil {
			return LogSearchResult{}, err
		}
		// A slice that holds more than the scan cap is narrowed to its newest half.
		for len(slice) > searchScanCap && hi > lo {
			lo = hi - (hi-lo)/2
			width = hi - lo + 1
			if slice, err = q.readSlice(ctx, c, pattern, lo, hi); err != nil {
				return LogSearchResult{}, err
			}
		}
		sortEvents(slice, true)
		if hi == upper && skip > 0 {
			n := 0
			for n < len(slice) && n < skip && slice[n].Timestamp.UnixMilli() == upper {
				n++
			}
			slice = slice[n:]
		}
		collected = append(collected, slice...)
		hi = lo - 1
		width = min(width*4, maxDescSlice.Milliseconds())
	}
	more := len(collected) > opts.Limit || (len(collected) == opts.Limit && hi >= lower)
	if len(collected) > opts.Limit {
		collected = collected[:opts.Limit]
	}
	res := LogSearchResult{Records: toRecords(collected)}
	if more && len(collected) > 0 {
		last := collected[len(collected)-1].Timestamp.UnixMilli()
		n := 0
		for _, e := range collected {
			if e.Timestamp.UnixMilli() == last {
				n++
			}
		}
		if last == upper {
			n += skip
		}
		res.NextCursor = encodeDescCursor(descCursor{End: last, Skip: n})
	}
	return res, nil
}

// readSlice reads every matching event stamped in [lo, hi] (milliseconds).
func (q *QueryClient) readSlice(ctx context.Context, c *Client, pattern string, lo, hi int64) ([]FilteredLogEvent, error) {
	var slice []FilteredLogEvent
	for e, err := range c.FilterLogEvents(ctx, FilterLogEventsOptions{Group: q.group, Pattern: pattern, Start: time.UnixMilli(lo), End: time.UnixMilli(hi)}) {
		if err != nil {
			return nil, err
		}
		slice = append(slice, e)
	}
	return slice, nil
}

func (c *Client) findGroupCtx(ctx context.Context, op, group string) (LogGroup, error) {
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return LogGroup{}, err
	}
	defer cancel()
	return c.findGroup(ctx, op, group)
}

// sortEvents orders by (timestamp, eventId), newest first when desc.
func sortEvents(events []FilteredLogEvent, desc bool) {
	slices.SortStableFunc(events, func(a, b FilteredLogEvent) int {
		r := a.Timestamp.Compare(b.Timestamp)
		if r == 0 {
			r = compareEventIDs(a.EventID, b.EventID)
		}
		if desc {
			return -r
		}
		return r
	})
}

// compareEventIDs compares numeric ids numerically and others as strings.
func compareEventIDs(a, b string) int {
	if len(a) != len(b) {
		if _, errA := strconv.ParseUint(a, 10, 64); errA == nil {
			if _, errB := strconv.ParseUint(b, 10, 64); errB == nil {
				return len(a) - len(b)
			}
		}
	}
	return strings.Compare(a, b)
}

func toRecords(events []FilteredLogEvent) []LogRecord {
	out := make([]LogRecord, len(events))
	for i, e := range events {
		out[i] = FilteredEventToRecord(e)
	}
	return out
}

// FilteredEventToRecord converts a FilterLogEvents event to a LogRecord (see
// MessageToRecord) with the attributes aws.log.stream and aws.event.id.
func FilteredEventToRecord(e FilteredLogEvent) LogRecord {
	r := MessageToRecord(e.Message, e.Timestamp, e.IngestionTime)
	if e.LogStreamName != "" {
		r.Attributes[AttrLogStream] = e.LogStreamName
	}
	if e.EventID != "" {
		r.Attributes[AttrEventID] = e.EventID
	}
	return r
}

// LiveTailEventToRecord converts a live-tail event to a LogRecord (see
// MessageToRecord) with the attributes aws.log.stream and, for plain
// messages, aws.log.group.
func LiveTailEventToRecord(e LiveTailEvent) LogRecord {
	r, plain := decodeMessage(e.Message, e.Timestamp, e.IngestionTime)
	if e.LogStreamName != "" {
		r.Attributes[AttrLogStream] = e.LogStreamName
	}
	if plain && e.LogGroupIdentifier != "" {
		r.Attributes[AttrLogGroup] = e.LogGroupIdentifier
	}
	return r
}

// MessageToRecord decodes a CloudWatch Logs message: a normalized LogRecord
// JSON document (with "timestamp" and "body") is decoded as such; any other
// message becomes an INFO record with the message as body, ts as timestamp
// and ingested as observed timestamp. Attributes and Resource are never nil.
func MessageToRecord(message string, ts, ingested time.Time) LogRecord {
	r, _ := decodeMessage(message, ts, ingested)
	return r
}

// decodeMessage is MessageToRecord that also reports whether the message was plain text.
func decodeMessage(message string, ts, ingested time.Time) (LogRecord, bool) {
	trimmed := strings.TrimSpace(message)
	if strings.HasPrefix(trimmed, "{") {
		var probe map[string]json.RawMessage
		if json.Unmarshal([]byte(trimmed), &probe) == nil && probe["timestamp"] != nil && probe["body"] != nil {
			var rec LogRecord
			if err := json.NewDecoder(bytes.NewReader([]byte(trimmed))).Decode(&rec); err == nil {
				if rec.Timestamp.IsZero() {
					rec.Timestamp = ts
				}
				return rec, false
			}
		}
	}
	return LogRecord{
		Timestamp:         ts,
		ObservedTimestamp: ingested,
		Severity:          SeverityInfo,
		SeverityText:      SeverityInfo.String(),
		Body:              message,
		Attributes:        map[string]AttrValue{},
		Resource:          map[string]AttrValue{},
	}, true
}

// Tail starts a live-tail session on the group with FilterPattern(query) and
// calls onRecord for every event (see LiveTailEventToRecord). Stop the
// returned handle to end it. TailOptions are not used: the service pushes
// events as they are ingested.
func (q *QueryClient) Tail(ctx context.Context, query string, onRecord func(LogRecord), _ TailOptions) (TailHandle, error) {
	if onRecord == nil {
		return nil, ValidationError("logs.tail", "onRecord is required")
	}
	return q.client.StartLiveTail(ctx, LiveTailOptions{Groups: []string{q.group}, Pattern: FilterPattern(query)}, func(e LiveTailEvent) {
		onRecord(LiveTailEventToRecord(e))
	})
}

// ListStreams returns the names of all streams of the group.
func (q *QueryClient) ListStreams(ctx context.Context) ([]string, error) {
	names := []string{}
	for s, err := range q.client.DescribeLogStreams(ctx, q.group, DescribeLogStreamsOptions{}) {
		if err != nil {
			return nil, err
		}
		names = append(names, s.Name)
	}
	return names, nil
}

// ListLabels returns UNSUPPORTED: CloudWatch Logs has no label inventory.
func (q *QueryClient) ListLabels(context.Context) ([]string, error) {
	return nil, UnsupportedError("logs.listLabels", "Amazon CloudWatch Logs has no label inventory")
}

// WaitForLog polls Search until a record matches (see the package-level WaitForLog).
func (q *QueryClient) WaitForLog(ctx context.Context, query string, opts WaitForLogOptions) (LogRecord, error) {
	return WaitForLog(ctx, q, query, opts)
}
