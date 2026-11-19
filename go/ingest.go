package cloudwatchlogs

import (
	"context"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// PutLogEvents service limits, enforced client side (AWS rejects larger calls).
const (
	// MaxBatchEvents is the maximum number of events in one PutLogEvents call.
	MaxBatchEvents = 10_000
	// MaxBatchBytes is the maximum size of one call: Σ(UTF-8 message bytes + 26).
	MaxBatchBytes = 1_048_576
	// EventOverheadBytes is the per-event overhead counted against MaxBatchBytes.
	EventOverheadBytes = 26
	// MaxBatchSpan is the largest timestamp span inside one call.
	MaxBatchSpan = 24 * time.Hour
)

// LogEvent is one event for PutLogEvents.
type LogEvent struct {
	// Timestamp is when the event happened (zero = now). CloudWatch Logs keeps
	// millisecond precision.
	Timestamp time.Time
	// Message is the raw event text.
	Message string
}

// PutLogEventsOptions configure PutLogEvents.
type PutLogEventsOptions struct {
	// AutoCreate creates a missing stream and retries once (nil = the client's
	// setting, which defaults to true).
	AutoCreate *bool
	// CreateGroup also creates a missing log group when auto-creating.
	CreateGroup bool
	// MaxEventBytes and OversizePolicy override the client's size policy.
	MaxEventBytes  int
	OversizePolicy OversizePolicy
}

// RejectedInfo is the rejectedLogEventsInfo of one PutLogEvents call. The
// indexes refer to the timestamp-sorted events of that batch and are -1 when
// absent. As returned by AWS, TooOldLogEventEndIndex and
// ExpiredLogEventEndIndex are exclusive (events [0, index) were rejected) and
// TooNewLogEventStartIndex is inclusive (events [index, n) were rejected).
// Moto 5.2.4 returns end indexes one lower (see docs/compatibility.md).
type RejectedInfo struct {
	// Batch is the 0-based index of the PutLogEvents call within PutLogEvents.
	Batch                    int
	TooOldLogEventEndIndex   int
	TooNewLogEventStartIndex int
	ExpiredLogEventEndIndex  int
}

// PutLogEventsResult reports what PutLogEvents did.
type PutLogEventsResult struct {
	// Accepted is the number of events CloudWatch Logs accepted.
	Accepted int
	// Rejected is the number of events rejected as too old, too new or expired
	// (they are never retried).
	Rejected int
	// Dropped is the number of oversize events dropped by OversizeDrop.
	Dropped int
	// Truncated is the number of oversize events truncated by OversizeTruncate.
	Truncated int
	// Batches is the number of PutLogEvents calls made.
	Batches int
	// RejectedInfo holds the rejection details of each batch that had rejections.
	RejectedInfo []RejectedInfo
}

// PutLogEvents sends events to a stream. The events are stably sorted by
// timestamp, oversize events follow the size policy, and the events are split
// into calls that respect the service limits (10,000 events, 1 MiB including
// 26 bytes per event, 24 h span). A missing stream is created once when
// auto-create is on. Throttling and 5xx are retried by the SDK; a response lost
// after sending may duplicate events. On error the result covers the batches
// sent so far.
func (c *Client) PutLogEvents(ctx context.Context, group, stream string, events []LogEvent, opts PutLogEventsOptions) (PutLogEventsResult, error) {
	op := "putLogEvents " + group + "/" + stream
	if err := validateGroupName(op, group); err != nil {
		return PutLogEventsResult{}, err
	}
	if err := validateStreamName(op, stream); err != nil {
		return PutLogEventsResult{}, err
	}
	maxBytes, policy := c.cfg.maxEventBytes(), c.cfg.oversizePolicy()
	if opts.MaxEventBytes != 0 {
		maxBytes = opts.MaxEventBytes
	}
	if opts.OversizePolicy != "" {
		policy = opts.OversizePolicy
	}
	if maxBytes < 1 || maxBytes > MaxEventBytesLimit {
		return PutLogEventsResult{}, ValidationError(op, fmt.Sprintf("maxEventBytes must be between 1 and %d", MaxEventBytesLimit))
	}
	plan, err := planBatches(op, events, maxBytes, policy, time.Now())
	if err != nil {
		return PutLogEventsResult{}, err
	}
	res := PutLogEventsResult{Dropped: plan.dropped, Truncated: plan.truncated}
	autoCreate := !c.cfg.DisableAutoCreate
	if opts.AutoCreate != nil {
		autoCreate = *opts.AutoCreate
	}
	for _, batch := range plan.batches {
		out, err := c.putBatch(ctx, op, group, stream, batch)
		if err != nil && autoCreate && CodeOf(err) == ErrorCodeNotFound {
			if _, cerr := c.EnsureLogStream(ctx, group, stream, EnsureLogStreamOptions{CreateGroup: opts.CreateGroup}); cerr != nil {
				return res, cerr
			}
			out, err = c.putBatch(ctx, op, group, stream, batch)
		}
		if err != nil {
			return res, err
		}
		res.Batches++
		rejected := 0
		if info := out.RejectedLogEventsInfo; info != nil {
			ri := RejectedInfo{
				Batch:                    res.Batches - 1,
				TooOldLogEventEndIndex:   indexOr(info.TooOldLogEventEndIndex),
				TooNewLogEventStartIndex: indexOr(info.TooNewLogEventStartIndex),
				ExpiredLogEventEndIndex:  indexOr(info.ExpiredLogEventEndIndex),
			}
			rejected = ri.count(len(batch))
			res.RejectedInfo = append(res.RejectedInfo, ri)
		}
		res.Rejected += rejected
		res.Accepted += len(batch) - rejected
	}
	return res, nil
}

func indexOr(p *int32) int {
	if p == nil {
		return -1
	}
	return int(*p)
}

// count returns how many of n events the info rejects.
func (r RejectedInfo) count(n int) int {
	head := max(r.TooOldLogEventEndIndex, r.ExpiredLogEventEndIndex, 0) // events [0, head) rejected
	tail := n
	if r.TooNewLogEventStartIndex >= 0 {
		tail = r.TooNewLogEventStartIndex // events [tail, n) rejected
	}
	head = min(head, n)
	tail = min(max(tail, head), n)
	return head + (n - tail)
}

func (c *Client) putBatch(ctx context.Context, op, group, stream string, batch []types.InputLogEvent) (*cwl.PutLogEventsOutput, error) {
	ctx, cancel, err := c.begin(ctx, op)
	if err != nil {
		return nil, err
	}
	defer cancel()
	out, err := c.api.PutLogEvents(ctx, &cwl.PutLogEventsInput{LogGroupName: &group, LogStreamName: &stream, LogEvents: batch})
	if err != nil {
		return nil, c.mapError(op, err)
	}
	return out, nil
}

// batchPlan is the result of planBatches.
type batchPlan struct {
	batches   [][]types.InputLogEvent
	dropped   int
	truncated int
}

// planBatches sorts the events, applies the size policy and splits them.
func planBatches(op string, events []LogEvent, maxBytes int, policy OversizePolicy, now time.Time) (batchPlan, error) {
	type item struct {
		ts  int64
		msg string
	}
	items := make([]item, 0, len(events))
	var plan batchPlan
	for _, e := range events {
		ts := now.UnixMilli()
		if !e.Timestamp.IsZero() {
			ts = e.Timestamp.UnixMilli()
		}
		msg := e.Message
		if len(msg) > maxBytes {
			switch policy {
			case OversizeDrop:
				plan.dropped++
				continue
			case OversizeError:
				return batchPlan{}, ValidationError(op, fmt.Sprintf("event of %d bytes exceeds maxEventBytes %d", len(msg), maxBytes))
			default:
				msg = TruncateMessage(msg, maxBytes)
				plan.truncated++
			}
		}
		items = append(items, item{ts: ts, msg: msg})
	}
	slices.SortStableFunc(items, func(a, b item) int {
		switch {
		case a.ts < b.ts:
			return -1
		case a.ts > b.ts:
			return 1
		}
		return 0
	})
	var cur []types.InputLogEvent
	curBytes := 0
	var first int64
	for _, it := range items {
		size := len(it.msg) + EventOverheadBytes
		if len(cur) > 0 && (len(cur) >= MaxBatchEvents || curBytes+size > MaxBatchBytes || it.ts-first > MaxBatchSpan.Milliseconds()) {
			plan.batches = append(plan.batches, cur)
			cur, curBytes = nil, 0
		}
		if len(cur) == 0 {
			first = it.ts
		}
		cur = append(cur, types.InputLogEvent{Timestamp: ptr(it.ts), Message: ptr(it.msg)})
		curBytes += size
	}
	if len(cur) > 0 {
		plan.batches = append(plan.batches, cur)
	}
	return plan, nil
}

// TruncateMessage shortens msg to at most maxBytes UTF-8 bytes, cutting on a
// rune boundary and appending " …[truncated N bytes]" (N = bytes removed).
// Messages that fit are returned unchanged.
func TruncateMessage(msg string, maxBytes int) string {
	if len(msg) <= maxBytes {
		return msg
	}
	// The marker with N = len(msg) is at least as long as the final marker.
	keep := maxBytes - len(truncationMarker(len(msg)))
	if keep < 0 { // maxBytes is smaller than the marker: cut without it
		return msg[:runeBoundary(msg, maxBytes)]
	}
	cut := runeBoundary(msg, keep)
	return msg[:cut] + truncationMarker(len(msg)-cut)
}

func truncationMarker(n int) string { return fmt.Sprintf(" …[truncated %d bytes]", n) }

// runeBoundary returns the largest index <= n that starts a rune of s.
func runeBoundary(s string, n int) int {
	if n >= len(s) {
		return len(s)
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return n
}
