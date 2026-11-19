package cloudwatchlogs

import (
	"context"
	"strings"
	"sync"
	"time"

	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// MaxLiveTailGroups is the most log groups one live-tail session can follow.
const MaxLiveTailGroups = 10

// LiveTailOptions configure StartLiveTail.
type LiveTailOptions struct {
	// Groups are log group names or ARNs (default: the client's LogGroup; at
	// most 10). Names are turned into ARNs with the client's region and account.
	Groups []string
	// Streams or StreamPrefixes (mutually exclusive, only with one group) limit
	// the session to some streams.
	Streams        []string
	StreamPrefixes []string
	// Pattern is a CloudWatch Logs filter pattern (empty = every event).
	Pattern string
	// OnError is called with the error that ends the session (also in Err).
	OnError func(error)
	// OnUpdate is called for every sessionUpdate frame, including empty
	// keep-alive updates.
	OnUpdate func(LiveTailUpdate)
}

// LiveTailUpdate describes one sessionUpdate frame.
type LiveTailUpdate struct {
	// Sampled is true when the service sampled the results.
	Sampled bool
	// Count is the number of events in the frame.
	Count int
}

// LiveTailEvent is one event delivered by a live-tail session.
type LiveTailEvent struct {
	Message            string
	Timestamp          time.Time
	IngestionTime      time.Time
	LogStreamName      string
	LogGroupIdentifier string
	// Sampled is true when the service sampled the update (more than ~500 events/s).
	Sampled bool
}

// LiveTailSession is a running StartLiveTail session. AWS streams events
// ingested after the session started, about once per second, for up to 3 h.
type LiveTailSession struct {
	sessionID string
	stream    *cwl.StartLiveTailEventStream
	cancel    context.CancelFunc
	client    *Client
	done      chan struct{}
	closeOnce sync.Once

	mu  sync.Mutex
	err error
}

// SessionID is the id from the service's sessionStart frame.
func (s *LiveTailSession) SessionID() string { return s.sessionID }

// Done is closed when the session has ended (service closed it, error or Close).
func (s *LiveTailSession) Done() <-chan struct{} { return s.done }

// Err returns the error that ended the session, or nil after a normal end or
// Close. SessionStreamingException gives UNAVAILABLE and SessionTimeoutException
// TIMEOUT (both retryable: start a new session).
func (s *LiveTailSession) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops the session. It is idempotent and safe to call from onEvent;
// wait on Done to know when the last event was delivered.
func (s *LiveTailSession) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.stream.Close()
	})
	return nil
}

// Stop implements TailHandle (same as Close).
func (s *LiveTailSession) Stop() { _ = s.Close() }

// StartLiveTail starts a live-tail session and calls onEvent (from one
// goroutine, in order) for every event until the session ends, Close is
// called or ctx is done. It returns after the service confirmed the session.
func (c *Client) StartLiveTail(ctx context.Context, opts LiveTailOptions, onEvent func(LiveTailEvent)) (*LiveTailSession, error) {
	const op = "startLiveTail"
	if onEvent == nil {
		return nil, ValidationError(op, "onEvent is required")
	}
	groups := opts.Groups
	if len(groups) == 0 && c.cfg.LogGroup != "" {
		groups = []string{c.cfg.LogGroup}
	}
	switch {
	case len(groups) == 0:
		return nil, ConfigError("startLiveTail: no log groups given and logGroup is not set")
	case len(groups) > MaxLiveTailGroups:
		return nil, ValidationError(op, "at most 10 log groups")
	case len(opts.Streams) > 0 && len(opts.StreamPrefixes) > 0:
		return nil, ValidationError(op, "streams and streamPrefixes are mutually exclusive")
	case (len(opts.Streams) > 0 || len(opts.StreamPrefixes) > 0) && len(groups) != 1:
		return nil, ValidationError(op, "streams and streamPrefixes need exactly one log group")
	}
	if c.closed.Load() {
		return nil, &Error{Code: ErrorCodeCancelled, Message: op + ": client is closed"}
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, c.cfg.TotalTimeout)
	defer setupCancel()
	arns := make([]string, len(groups))
	for i, g := range groups {
		arn, err := c.groupARN(setupCtx, op, g)
		if err != nil {
			return nil, err
		}
		arns[i] = arn
	}
	in := &cwl.StartLiveTailInput{LogGroupIdentifiers: arns}
	if len(opts.Streams) > 0 {
		in.LogStreamNames = opts.Streams
	}
	if len(opts.StreamPrefixes) > 0 {
		in.LogStreamNamePrefixes = opts.StreamPrefixes
	}
	if opts.Pattern != "" {
		in.LogEventFilterPattern = &opts.Pattern
	}
	sctx, cancel := context.WithCancel(ctx)
	// Until the session is confirmed, the setup deadline also ends the stream.
	stopSetupTimer := context.AfterFunc(setupCtx, cancel)
	fail := func(err error) (*LiveTailSession, error) {
		cancel()
		if setupCtx.Err() != nil {
			return nil, contextErr(op, setupCtx.Err())
		}
		return nil, err
	}
	out, err := c.api.StartLiveTail(sctx, in)
	if err != nil {
		return fail(c.mapError(op, err))
	}
	s := &LiveTailSession{stream: out.GetStream(), cancel: cancel, client: c, done: make(chan struct{})}
	events := s.stream.Events()
	// Wait for sessionStart (the first frame) before returning.
	var pending []LiveTailEvent
	ev, ok := <-events
	if !ok || !stopSetupTimer() {
		err := s.stream.Err()
		_ = s.stream.Close()
		if err != nil {
			return fail(c.mapError(op, err))
		}
		return fail(&Error{Code: ErrorCodeUnavailable, Message: op + ": the stream ended before the session started", Retryable: true})
	}
	switch v := ev.(type) {
	case *types.StartLiveTailResponseStreamMemberSessionStart:
		s.sessionID = deref(v.Value.SessionId)
	case *types.StartLiveTailResponseStreamMemberSessionUpdate:
		pending = toLiveTailEvents(v.Value)
	}
	c.mu.Lock()
	c.sessions[s] = struct{}{}
	c.mu.Unlock()
	go s.run(op, events, pending, onEvent, opts)
	return s, nil
}

func (s *LiveTailSession) run(op string, events <-chan types.StartLiveTailResponseStream, pending []LiveTailEvent, onEvent func(LiveTailEvent), opts LiveTailOptions) {
	defer func() {
		s.client.mu.Lock()
		delete(s.client.sessions, s)
		s.client.mu.Unlock()
		s.cancel()
		_ = s.stream.Close()
		close(s.done)
	}()
	deliver := func(evs []LiveTailEvent) {
		for _, e := range evs {
			safeCall(func() { onEvent(e) })
		}
	}
	update := func(evs []LiveTailEvent, sampled bool) {
		if opts.OnUpdate != nil {
			safeCall(func() { opts.OnUpdate(LiveTailUpdate{Sampled: sampled, Count: len(evs)}) })
		}
		deliver(evs)
	}
	if pending != nil {
		update(pending, len(pending) > 0 && pending[0].Sampled)
	}
	for ev := range events {
		if u, ok := ev.(*types.StartLiveTailResponseStreamMemberSessionUpdate); ok {
			update(toLiveTailEvents(u.Value), u.Value.SessionMetadata != nil && u.Value.SessionMetadata.Sampled)
		}
	}
	if err := s.stream.Err(); err != nil {
		mapped := s.client.mapError(op+" "+s.sessionID, err)
		if CodeOf(mapped) != ErrorCodeCancelled {
			s.mu.Lock()
			s.err = mapped
			s.mu.Unlock()
			if opts.OnError != nil {
				safeCall(func() { opts.OnError(mapped) })
			}
		}
	}
}

func toLiveTailEvents(u types.LiveTailSessionUpdate) []LiveTailEvent {
	sampled := u.SessionMetadata != nil && u.SessionMetadata.Sampled
	out := make([]LiveTailEvent, len(u.SessionResults))
	for i, r := range u.SessionResults {
		out[i] = LiveTailEvent{
			Message:            deref(r.Message),
			Timestamp:          msTime(r.Timestamp),
			IngestionTime:      msTime(r.IngestionTime),
			LogStreamName:      deref(r.LogStreamName),
			LogGroupIdentifier: deref(r.LogGroupIdentifier),
			Sampled:            sampled,
		}
	}
	return out
}

// LogGroupARN returns the ARN of a log group (without ":*"). For a name it is
// built from the region and Config.AccountID when the account is configured,
// otherwise the group is looked up with DescribeLogGroups (NOT_FOUND when it
// does not exist); an ARN is returned without ":*".
func (c *Client) LogGroupARN(ctx context.Context, group string) (string, error) {
	ctx, cancel, err := c.begin(ctx, "logGroupArn")
	if err != nil {
		return "", err
	}
	defer cancel()
	return c.groupARN(ctx, "logGroupArn", group)
}

// groupARN returns the ARN of a group name or ARN (see LogGroupARN).
func (c *Client) groupARN(ctx context.Context, op, group string) (string, error) {
	if strings.HasPrefix(group, "arn:") {
		return trimStarARN(group), nil
	}
	if err := validateGroupName(op, group); err != nil {
		return "", err
	}
	if c.cfg.AccountID == "" {
		g, err := c.findGroup(ctx, op, group)
		if err != nil {
			return "", err
		}
		return g.ARN, nil
	}
	if c.region == "" {
		return "", ConfigError("region is required to build log group ARNs")
	}
	return "arn:" + partitionOf(c.region) + ":logs:" + c.region + ":" + c.cfg.AccountID + ":log-group:" + group, nil
}

// partitionOf returns the AWS partition of a region.
func partitionOf(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	case strings.HasPrefix(region, "us-iso-"):
		return "aws-iso"
	case strings.HasPrefix(region, "us-isob-"):
		return "aws-iso-b"
	case strings.HasPrefix(region, "eusc-"):
		return "aws-eusc"
	}
	return "aws"
}
