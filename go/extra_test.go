package cloudwatchlogs

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGetLogGroupAndARN(t *testing.T) {
	m := newAWSMock(t).on("DescribeLogGroups",
		jsonReply(strings.Replace(groupJSON, "%s", `,"logGroupClass":"STANDARD"`, 1)),
		jsonReply(`{"logGroups":[]}`),
		awsError(400, "AccessDeniedException", "no"))
	c := newMockClient(t, m, nil)
	g, ok, err := c.GetLogGroup(t.Context(), "app/web")
	if err != nil || !ok || g.Class != "STANDARD" || g.RetentionDays != 7 {
		t.Fatalf("%+v %t %v", g, ok, err)
	}
	if _, ok, err := c.GetLogGroup(t.Context(), "app/web"); ok || err != nil {
		t.Fatalf("missing: %t %v", ok, err)
	}
	_, _, err = c.GetLogGroup(t.Context(), "app/web")
	wantCode(t, err, ErrorCodePermissionDenied)
	_, _, err = c.GetLogGroup(t.Context(), "bad name!")
	wantCode(t, err, ErrorCodeValidation)

	arn, err := c.LogGroupARN(t.Context(), "app/web")
	if err != nil || arn != "arn:aws:logs:us-east-1:123456789012:log-group:app/web" {
		t.Fatalf("%s %v", arn, err)
	}
	if arn, _ := c.LogGroupARN(t.Context(), "arn:aws:logs:r:1:log-group:x:*"); arn != "arn:aws:logs:r:1:log-group:x" {
		t.Fatal(arn)
	}
	closed := newMockClient(t, m, nil)
	_ = closed.Close()
	_, err = closed.LogGroupARN(t.Context(), "a")
	wantCode(t, err, ErrorCodeCancelled)
	_, _, err = closed.GetLogGroup(t.Context(), "a")
	wantCode(t, err, ErrorCodeCancelled)
}

func TestDescribeLimits(t *testing.T) {
	m := newAWSMock(t).
		on("DescribeLogGroups", jsonReply(`{"logGroups":[{"logGroupName":"a"},{"logGroupName":"b"},{"logGroupName":"c"}],"nextToken":"n"}`)).
		on("DescribeLogStreams", jsonReply(`{"logStreams":[{"logStreamName":"a"},{"logStreamName":"b"}],"nextToken":"n"}`))
	c := newMockClient(t, m, nil)
	groups, err := collect(c.DescribeLogGroups(t.Context(), DescribeLogGroupsOptions{Limit: 2}))
	if err != nil || len(groups) != 2 || len(m.callsOf("DescribeLogGroups")) != 1 {
		t.Fatalf("%+v %v", groups, err)
	}
	streams, err := collect(c.DescribeLogStreams(t.Context(), "g", DescribeLogStreamsOptions{Limit: 3}))
	if err != nil || len(streams) != 3 {
		t.Fatalf("%+v %v", streams, err)
	}
	m2 := newAWSMock(t).on("DescribeLogGroups", awsError(400, "AccessDeniedException", "no"))
	_, err = collect(newMockClient(t, m2, nil).DescribeLogGroups(t.Context(), DescribeLogGroupsOptions{Limit: 2}))
	wantCode(t, err, ErrorCodePermissionDenied)
}

func TestFilterLogEventsPage(t *testing.T) {
	m := newAWSMock(t).on("FilterLogEvents", replay(t, "filterLogEvents.page1"), replay(t, "filterLogEvents.bad-token"), awsError(400, "ResourceNotFoundException", "x"))
	c := newMockClient(t, m, nil)
	page, err := c.FilterLogEventsPage(t.Context(), FilterLogEventsOptions{Group: "cwlv-0badc0de/app", PageSize: 2}, "")
	if err != nil || len(page.Events) != 2 || page.NextToken == "" {
		t.Fatalf("%+v %v", page, err)
	}
	page, err = c.FilterLogEventsPage(t.Context(), FilterLogEventsOptions{}, "garbage")
	if err != nil || len(page.Events) != 0 || page.NextToken != "" {
		t.Fatalf("bad token %+v %v", page, err)
	}
	if m.callsOf("FilterLogEvents")[1].Body["nextToken"] != "garbage" {
		t.Fatal("token not sent")
	}
	_, err = c.FilterLogEventsPage(t.Context(), FilterLogEventsOptions{}, "")
	wantCode(t, err, ErrorCodeNotFound)
	_, err = c.FilterLogEventsPage(t.Context(), FilterLogEventsOptions{Limit: -1}, "")
	wantCode(t, err, ErrorCodeValidation)
	closed := newMockClient(t, m, nil)
	_ = closed.Close()
	_, err = closed.FilterLogEventsPage(t.Context(), FilterLogEventsOptions{}, "")
	wantCode(t, err, ErrorCodeCancelled)
}

func TestLiveTailCallbacks(t *testing.T) {
	m := newAWSMock(t).on("StartLiveTail", streamReply([][]byte{
		eventFrame(t, "initial-response", nil),
		eventFrame(t, "sessionUpdate", update("first")),
		eventFrame(t, "sessionUpdate", map[string]any{"sessionResults": []any{}}),
		exceptionFrame(t, "SessionStreamingException"),
	}, false))
	var mu sync.Mutex
	var updates []LiveTailUpdate
	var ended error
	s, err := newMockClient(t, m, nil).StartLiveTail(t.Context(), LiveTailOptions{
		OnUpdate: func(u LiveTailUpdate) { mu.Lock(); updates = append(updates, u); mu.Unlock() },
		OnError:  func(err error) { mu.Lock(); ended = err; mu.Unlock() },
	}, func(LiveTailEvent) {})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, s)
	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 2 || !updates[0].Sampled || updates[0].Count != 1 || updates[1].Count != 0 || CodeOf(ended) != ErrorCodeUnavailable {
		t.Fatalf("updates %+v ended %v", updates, ended)
	}
}

func TestRecordConversions(t *testing.T) {
	ts, ing := time.UnixMilli(1000).UTC(), time.UnixMilli(2000).UTC()
	r := LiveTailEventToRecord(LiveTailEvent{Message: "plain", Timestamp: ts, IngestionTime: ing, LogStreamName: "s", LogGroupIdentifier: "arn:g"})
	if r.Attributes[AttrLogGroup] != "arn:g" || r.Attributes[AttrLogStream] != "s" || !r.ObservedTimestamp.Equal(ing) {
		t.Fatalf("%+v", r)
	}
	r = LiveTailEventToRecord(LiveTailEvent{Message: `{"timestamp":"1970-01-01T00:00:01Z","body":"b"}`, LogGroupIdentifier: "arn:g"})
	if r.Body != "b" || r.Attributes[AttrLogGroup] != nil {
		t.Fatalf("%+v", r)
	}
	r = FilteredEventToRecord(FilteredLogEvent{Message: "m"})
	if r.Attributes[AttrLogStream] != nil || r.Attributes[AttrEventID] != nil {
		t.Fatalf("%+v", r)
	}
}

func TestSearchScanCap(t *testing.T) {
	end := time.Now()
	store := &eventStore{}
	// 12,000 events in the newest 5-minute slice: the slice is narrowed until it
	// holds at most 10,000 events.
	for i := range 12_000 {
		store.add(end.Add(-4*time.Minute).UnixMilli()+int64(i*20), "needle")
	}
	m := newAWSMock(t).on("FilterLogEvents", store.handler).on("DescribeLogGroups", jsonReply(`{"logGroups":[]}`))
	q := queryOf(t, newMockClient(t, m, nil))
	res, err := q.Search(t.Context(), "needle", SearchOptions{Limit: 5, End: end})
	if err != nil || len(res.Records) != 5 || res.NextCursor == "" {
		t.Fatalf("%d %v", len(res.Records), err)
	}
	if !res.Records[0].Timestamp.After(res.Records[4].Timestamp) {
		t.Fatal("order")
	}
}
