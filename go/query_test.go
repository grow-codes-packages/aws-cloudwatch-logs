package cloudwatchlogs

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFilterLogEvents(t *testing.T) {
	t.Run("pages and options", func(t *testing.T) {
		m := newAWSMock(t).on("FilterLogEvents", replay(t, "filterLogEvents.page1"), replay(t, "filterLogEvents.page2"))
		c := newMockClient(t, m, nil)
		start, end := time.UnixMilli(1_795_000_000_000), time.UnixMilli(1_796_000_000_000)
		evs, err := collect(c.FilterLogEvents(t.Context(), FilterLogEventsOptions{Group: "cwlv-0badc0de/app", Streams: []string{"s1"}, Pattern: "marker", Start: start, End: end, PageSize: 2}))
		if err != nil || len(evs) < 3 || evs[0].EventID != "3" || evs[0].LogStreamName != "s1" || evs[0].Timestamp.IsZero() || evs[0].IngestionTime.IsZero() {
			t.Fatalf("%+v %v", evs, err)
		}
		body := m.callsOf("FilterLogEvents")[0].Body
		if body["filterPattern"] != "marker" || body["startTime"] != float64(start.UnixMilli()) || body["endTime"] != float64(end.UnixMilli()) || body["limit"] != float64(2) {
			t.Fatalf("body %v", body)
		}
		if m.callsOf("FilterLogEvents")[1].Body["nextToken"] == nil {
			t.Fatal("token")
		}
	})
	t.Run("limit", func(t *testing.T) {
		m := newAWSMock(t).on("FilterLogEvents", replay(t, "filterLogEvents.page1"), replay(t, "filterLogEvents.page2"))
		evs, err := collect(newMockClient(t, m, nil).FilterLogEvents(t.Context(), FilterLogEventsOptions{StreamPrefix: "s", Limit: 1}))
		if err != nil || len(evs) != 1 || len(m.callsOf("FilterLogEvents")) != 1 {
			t.Fatalf("%+v %v", evs, err)
		}
		body := m.callsOf("FilterLogEvents")[0].Body
		if body["limit"] != float64(1) || body["logGroupName"] != "app/web" || body["logStreamNamePrefix"] != "s" {
			t.Fatalf("body %v", body)
		}
		for _, err := range newMockClient(t, newAWSMock(t).on("FilterLogEvents", replay(t, "filterLogEvents.page1")), nil).FilterLogEvents(t.Context(), FilterLogEventsOptions{}) {
			_ = err
			break
		}
	})
	t.Run("errors", func(t *testing.T) {
		m := newAWSMock(t).on("FilterLogEvents", replay(t, "filterLogEvents.not-found"))
		c := newMockClient(t, m, nil)
		_, err := collect(c.FilterLogEvents(t.Context(), FilterLogEventsOptions{}))
		wantCode(t, err, ErrorCodeNotFound)
		now := time.Now()
		for i, o := range []FilterLogEventsOptions{
			{Streams: []string{"a"}, StreamPrefix: "b"}, {Limit: -1}, {Start: now, End: now.Add(-time.Second)}, {PageSize: 10_001}, {Group: "bad name!"},
		} {
			if _, err := collect(c.FilterLogEvents(t.Context(), o)); CodeOf(err) != ErrorCodeValidation {
				t.Errorf("case %d: %v", i, err)
			}
		}
		noGroup := newMockClient(t, m, func(cfg *Config) { cfg.LogGroup = "" })
		_, err = collect(noGroup.FilterLogEvents(t.Context(), FilterLogEventsOptions{}))
		wantCode(t, err, ErrorCodeConfig)
	})
}

func TestGetLogEvents(t *testing.T) {
	t.Run("forward until the token repeats", func(t *testing.T) {
		m := newAWSMock(t).on("GetLogEvents", replay(t, "getLogEvents.head"), replay(t, "getLogEvents.forward"), replay(t, "getLogEvents.end-of-stream"))
		c := newMockClient(t, m, nil)
		start, end := time.UnixMilli(1_795_000_000_000), time.UnixMilli(1_796_000_000_000)
		evs, err := collect(c.GetLogEvents(t.Context(), "cwlv-0badc0de/app", "s1", GetLogEventsOptions{StartFromHead: true, PageSize: 2, Start: start, End: end}))
		if err != nil || len(evs) < 2 || evs[0].Message == "" {
			t.Fatalf("%+v %v", evs, err)
		}
		calls := m.callsOf("GetLogEvents")
		if calls[0].Body["startFromHead"] != true || calls[0].Body["startTime"] == nil || calls[1].Body["nextToken"] == nil {
			t.Fatalf("bodies %v %v", calls[0].Body, calls[1].Body)
		}
	})
	t.Run("backward and limit", func(t *testing.T) {
		m := newAWSMock(t).on("GetLogEvents",
			jsonReply(`{"events":[{"message":"n1","timestamp":2},{"message":"n2","timestamp":3}],"nextBackwardToken":"b/1","nextForwardToken":"f/1"}`),
			jsonReply(`{"events":[{"message":"o1","timestamp":1}],"nextBackwardToken":"b/0","nextForwardToken":"f/2"}`),
			jsonReply(`{"events":[],"nextBackwardToken":"b/0","nextForwardToken":"f/2"}`))
		c := newMockClient(t, m, nil)
		evs, err := collect(c.GetLogEvents(t.Context(), "app/web", "s1", GetLogEventsOptions{}))
		if err != nil || len(evs) != 3 || evs[2].Message != "o1" {
			t.Fatalf("%+v %v", evs, err)
		}
		m2 := newAWSMock(t).on("GetLogEvents", replay(t, "getLogEvents.head"))
		evs, err = collect(newMockClient(t, m2, nil).GetLogEvents(t.Context(), "app/web", "s1", GetLogEventsOptions{Limit: 1}))
		if err != nil || len(evs) != 1 || m2.callsOf("GetLogEvents")[0].Body["limit"] != float64(1) {
			t.Fatalf("%+v %v", evs, err)
		}
		for _, err := range newMockClient(t, m2, nil).GetLogEvents(t.Context(), "app/web", "s1", GetLogEventsOptions{}) {
			_ = err
			break
		}
	})
	t.Run("errors", func(t *testing.T) {
		m := newAWSMock(t).on("GetLogEvents", replay(t, "getLogEvents.stream-not-found"))
		c := newMockClient(t, m, nil)
		_, err := collect(c.GetLogEvents(t.Context(), "app/web", "s1", GetLogEventsOptions{}))
		wantCode(t, err, ErrorCodeNotFound)
		for i, args := range []struct {
			g, s string
			o    GetLogEventsOptions
		}{{"", "s", GetLogEventsOptions{}}, {"g", "", GetLogEventsOptions{}}, {"g", "s", GetLogEventsOptions{PageSize: -1}}, {"g", "s", GetLogEventsOptions{Limit: -1}}} {
			if _, err := collect(c.GetLogEvents(t.Context(), args.g, args.s, args.o)); CodeOf(err) != ErrorCodeValidation {
				t.Errorf("case %d: %v", i, err)
			}
		}
		closed := newMockClient(t, m, nil)
		_ = closed.Close()
		_, err = collect(closed.GetLogEvents(t.Context(), "g", "s", GetLogEventsOptions{}))
		wantCode(t, err, ErrorCodeCancelled)
	})
}

func TestInsights(t *testing.T) {
	t.Run("start and results", func(t *testing.T) {
		m := newAWSMock(t).on("StartQuery", replay(t, "startQuery.ok")).on("GetQueryResults", replay(t, "getQueryResults.complete"))
		c := newMockClient(t, m, nil)
		start, end := time.Unix(1_795_000_000, 0), time.Unix(1_795_000_100, 500_000_000)
		id, err := c.StartQuery(t.Context(), "fields @timestamp, @message", QueryOptions{Groups: []string{"g1", "g2"}, Start: start, End: end, Limit: 3})
		if err != nil || id == "" {
			t.Fatal(err)
		}
		body := m.callsOf("StartQuery")[0].Body
		if body["startTime"] != float64(1_795_000_000) || body["endTime"] != float64(1_795_000_101) || body["limit"] != float64(3) || len(body["logGroupNames"].([]any)) != 2 {
			t.Fatalf("body %v", body)
		}
		res, err := c.GetQueryResults(t.Context(), id)
		if err != nil || res.Status != QueryComplete || len(res.Rows) == 0 || res.Rows[0]["@message"] == "" || res.QueryID != id {
			t.Fatalf("%+v %v", res, err)
		}
		if _, err := ParseQueryTimestamp(res.Rows[0]["@timestamp"]); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("arns, defaults and statistics", func(t *testing.T) {
		m := newAWSMock(t).
			on("StartQuery", jsonReply(`{"queryId":"q1"}`)).
			on("GetQueryResults", jsonReply(`{"status":"Complete","results":[],"statistics":{"recordsMatched":1,"recordsScanned":2,"bytesScanned":3}}`))
		c := newMockClient(t, m, nil)
		if _, err := c.StartQuery(t.Context(), "fields @message", QueryOptions{Groups: []string{"arn:aws:logs:r:1:log-group:g:*", "h"}}); err != nil {
			t.Fatal(err)
		}
		body := m.callsOf("StartQuery")[0].Body
		ids := body["logGroupIdentifiers"].([]any)
		if ids[0] != "arn:aws:logs:r:1:log-group:g" || body["endTime"].(float64)-body["startTime"].(float64) < 3599 {
			t.Fatalf("body %v", body)
		}
		if _, err := c.StartQuery(t.Context(), "fields @message", QueryOptions{}); err != nil || m.callsOf("StartQuery")[1].Body["logGroupNames"].([]any)[0] != "app/web" {
			t.Fatal("default group")
		}
		res, err := c.GetQueryResults(t.Context(), "q1")
		if err != nil || res.Statistics == nil || res.Statistics.BytesScanned != 3 {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("validation", func(t *testing.T) {
		m := newAWSMock(t)
		c := newMockClient(t, m, nil)
		now := time.Now()
		many := make([]string, 51)
		for i := range many {
			many[i] = "g"
		}
		for i, o := range []QueryOptions{{Groups: many}, {Limit: -1}, {Start: now, End: now.Add(-time.Hour)}, {Groups: []string{"bad name!"}}} {
			if _, err := c.StartQuery(t.Context(), "fields @message", o); CodeOf(err) != ErrorCodeValidation {
				t.Errorf("case %d: %v", i, err)
			}
		}
		_, err := c.StartQuery(t.Context(), " ", QueryOptions{})
		wantCode(t, err, ErrorCodeValidation)
		noGroup := newMockClient(t, m, func(cfg *Config) { cfg.LogGroup = "" })
		_, err = noGroup.StartQuery(t.Context(), "fields @message", QueryOptions{})
		wantCode(t, err, ErrorCodeConfig)
		_, err = c.GetQueryResults(t.Context(), "")
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.StopQuery(t.Context(), "")
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.Query(t.Context(), "", QueryOptions{})
		wantCode(t, err, ErrorCodeValidation)
		closed := newMockClient(t, m, nil)
		_ = closed.Close()
		_, err = closed.StartQuery(t.Context(), "fields @message", QueryOptions{})
		wantCode(t, err, ErrorCodeCancelled)
		_, err = closed.GetQueryResults(t.Context(), "q")
		wantCode(t, err, ErrorCodeCancelled)
		_, err = closed.StopQuery(t.Context(), "q")
		wantCode(t, err, ErrorCodeCancelled)
	})
	t.Run("errors from fixtures", func(t *testing.T) {
		m := newAWSMock(t).on("StartQuery", replay(t, "startQuery.not-found")).
			on("GetQueryResults", replay(t, "getQueryResults.unknown-id-moto-500")).
			on("StopQuery", replay(t, "stopQuery.moto-500"))
		c := newMockClient(t, m, nil)
		_, err := c.StartQuery(t.Context(), "fields @message", QueryOptions{})
		wantCode(t, err, ErrorCodeNotFound)
		_, err = c.GetQueryResults(t.Context(), "nope")
		wantCode(t, err, ErrorCodeUnavailable)
		_, err = c.StopQuery(t.Context(), "nope")
		wantCode(t, err, ErrorCodeUnavailable)
	})
	t.Run("stop query", func(t *testing.T) {
		m := newAWSMock(t).on("StopQuery", jsonReply(`{"success":true}`), jsonReply(`{"success":false}`))
		c := newMockClient(t, m, nil)
		if ok, err := c.StopQuery(t.Context(), "q1"); !ok || err != nil {
			t.Fatal(err)
		}
		if ok, err := c.StopQuery(t.Context(), "q1"); ok || err != nil {
			t.Fatal(err)
		}
	})
}

func TestQuery(t *testing.T) {
	t.Run("polls until complete", func(t *testing.T) {
		m := newAWSMock(t).
			on("StartQuery", awsError(400, "LimitExceededException", "too many"), jsonReply(`{"queryId":"q1"}`)).
			on("GetQueryResults", jsonReply(`{"status":"Scheduled","results":[]}`), jsonReply(`{"status":"Running","results":[[{"field":"@message","value":"partial"}]]}`),
				jsonReply(`{"status":"Complete","results":[[{"field":"@message","value":"m1"}],[{"field":"@message","value":"m2"}]]}`))
		c := newMockClient(t, m, nil)
		res, err := c.Query(t.Context(), "fields @message", QueryOptions{PollInterval: time.Millisecond})
		if err != nil || res.Status != QueryComplete || len(res.Rows) != 2 || res.Rows[1]["@message"] != "m2" {
			t.Fatalf("%+v %v", res, err)
		}
		if len(m.callsOf("StartQuery")) != 2 || len(m.callsOf("GetQueryResults")) != 3 {
			t.Fatal("calls")
		}
	})
	t.Run("terminal failures", func(t *testing.T) {
		for status, want := range map[string]ErrorCode{"Failed": ErrorCodeUnavailable, "Cancelled": ErrorCodeCancelled, "Timeout": ErrorCodeTimeout, "Unknown": ErrorCodeUnavailable, "": ErrorCodeUnavailable} {
			m := newAWSMock(t).on("StartQuery", jsonReply(`{"queryId":"q1"}`)).on("GetQueryResults", jsonReply(`{"status":"`+status+`","results":[]}`))
			_, err := newMockClient(t, m, nil).Query(t.Context(), "fields @message", QueryOptions{})
			e := wantErr(t, err, want)
			if !strings.Contains(e.Message, "q1") {
				t.Errorf("%s: %s", status, e.Message)
			}
		}
	})
	t.Run("limit retries exhausted and other start errors", func(t *testing.T) {
		m := newAWSMock(t).on("StartQuery", awsError(400, "LimitExceededException", "too many"))
		_, err := newMockClient(t, m, nil).Query(t.Context(), "fields @message", QueryOptions{PollInterval: time.Millisecond, RetryOnQueryLimit: 1})
		wantCode(t, err, ErrorCodeValidation)
		if len(m.callsOf("StartQuery")) != 2 {
			t.Fatalf("calls %d", len(m.callsOf("StartQuery")))
		}
		m2 := newAWSMock(t).on("StartQuery", awsError(400, "MalformedQueryException", "bad"))
		_, err = newMockClient(t, m2, nil).Query(t.Context(), "fields @message", QueryOptions{RetryOnQueryLimit: -1})
		wantCode(t, err, ErrorCodeValidation)
		m3 := newAWSMock(t).on("StartQuery", jsonReply(`{"queryId":"q1"}`)).on("GetQueryResults", awsError(400, "AccessDeniedException", "no"))
		_, err = newMockClient(t, m3, nil).Query(t.Context(), "fields @message", QueryOptions{})
		wantCode(t, err, ErrorCodePermissionDenied)
	})
	t.Run("timeout stops the query", func(t *testing.T) {
		m := newAWSMock(t).
			on("StartQuery", jsonReply(`{"queryId":"q1"}`)).
			on("GetQueryResults", jsonReply(`{"status":"Running","results":[]}`)).
			on("StopQuery", jsonReply(`{"success":true}`))
		_, err := newMockClient(t, m, nil).Query(t.Context(), "fields @message", QueryOptions{Timeout: 50 * time.Millisecond, PollInterval: 10 * time.Millisecond})
		wantCode(t, err, ErrorCodeTimeout)
		if len(m.callsOf("StopQuery")) != 1 {
			t.Fatal("stop not called")
		}
		m2 := newAWSMock(t).on("StartQuery", awsError(400, "LimitExceededException", "x"))
		_, err = newMockClient(t, m2, nil).Query(t.Context(), "fields @message", QueryOptions{Timeout: 30 * time.Millisecond, PollInterval: 20 * time.Millisecond, RetryOnQueryLimit: 5})
		wantCode(t, err, ErrorCodeTimeout)
	})
	t.Run("cancel stops the query", func(t *testing.T) {
		m := newAWSMock(t).
			on("StartQuery", jsonReply(`{"queryId":"q1"}`)).
			on("GetQueryResults", func(w http.ResponseWriter, r *http.Request) {
				jsonReply(`{"status":"Running","results":[]}`)(w, r)
			}).
			on("StopQuery", jsonReply(`{"success":true}`))
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(30*time.Millisecond, cancel)
		_, err := newMockClient(t, m, nil).Query(ctx, "fields @message", QueryOptions{PollInterval: 5 * time.Millisecond})
		if !errors.Is(err, ErrCancelled) {
			t.Fatal(err)
		}
		if len(m.callsOf("StopQuery")) != 1 {
			t.Fatal("stop not called")
		}
	})
	if (&Client{}).queryContextErr(context.Background(), context.Background(), "op", "", errors.New("plain")) == nil {
		t.Fatal("plain error")
	}
}

func TestDescribeQueries(t *testing.T) {
	m := newAWSMock(t).on("DescribeQueries",
		jsonReply(`{"queries":[{"queryId":"q1","queryString":"fields","status":"Complete","createTime":1795034978649,"logGroupName":"g"}],"nextToken":"n"}`),
		jsonReply(`{"queries":[{"queryId":"q2","status":"Running"}]}`))
	c := newMockClient(t, m, nil)
	qs, err := c.DescribeQueries(t.Context(), DescribeQueriesOptions{Group: "g", Status: QueryComplete})
	if err != nil || len(qs) != 2 || qs[0].QueryID != "q1" || qs[0].CreateTime.IsZero() || qs[1].Status != QueryRunning {
		t.Fatalf("%+v %v", qs, err)
	}
	if body := m.callsOf("DescribeQueries")[0].Body; body["logGroupName"] != "g" || body["status"] != "Complete" {
		t.Fatalf("%v", body)
	}
	m2 := newAWSMock(t).on("DescribeQueries", jsonReply(`{"queries":[{"queryId":"a"},{"queryId":"b"}],"nextToken":"n"}`))
	if qs, err := newMockClient(t, m2, nil).DescribeQueries(t.Context(), DescribeQueriesOptions{MaxResults: 1}); err != nil || len(qs) != 1 {
		t.Fatalf("%+v %v", qs, err)
	}
	_, err = c.DescribeQueries(t.Context(), DescribeQueriesOptions{MaxResults: -1})
	wantCode(t, err, ErrorCodeValidation)
	m3 := newAWSMock(t).on("DescribeQueries", awsError(400, "AccessDeniedException", "no"))
	_, err = newMockClient(t, m3, nil).DescribeQueries(t.Context(), DescribeQueriesOptions{})
	wantCode(t, err, ErrorCodePermissionDenied)
}

func TestParseQueryTimestamp(t *testing.T) {
	want := time.Date(2026, 11, 18, 10, 0, 0, 123_000_000, time.UTC)
	for _, s := range []string{"2026-11-18 10:00:00.123", "1794996000123", "2026-11-18T10:00:00.123Z"} {
		if got, err := ParseQueryTimestamp(s); err != nil || !got.Equal(want) {
			t.Errorf("%s: %s %v", s, got, err)
		}
	}
	if _, err := ParseQueryTimestamp("yesterday"); CodeOf(err) != ErrorCodeValidation {
		t.Fatal(err)
	}
	if !QueryComplete.Terminal() || QueryRunning.Terminal() {
		t.Fatal("terminal")
	}
}

func TestDescribeQueriesMotoFloats(t *testing.T) {
	m := newAWSMock(t).on("DescribeQueries", replay(t, "describeQueries.ok"))
	qs, err := newMockClient(t, m, nil).DescribeQueries(t.Context(), DescribeQueriesOptions{Group: "cwlv-0badc0de/app"})
	if err != nil || len(qs) == 0 || qs[0].CreateTime.IsZero() || qs[0].QueryString == "" {
		t.Fatalf("%+v %v", qs, err)
	}
	bad := newAWSMock(t).on("DescribeQueries", rawReply(200, "application/x-amz-json-1.1", `{"queries":[{"createTime":"soon"}]}`))
	_, err = newMockClient(t, bad, nil).DescribeQueries(t.Context(), DescribeQueriesOptions{})
	e := wantErr(t, err, ErrorCodeUnavailable)
	if !strings.Contains(e.Message, "invalid response") {
		t.Fatal(e.Message)
	}
}
