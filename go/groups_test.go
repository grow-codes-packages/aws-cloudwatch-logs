package cloudwatchlogs

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const groupJSON = `{"logGroups":[{"arn":"arn:aws:logs:us-east-1:123456789012:log-group:app/web:*","logGroupArn":"arn:aws:logs:us-east-1:123456789012:log-group:app/web","creationTime":1795034545277,"logGroupName":"app/web","metricFilterCount":0,"storedBytes":0,"retentionInDays":7%s}]}`

func TestEnsureLogGroup(t *testing.T) {
	t.Run("create with retention and tags", func(t *testing.T) {
		m := newAWSMock(t).
			on("CreateLogGroup", replay(t, "ensureLogGroup.create")).
			on("DescribeLogGroups", jsonReply(strings.Replace(groupJSON, "%s", "", 1))).
			on("PutRetentionPolicy", replay(t, "putRetentionPolicy.ok"))
		c := newMockClient(t, m, nil)
		res, err := c.EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{RetentionDays: 7, Tags: map[string]string{"team": "sdk"}, KMSKeyID: "key-1"})
		if err != nil || !res.Created || res.ARN != "arn:aws:logs:us-east-1:123456789012:log-group:app/web" {
			t.Fatalf("%+v %v", res, err)
		}
		body := m.callsOf("CreateLogGroup")[0].Body
		if body["kmsKeyId"] != "key-1" || body["tags"].(map[string]any)["team"] != "sdk" {
			t.Fatalf("create body %v", body)
		}
		if m.callsOf("PutRetentionPolicy")[0].Body["retentionInDays"] != float64(7) {
			t.Fatal("retention")
		}
	})
	t.Run("existing group adds tags", func(t *testing.T) {
		m := newAWSMock(t).
			on("CreateLogGroup", awsError(400, "ResourceAlreadyExistsException", "exists")).
			on("DescribeLogGroups", jsonReply(strings.Replace(groupJSON, "%s", `,"kmsKeyId":"arn:aws:kms:us-east-1:1:key/key-1"`, 1))).
			on("TagResource", jsonReply(`{}`)).
			on("PutRetentionPolicy", replay(t, "putRetentionPolicy.ok"))
		c := newMockClient(t, m, nil)
		res, err := c.EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{RetentionDays: 14, Tags: map[string]string{"a": "b"}, KMSKeyID: "key-1"})
		if err != nil || res.Created {
			t.Fatalf("%+v %v", res, err)
		}
		if arn := m.callsOf("TagResource")[0].Body["resourceArn"]; arn != "arn:aws:logs:us-east-1:123456789012:log-group:app/web" {
			t.Fatalf("arn %v", arn)
		}
		_, err = c.EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{KMSKeyID: "other"})
		wantCode(t, err, ErrorCodeConflict)
	})
	t.Run("errors", func(t *testing.T) {
		m := newAWSMock(t).on("CreateLogGroup", replay(t, "ensureLogGroup.access-denied"))
		c := newMockClient(t, m, nil)
		_, err := c.EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{})
		wantCode(t, err, ErrorCodePermissionDenied)
		_, err = c.EnsureLogGroup(t.Context(), "bad name!", EnsureLogGroupOptions{})
		wantCode(t, err, ErrorCodeValidation)
		_, err = c.EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{RetentionDays: 8})
		wantCode(t, err, ErrorCodeValidation)
		m2 := newAWSMock(t).on("CreateLogGroup", replay(t, "ensureLogGroup.create")).on("DescribeLogGroups", replay(t, "describeLogGroups.empty"))
		_, err = newMockClient(t, m2, nil).EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{})
		wantCode(t, err, ErrorCodeNotFound)
		m3 := newAWSMock(t).
			on("CreateLogGroup", awsError(400, "ResourceAlreadyExistsException", "exists")).
			on("DescribeLogGroups", jsonReply(strings.Replace(groupJSON, "%s", "", 1))).
			on("TagResource", awsError(400, "AccessDeniedException", "no"))
		_, err = newMockClient(t, m3, nil).EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{Tags: map[string]string{"a": "b"}})
		wantCode(t, err, ErrorCodePermissionDenied)
		m4 := newAWSMock(t).
			on("CreateLogGroup", replay(t, "ensureLogGroup.create")).
			on("DescribeLogGroups", jsonReply(strings.Replace(groupJSON, "%s", "", 1))).
			on("PutRetentionPolicy", replay(t, "putRetentionPolicy.not-found"))
		_, err = newMockClient(t, m4, nil).EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{RetentionDays: 1})
		wantCode(t, err, ErrorCodeNotFound)
		m5 := newAWSMock(t).on("CreateLogGroup", replay(t, "ensureLogGroup.create")).on("DescribeLogGroups", awsError(400, "ThrottlingException", "x"))
		_, err = newMockClient(t, m5, nil).EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{})
		wantCode(t, err, ErrorCodeRateLimited)
		closed := newMockClient(t, newAWSMock(t), nil)
		_ = closed.Close()
		_, err = closed.EnsureLogGroup(t.Context(), "app/web", EnsureLogGroupOptions{})
		wantCode(t, err, ErrorCodeCancelled)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = c.EnsureLogGroup(ctx, "app/web", EnsureLogGroupOptions{})
		wantCode(t, err, ErrorCodeCancelled)
	})
	t.Run("find group pages", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups",
			jsonReply(`{"logGroups":[{"logGroupName":"app/web-other","arn":"arn:x:*"}],"nextToken":"t1"}`),
			jsonReply(`{"logGroups":[{"logGroupName":"app/web","arn":"arn:aws:logs:r:1:log-group:app/web:*"}]}`))
		g, err := newMockClient(t, m, nil).findGroup(t.Context(), "op", "app/web")
		if err != nil || g.ARN != "arn:aws:logs:r:1:log-group:app/web" {
			t.Fatalf("%+v %v", g, err)
		}
	})
	if !sameKMSKey("arn:aws:kms:r:1:key/abc", "abc") || !sameKMSKey("x", "x") || sameKMSKey("", "abc") || !sameKMSKey("arn:aws:kms:r:1:alias/a", "alias/a") {
		t.Fatal("sameKMSKey")
	}
}

func TestEnsureLogStream(t *testing.T) {
	m := newAWSMock(t).on("CreateLogStream", replay(t, "ensureLogStream.create"), replay(t, "ensureLogStream.exists"), replay(t, "ensureLogStream.group-not-found"))
	c := newMockClient(t, m, nil)
	if res, err := c.EnsureLogStream(t.Context(), "app/web", "s1", EnsureLogStreamOptions{}); err != nil || !res.Created {
		t.Fatalf("%+v %v", res, err)
	}
	if res, err := c.EnsureLogStream(t.Context(), "app/web", "s1", EnsureLogStreamOptions{}); err != nil || res.Created {
		t.Fatalf("%+v %v", res, err)
	}
	_, err := c.EnsureLogStream(t.Context(), "app/web", "s1", EnsureLogStreamOptions{})
	wantCode(t, err, ErrorCodeNotFound)
	_, err = c.EnsureLogStream(t.Context(), "", "s1", EnsureLogStreamOptions{})
	wantCode(t, err, ErrorCodeValidation)
	_, err = c.EnsureLogStream(t.Context(), "app/web", "a:b", EnsureLogStreamOptions{})
	wantCode(t, err, ErrorCodeValidation)

	m2 := newAWSMock(t).
		on("CreateLogGroup", replay(t, "ensureLogGroup.create")).
		on("DescribeLogGroups", jsonReply(strings.Replace(groupJSON, "%s", "", 1))).
		on("CreateLogStream", replay(t, "ensureLogStream.create"))
	if res, err := newMockClient(t, m2, nil).EnsureLogStream(t.Context(), "app/web", "s1", EnsureLogStreamOptions{CreateGroup: true}); err != nil || !res.Created {
		t.Fatalf("%+v %v", res, err)
	}
	m3 := newAWSMock(t).on("CreateLogGroup", awsError(400, "AccessDeniedException", "no"))
	_, err = newMockClient(t, m3, nil).EnsureLogStream(t.Context(), "app/web", "s1", EnsureLogStreamOptions{CreateGroup: true})
	wantCode(t, err, ErrorCodePermissionDenied)
	closed := newMockClient(t, newAWSMock(t), nil)
	_ = closed.Close()
	_, err = closed.EnsureLogStream(t.Context(), "app/web", "s1", EnsureLogStreamOptions{})
	wantCode(t, err, ErrorCodeCancelled)
}

func TestRetentionAndDelete(t *testing.T) {
	m := newAWSMock(t).
		on("PutRetentionPolicy", replay(t, "putRetentionPolicy.ok")).
		on("DeleteRetentionPolicy", replay(t, "putRetentionPolicy.delete")).
		on("DeleteLogGroup", replay(t, "deleteLogGroup.ok"), replay(t, "deleteLogGroup.not-found"), replay(t, "deleteLogGroup.not-found"), replay(t, "deleteLogGroup.access-denied")).
		on("DeleteLogStream", replay(t, "deleteLogStream.ok"), replay(t, "deleteLogStream.not-found"))
	c := newMockClient(t, m, nil)
	if err := c.PutRetentionPolicy(t.Context(), "app/web", 30); err != nil {
		t.Fatal(err)
	}
	if err := c.PutRetentionPolicy(t.Context(), "app/web", 0); err != nil || len(m.callsOf("DeleteRetentionPolicy")) != 1 {
		t.Fatal(err)
	}
	wantCode(t, c.PutRetentionPolicy(t.Context(), "app/web", -1), ErrorCodeValidation)
	wantCode(t, c.PutRetentionPolicy(t.Context(), "app/web", 2), ErrorCodeValidation)
	wantCode(t, c.PutRetentionPolicy(t.Context(), "", 1), ErrorCodeValidation)

	if ok, err := c.DeleteLogGroup(t.Context(), "app/web", DeleteOptions{}); !ok || err != nil {
		t.Fatal(err)
	}
	if ok, err := c.DeleteLogGroup(t.Context(), "app/web", DeleteOptions{IgnoreMissing: true}); ok || err != nil {
		t.Fatal(err)
	}
	_, err := c.DeleteLogGroup(t.Context(), "app/web", DeleteOptions{})
	wantCode(t, err, ErrorCodeNotFound)
	_, err = c.DeleteLogGroup(t.Context(), "app/web", DeleteOptions{IgnoreMissing: true})
	wantCode(t, err, ErrorCodePermissionDenied)
	_, err = c.DeleteLogGroup(t.Context(), "", DeleteOptions{})
	wantCode(t, err, ErrorCodeValidation)

	if ok, err := c.DeleteLogStream(t.Context(), "app/web", "s1", DeleteOptions{}); !ok || err != nil {
		t.Fatal(err)
	}
	if ok, err := c.DeleteLogStream(t.Context(), "app/web", "s1", DeleteOptions{IgnoreMissing: true}); ok || err != nil {
		t.Fatal(err)
	}
	_, err = c.DeleteLogStream(t.Context(), "", "s1", DeleteOptions{})
	wantCode(t, err, ErrorCodeValidation)
	_, err = c.DeleteLogStream(t.Context(), "app/web", "", DeleteOptions{})
	wantCode(t, err, ErrorCodeValidation)

	closed := newMockClient(t, newAWSMock(t), nil)
	_ = closed.Close()
	wantCode(t, closed.PutRetentionPolicy(t.Context(), "app/web", 1), ErrorCodeCancelled)
	_, err = closed.DeleteLogGroup(t.Context(), "app/web", DeleteOptions{})
	wantCode(t, err, ErrorCodeCancelled)
	_, err = closed.DeleteLogStream(t.Context(), "app/web", "s", DeleteOptions{})
	wantCode(t, err, ErrorCodeCancelled)
}

func TestDescribe(t *testing.T) {
	t.Run("log groups pages", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogGroups", replay(t, "describeLogGroups.page1"), replay(t, "describeLogGroups.page2"))
		c := newMockClient(t, m, nil)
		groups, err := collect(c.DescribeLogGroups(t.Context(), DescribeLogGroupsOptions{Prefix: "cwlv-0badc0de/", PageSize: 1}))
		if err != nil || len(groups) != 2 {
			t.Fatalf("%+v %v", groups, err)
		}
		g := groups[0]
		if g.Name != "cwlv-0badc0de/app" || strings.HasSuffix(g.ARN, ":*") || g.RetentionDays != 7 || g.CreationTime.IsZero() {
			t.Fatalf("%+v", g)
		}
		calls := m.callsOf("DescribeLogGroups")
		if calls[1].Body["nextToken"] == nil || calls[0].Body["limit"] != float64(1) {
			t.Fatalf("paging bodies %v %v", calls[0].Body, calls[1].Body)
		}
		// early stop
		m2 := newAWSMock(t).on("DescribeLogGroups", replay(t, "describeLogGroups.page1"))
		for _, err := range newMockClient(t, m2, nil).DescribeLogGroups(t.Context(), DescribeLogGroupsOptions{}) {
			_ = err
			break
		}
		if len(m2.callsOf("DescribeLogGroups")) != 1 {
			t.Fatal("early stop")
		}
		_, err = collect(c.DescribeLogGroups(t.Context(), DescribeLogGroupsOptions{PageSize: 51}))
		wantCode(t, err, ErrorCodeValidation)
		m3 := newAWSMock(t).on("DescribeLogGroups", replay(t, "describeLogGroups.limit-too-big"))
		_, err = collect(newMockClient(t, m3, nil).DescribeLogGroups(t.Context(), DescribeLogGroupsOptions{}))
		wantCode(t, err, ErrorCodeValidation)
		closed := newMockClient(t, newAWSMock(t), nil)
		_ = closed.Close()
		_, err = collect(closed.DescribeLogGroups(t.Context(), DescribeLogGroupsOptions{}))
		wantCode(t, err, ErrorCodeCancelled)
	})
	t.Run("log streams", func(t *testing.T) {
		m := newAWSMock(t).on("DescribeLogStreams", replay(t, "describeLogStreams.page1"), replay(t, "describeLogStreams.page2"))
		c := newMockClient(t, m, nil)
		streams, err := collect(c.DescribeLogStreams(t.Context(), "cwlv-0badc0de/app", DescribeLogStreamsOptions{PageSize: 2, Prefix: "s", OrderBy: OrderByLogStreamName, Descending: true}))
		if err != nil || len(streams) < 2 || streams[0].Name == "" || streams[0].CreationTime.IsZero() {
			t.Fatalf("%+v %v", streams, err)
		}
		body := m.callsOf("DescribeLogStreams")[0].Body
		if body["orderBy"] != "LogStreamName" || body["descending"] != true || body["logStreamNamePrefix"] != "s" || body["logGroupName"] != "cwlv-0badc0de/app" {
			t.Fatalf("body %v", body)
		}
		m2 := newAWSMock(t).on("DescribeLogStreams", replay(t, "describeLogStreams.last-event-desc"))
		if _, err := collect(newMockClient(t, m2, nil).DescribeLogStreams(t.Context(), "cwlv-0badc0de/app", DescribeLogStreamsOptions{OrderBy: OrderByLastEventTime})); err != nil {
			t.Fatal(err)
		}
		if m2.callsOf("DescribeLogStreams")[0].Body["orderBy"] != "LastEventTime" {
			t.Fatal("order")
		}
		for _, o := range []DescribeLogStreamsOptions{{OrderBy: OrderByLastEventTime, Prefix: "x"}, {OrderBy: "Size"}, {PageSize: 99}} {
			_, err := collect(c.DescribeLogStreams(t.Context(), "g", o))
			wantCode(t, err, ErrorCodeValidation)
		}
		_, err = collect(c.DescribeLogStreams(t.Context(), "", DescribeLogStreamsOptions{}))
		wantCode(t, err, ErrorCodeValidation)
		m3 := newAWSMock(t).on("DescribeLogStreams", replay(t, "describeLogStreams.not-found"))
		_, err = collect(newMockClient(t, m3, nil).DescribeLogStreams(t.Context(), "g", DescribeLogStreamsOptions{}))
		wantCode(t, err, ErrorCodeNotFound)
	})
}

func TestTags(t *testing.T) {
	m := newAWSMock(t).
		on("DescribeLogGroups", jsonReply(strings.Replace(groupJSON, "%s", "", 1))).
		on("TagResource", jsonReply(`{}`)).
		on("UntagResource", jsonReply(`{}`)).
		on("ListTagsForResource", replay(t, "ensureLogGroup.list-tags"))
	c := newMockClient(t, m, nil)
	if err := c.TagLogGroup(t.Context(), "app/web", map[string]string{"a": "b"}); err != nil {
		t.Fatal(err)
	}
	if err := c.UntagLogGroup(t.Context(), "arn:aws:logs:r:1:log-group:x:*", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if arn := m.callsOf("UntagResource")[0].Body["resourceArn"]; arn != "arn:aws:logs:r:1:log-group:x" {
		t.Fatalf("untag arn %v", arn)
	}
	tags, err := c.ListLogGroupTags(t.Context(), "app/web")
	if err != nil || len(tags) == 0 {
		t.Fatalf("%v %v", tags, err)
	}
	wantCode(t, c.TagLogGroup(t.Context(), "app/web", nil), ErrorCodeValidation)
	wantCode(t, c.UntagLogGroup(t.Context(), "app/web", nil), ErrorCodeValidation)
	wantCode(t, c.TagLogGroup(t.Context(), "bad name!", map[string]string{"a": "b"}), ErrorCodeValidation)
	wantCode(t, c.UntagLogGroup(t.Context(), "bad name!", []string{"a"}), ErrorCodeValidation)
	_, err = c.ListLogGroupTags(t.Context(), "bad name!")
	wantCode(t, err, ErrorCodeValidation)

	m2 := newAWSMock(t).
		on("DescribeLogGroups", jsonReply(strings.Replace(groupJSON, "%s", "", 1))).
		on("ListTagsForResource", awsError(400, "ResourceNotFoundException", "x"))
	_, err = newMockClient(t, m2, nil).ListLogGroupTags(t.Context(), "app/web")
	wantCode(t, err, ErrorCodeNotFound)
	closed := newMockClient(t, newAWSMock(t), nil)
	_ = closed.Close()
	wantCode(t, closed.TagLogGroup(t.Context(), "a", map[string]string{"a": "b"}), ErrorCodeCancelled)
	wantCode(t, closed.UntagLogGroup(t.Context(), "a", []string{"a"}), ErrorCodeCancelled)
	_, err = closed.ListLogGroupTags(t.Context(), "a")
	if !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
}
