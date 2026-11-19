// Package cloudwatchlogs is the Grow Codes SDK for Amazon CloudWatch Logs, built on
// the official AWS SDK for Go v2 (service/cloudwatchlogs).
//
// Create a client from the environment (the standard AWS credential chain plus
// GROW_AWS_CLOUDWATCH_LOGS_* and CLOUDWATCH_LOG_GROUP / CLOUDWATCH_LOG_STREAM) or
// from a Config literal:
//
//	client, err := cloudwatchlogs.NewClientFromEnv()
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	res, err := client.PutLogEvents(ctx, "app/web", "worker-1",
//		[]cloudwatchlogs.LogEvent{{Message: "hello"}}, cloudwatchlogs.PutLogEventsOptions{})
//
// Service functions: EnsureLogGroup, EnsureLogStream, PutRetentionPolicy,
// DeleteLogGroup, DeleteLogStream, DescribeLogGroups and DescribeLogStreams
// (iterators), TagLogGroup / UntagLogGroup / ListLogGroupTags, PutLogEvents (with
// client-side batching that respects the service limits), BuildEMF / EMF,
// FilterLogEvents and GetLogEvents (iterators), StartQuery / GetQueryResults /
// Query / StopQuery / DescribeQueries (Logs Insights) and StartLiveTail.
//
// The logging category API: NewLogClient returns a batching LogClient with a
// Logger facade and redaction that writes to the configured log group, and the
// Client itself implements LogQueryClient (Search, Tail, ListStreams, ListLabels,
// WaitForLog). NewSlogHandler and the zapadapter subpackage bridge slog and zap.
//
// Every error is an *Error; compare codes with errors.Is against the Err*
// sentinels (errors.Is(err, cloudwatchlogs.ErrNotFound)). AWS errors are mapped
// by AWS error code (AWSErrorCode returns the original code). Credentials are
// wrapped in Secret and never appear in errors, hooks or logs.
//
// Amazon CloudWatch Logs is a trademark of Amazon.com, Inc. or its affiliates. This package is not affiliated
// with or endorsed by Amazon.com, Inc. or its affiliates.
package cloudwatchlogs
