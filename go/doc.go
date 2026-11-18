// Package cloudwatchlogs is the Grow Codes SDK for Amazon CloudWatch Logs.
//
// Create a client from the environment (GROW_AWS_CLOUDWATCH_LOGS_URL,
// GROW_AWS_CLOUDWATCH_LOGS_TOKEN, ...) or from a Config literal:
//
//	client, err := cloudwatchlogs.NewClientFromEnv()
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	status, err := client.Health(ctx)
//
// Every error is an *Error; compare codes with errors.Is against the Err*
// sentinels (errors.Is(err, cloudwatchlogs.ErrNotFound)). Credentials are wrapped in
// Secret and never appear in errors, hooks or logs.
//
// Amazon CloudWatch Logs is a trademark of Amazon.com, Inc. or its affiliates. This package is not affiliated
// with or endorsed by Amazon.com, Inc. or its affiliates.
package cloudwatchlogs
