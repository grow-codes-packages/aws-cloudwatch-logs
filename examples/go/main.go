// Command example walks through the Amazon CloudWatch Logs Go SDK: configuration
// from the environment, health and ping, creating a log group and stream,
// writing events, reading them back, a Logs Insights query, the batching
// LogClient with its Logger, and Search.
//
// It uses the standard AWS credential chain. Against the Docker emulator:
//
//	GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000 AWS_REGION=us-east-1 \
//	AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	cloudwatchlogs "github.com/grow-codes-packages/aws-cloudwatch-logs/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	group := "grow-example/go-" + time.Now().UTC().Format("20060102-150405")
	cfg, err := cloudwatchlogs.ConfigFromEnvWith(cloudwatchlogs.Config{LogGroup: group, LogStream: "example"}, nil)
	if err != nil {
		return err // CONFIG error naming the variable, never its value
	}
	fmt.Println("config:", cfg) // credentials print as [REDACTED]

	client, err := cloudwatchlogs.NewClient(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	health, err := client.Health(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("health: healthy=%t status=%s\n", health.Healthy, health.Status)
	latency, err := client.Ping(ctx)
	switch {
	case errors.Is(err, cloudwatchlogs.ErrAuthentication):
		return fmt.Errorf("check your AWS credentials: %w", err)
	case err != nil:
		return err
	}
	fmt.Println("ping:", latency)

	// Bootstrap: group (retention + tags) and stream; both calls are idempotent.
	created, err := client.EnsureLogGroup(ctx, group, cloudwatchlogs.EnsureLogGroupOptions{RetentionDays: 7, Tags: map[string]string{"team": "sdk"}})
	if err != nil {
		return err
	}
	defer func() {
		_, _ = client.DeleteLogGroup(context.Background(), group, cloudwatchlogs.DeleteOptions{IgnoreMissing: true})
	}()
	fmt.Printf("log group %s created=%t arn=%s\n", group, created.Created, created.ARN)

	// Ingest: events are sorted, size-checked and split into valid batches.
	now := time.Now()
	res, err := client.PutLogEvents(ctx, group, "raw", []cloudwatchlogs.LogEvent{
		{Timestamp: now, Message: "hello from Go"},
		{Timestamp: now.Add(time.Millisecond), Message: "something failed: ERROR"},
	}, cloudwatchlogs.PutLogEventsOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("put: accepted=%d rejected=%d batches=%d\n", res.Accepted, res.Rejected, res.Batches)

	// Read back: one stream, then a filter across the group.
	for ev, err := range client.GetLogEvents(ctx, group, "raw", cloudwatchlogs.GetLogEventsOptions{StartFromHead: true}) {
		if err != nil {
			return err
		}
		fmt.Println("event:", ev.Timestamp.Format(time.RFC3339Nano), ev.Message)
	}
	for ev, err := range client.FilterLogEvents(ctx, cloudwatchlogs.FilterLogEventsOptions{Pattern: "ERROR"}) {
		if err != nil {
			return err
		}
		fmt.Println("match:", ev.LogStreamName, ev.Message)
	}

	// Logs Insights: start, poll until complete.
	q, err := client.Query(ctx, "fields @timestamp, @message | sort @timestamp desc | limit 5", cloudwatchlogs.QueryOptions{Start: now.Add(-time.Hour)})
	if err != nil {
		return err
	}
	fmt.Printf("insights: %s, %d rows\n", q.Status, len(q.Rows))

	// Embedded Metric Format: a metric published through a log event.
	if _, err := client.EMF(ctx, "latency", 42, map[string]string{"service": "example"}, cloudwatchlogs.EMFOptions{Unit: "Milliseconds"}); err != nil {
		return err
	}

	// Category API: batching LogClient + Logger, then Search the same group.
	logs, err := client.LogClient(cloudwatchlogs.LogClientConfig{
		LogClientOptions: cloudwatchlogs.LogClientOptions{Resource: map[string]cloudwatchlogs.AttrValue{"service.name": "example"}},
	})
	if err != nil {
		return err
	}
	logger := logs.Logger("example", nil)
	logger.Info(ctx, "user signed in", "user", "ada", "password", "never-sent") // password is redacted
	if err := logs.Flush(ctx); err != nil {
		return err
	}
	search, err := client.LogQueryClient("") // the configured log group
	if err != nil {
		return err
	}
	rec, err := search.WaitForLog(ctx, "user signed in", cloudwatchlogs.WaitForLogOptions{Timeout: 20 * time.Second})
	if err != nil {
		return err
	}
	fmt.Printf("search: %s %s %v\n", rec.SeverityText, rec.Body, rec.Attributes["password"])
	return nil
}
