package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"math"
	"slices"
	"sort"
	"time"
)

// DefaultEMFNamespace is the metric namespace used when EMFOptions.Namespace is empty.
const DefaultEMFNamespace = "GrowSdk"

// EMFUnits lists the units CloudWatch metrics accept.
var EMFUnits = []string{
	"Seconds", "Microseconds", "Milliseconds", "Bytes", "Kilobytes", "Megabytes", "Gigabytes", "Terabytes",
	"Bits", "Kilobits", "Megabits", "Gigabits", "Terabits", "Percent", "Count",
	"Bytes/Second", "Kilobytes/Second", "Megabytes/Second", "Gigabytes/Second", "Terabytes/Second",
	"Bits/Second", "Kilobits/Second", "Megabits/Second", "Gigabits/Second", "Terabits/Second",
	"Count/Second", "None",
}

// EMFOptions configure BuildEMF and Client.EMF.
type EMFOptions struct {
	// Namespace is the metric namespace, 1-1024 characters (default "GrowSdk").
	Namespace string
	// Unit is a CloudWatch unit from EMFUnits (default "None").
	Unit string
	// Timestamp is the metric time (default now).
	Timestamp time.Time
	// Properties are extra top-level fields (searchable, not dimensions).
	Properties map[string]any
	// Values, when not empty, replaces value with up to 100 values.
	Values []float64
	// Group and Stream select where Client.EMF writes (default: the client's
	// LogGroup and LogStream).
	Group  string
	Stream string
}

// BuildEMF builds a CloudWatch Embedded Metric Format document for one metric:
// value (or opts.Values) is published under metric with the dimensions as one
// dimension set (at most 30). It returns the JSON text; invalid input gives a
// VALIDATION error. Metric extraction happens inside AWS.
func BuildEMF(metric string, value float64, dimensions map[string]string, opts EMFOptions) (string, error) {
	const op = "emf"
	namespace := opts.Namespace
	if namespace == "" {
		namespace = DefaultEMFNamespace
	}
	unit := opts.Unit
	if unit == "" {
		unit = "None"
	}
	switch {
	case metric == "" || len(metric) > 1024:
		return "", ValidationError(op, "metric name must be 1-1024 characters")
	case len(namespace) > 1024:
		return "", ValidationError(op, "namespace must be 1-1024 characters")
	case !slices.Contains(EMFUnits, unit):
		return "", ValidationError(op, "unknown unit "+unit)
	case len(dimensions) > 30:
		return "", ValidationError(op, "at most 30 dimensions")
	case len(opts.Values) > 100:
		return "", ValidationError(op, "value must be a finite number or 1-100 finite numbers")
	}
	values := opts.Values
	if len(values) == 0 {
		values = []float64{value}
	}
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "", ValidationError(op, "value must be a finite number or 1-100 finite numbers")
		}
	}
	keys := make([]string, 0, len(dimensions))
	for k := range dimensions {
		if k == "" || k == "_aws" || k == metric {
			return "", ValidationError(op, "dimension names must be non-empty and not reserved")
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ts := opts.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	doc := map[string]any{}
	for k, v := range opts.Properties {
		if k == "_aws" || k == metric {
			return "", ValidationError(op, "property "+k+" is reserved")
		}
		doc[k] = v
	}
	for _, k := range keys {
		doc[k] = dimensions[k]
	}
	if len(opts.Values) > 0 {
		doc[metric] = values
	} else {
		doc[metric] = value
	}
	doc["_aws"] = map[string]any{
		"Timestamp": ts.UnixMilli(),
		"CloudWatchMetrics": []any{map[string]any{
			"Namespace":  namespace,
			"Dimensions": [][]string{keys},
			"Metrics":    []any{map[string]string{"Name": metric, "Unit": unit}},
		}},
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return "", ValidationError(op, "properties must be JSON-encodable")
	}
	return string(out), nil
}

// EMF builds an Embedded Metric Format document (see BuildEMF) and writes it
// with PutLogEvents to opts.Group/opts.Stream (default: the client's
// LogGroup and LogStream; a missing stream is created).
func (c *Client) EMF(ctx context.Context, metric string, value float64, dimensions map[string]string, opts EMFOptions) (PutLogEventsResult, error) {
	group, stream := opts.Group, opts.Stream
	if group == "" {
		group = c.cfg.LogGroup
	}
	if stream == "" {
		stream = c.cfg.LogStream
	}
	if group == "" {
		return PutLogEventsResult{}, ConfigError("emf: logGroup is not set (GROW_AWS_CLOUDWATCH_LOGS_LOG_GROUP or CLOUDWATCH_LOG_GROUP)")
	}
	if opts.Timestamp.IsZero() {
		opts.Timestamp = time.Now()
	}
	doc, err := BuildEMF(metric, value, dimensions, opts)
	if err != nil {
		return PutLogEventsResult{}, err
	}
	return c.PutLogEvents(ctx, group, stream, []LogEvent{{Timestamp: opts.Timestamp, Message: doc}}, PutLogEventsOptions{})
}
