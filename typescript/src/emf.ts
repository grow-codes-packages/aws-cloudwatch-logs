// Service-owned: CloudWatch Embedded Metric Format (EMF) documents (API.md §2.2).
//
// An EMF document is a JSON log event that CloudWatch turns into a metric. The SDK only builds and
// sends the document; metric extraction happens inside AWS.

import { ValidationError } from "./core/index.js";

/** Units accepted by CloudWatch metrics. */
export const EMF_UNITS: readonly string[] = Object.freeze([
  "Seconds",
  "Microseconds",
  "Milliseconds",
  "Bytes",
  "Kilobytes",
  "Megabytes",
  "Gigabytes",
  "Terabytes",
  "Bits",
  "Kilobits",
  "Megabits",
  "Gigabits",
  "Terabits",
  "Percent",
  "Count",
  "Bytes/Second",
  "Kilobytes/Second",
  "Megabytes/Second",
  "Gigabytes/Second",
  "Terabytes/Second",
  "Bits/Second",
  "Kilobits/Second",
  "Megabits/Second",
  "Gigabits/Second",
  "Terabits/Second",
  "Count/Second",
  "None",
]);

/** Default EMF namespace. */
export const DEFAULT_EMF_NAMESPACE = "GrowSdk";

/** Options of {@link buildEmf}. */
export interface EmfOptions {
  /** Metric namespace, 1–1024 characters (default `GrowSdk`). */
  namespace?: string;
  /** CloudWatch unit (default `None`). */
  unit?: string;
  /** Metric time (default now). */
  timestamp?: Date;
  /** Extra top-level properties (searchable fields that are not dimensions). */
  properties?: Record<string, string | number | boolean>;
}

/** An EMF document (see the CloudWatch EMF specification). */
export interface EmfDocument {
  _aws: {
    Timestamp: number;
    CloudWatchMetrics: {
      Namespace: string;
      Dimensions: string[][];
      Metrics: { Name: string; Unit: string }[];
    }[];
  };
  [key: string]: unknown;
}

/**
 * Builds an EMF document for one metric. `value` is a finite number or an array of at most 100
 * finite numbers; `dimensions` (at most 30) become one dimension set. Throws `ValidationError` on
 * invalid input.
 */
export function buildEmf(
  metric: string,
  value: number | readonly number[],
  dimensions: Readonly<Record<string, string>> = {},
  options: EmfOptions = {},
): EmfDocument {
  const namespace = options.namespace ?? DEFAULT_EMF_NAMESPACE;
  const unit = options.unit ?? "None";
  if (typeof metric !== "string" || metric === "" || metric.length > 1024) {
    throw new ValidationError("emf: metric name must be 1-1024 characters");
  }
  if (namespace === "" || namespace.length > 1024) {
    throw new ValidationError("emf: namespace must be 1-1024 characters");
  }
  if (!EMF_UNITS.includes(unit)) throw new ValidationError(`emf: unknown unit ${unit}`);
  const values = typeof value === "number" ? [value] : [...value];
  if (
    values.length === 0 ||
    values.length > 100 ||
    !values.every((v) => typeof v === "number" && Number.isFinite(v))
  ) {
    throw new ValidationError("emf: value must be a finite number or 1-100 finite numbers");
  }
  const dimensionKeys = Object.keys(dimensions);
  if (dimensionKeys.length > 30) throw new ValidationError("emf: at most 30 dimensions");
  for (const key of dimensionKeys) {
    if (key === "" || key === "_aws" || key === metric || typeof dimensions[key] !== "string") {
      throw new ValidationError("emf: dimension names must be non-empty and values strings");
    }
  }
  const timestamp = (options.timestamp ?? new Date()).getTime();
  if (!Number.isFinite(timestamp)) throw new ValidationError("emf: invalid timestamp");
  const doc: EmfDocument = {
    _aws: {
      Timestamp: timestamp,
      CloudWatchMetrics: [
        {
          Namespace: namespace,
          Dimensions: [dimensionKeys],
          Metrics: [{ Name: metric, Unit: unit }],
        },
      ],
    },
  };
  for (const [k, v] of Object.entries(options.properties ?? {})) {
    if (k === "_aws" || k === metric) throw new ValidationError(`emf: property ${k} is reserved`);
    doc[k] = v;
  }
  for (const key of dimensionKeys) doc[key] = dimensions[key];
  doc[metric] = typeof value === "number" ? value : values;
  return doc;
}
