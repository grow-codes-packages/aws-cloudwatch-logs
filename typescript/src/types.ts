// Service-owned: public result and option types of the CloudWatch Logs service functions.

/** A point in time: a `Date` or epoch milliseconds. */
export type TimeInput = Date | number;

/** Per-call options. */
export interface CallOptions {
  /** Cancels the call (`CancelledError`). */
  signal?: AbortSignal;
}

/** A log group (`DescribeLogGroups`). */
export interface LogGroupInfo {
  /** Log group name. */
  name: string;
  /** Log group ARN without the trailing `:*`. */
  arn: string;
  /** Creation time. */
  creationTime?: Date;
  /** Retention in days (`undefined` = never expire). */
  retentionDays?: number;
  /** Stored bytes (approximate, eventually consistent). */
  storedBytes?: number;
  /** KMS key ARN used to encrypt the group. */
  kmsKeyId?: string;
  /** Number of metric filters. */
  metricFilterCount?: number;
  /** `STANDARD`, `INFREQUENT_ACCESS`, … */
  logGroupClass?: string;
}

/** A log stream (`DescribeLogStreams`). */
export interface LogStreamInfo {
  /** Log stream name. */
  name: string;
  /** Log stream ARN. */
  arn?: string;
  /** Creation time. */
  creationTime?: Date;
  /** Time of the first event. */
  firstEventTime?: Date;
  /** Time of the last event (eventually consistent on AWS; do not rely on it). */
  lastEventTime?: Date;
  /** Last ingestion time. */
  lastIngestionTime?: Date;
}

/** One event returned by `filterLogEvents`. */
export interface FilteredLogEvent {
  /** Event time. */
  timestamp: Date;
  /** Message text. */
  message: string;
  /** Ingestion time. */
  ingestionTime?: Date;
  /** Stream that holds the event. */
  logStreamName?: string;
  /** Event id (unique within the group). */
  eventId?: string;
}

/** One event returned by `getLogEvents`. */
export interface StreamLogEvent {
  /** Event time. */
  timestamp: Date;
  /** Message text. */
  message: string;
  /** Ingestion time. */
  ingestionTime?: Date;
}

/** Events of one batch that the service did not store (indexes into the sorted batch). */
export interface RejectedLogEventsInfo {
  /** Index of the batch within the call (0-based). */
  batch: number;
  /** Events `0..tooOldLogEventEndIndex` (exclusive) were older than 14 days or the retention. */
  tooOldLogEventEndIndex?: number;
  /** Events from this index on (inclusive) were more than 2 hours in the future. */
  tooNewLogEventStartIndex?: number;
  /** Events `0..expiredLogEventEndIndex` (exclusive) were older than the retention period. */
  expiredLogEventEndIndex?: number;
}

/** Result of `putLogEvents`. */
export interface PutLogEventsResult {
  /** Events the service accepted. */
  accepted: number;
  /** Events the service rejected (too old, too new, expired). */
  rejected: number;
  /** Events truncated by the size policy before sending. */
  truncated: number;
  /** Events dropped by the size policy (`oversizePolicy: "drop"`). */
  dropped: number;
  /** PutLogEvents calls made. */
  batches: number;
  /** Details of rejected events per batch. */
  rejectedInfo: RejectedLogEventsInfo[];
}

/** Logs Insights query status. */
export type QueryStatus =
  "Scheduled" | "Running" | "Complete" | "Failed" | "Cancelled" | "Timeout" | "Unknown";

/** Logs Insights statistics (AWS only). */
export interface QueryStatistics {
  recordsMatched: number;
  recordsScanned: number;
  bytesScanned: number;
}

/** Result of `getQueryResults` / `query`. */
export interface QueryResults {
  /** Query id. */
  queryId: string;
  /** Query status. */
  status: QueryStatus;
  /** Result rows: field name → value (`@timestamp`, `@message`, `@ptr`, …). */
  rows: Record<string, string>[];
  /** Scan statistics when the service reports them. */
  statistics?: QueryStatistics;
}

/** One Logs Insights query (`describeQueries`). */
export interface QueryInfo {
  queryId: string;
  queryString: string;
  status: QueryStatus;
  createTime?: Date;
  logGroupName?: string;
}

/** One live-tail event. */
export interface LiveTailEvent {
  timestamp: Date;
  ingestionTime?: Date;
  message: string;
  logStreamName?: string;
  logGroupIdentifier?: string;
}

/** Handle of a live-tail session. */
export interface LiveTailHandle {
  /** Session id from the `sessionStart` frame. */
  readonly sessionId: string;
  /** Resolves when the session ends (service closed it, error, or {@link close}). Never rejects. */
  readonly done: Promise<void>;
  /** The error that ended the session, if any (also passed to `onError`). */
  readonly error: Error | undefined;
  /** Whether the session has ended. */
  readonly closed: boolean;
  /** Stops the session; idempotent. */
  close(): Promise<void>;
}
