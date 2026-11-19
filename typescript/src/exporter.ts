// Service-owned: the logging-category exporter (SPEC §4 `LogExporter`, API.md §3).

import type { CloudWatchLogsClient } from "./client.js";
import { ValidationError } from "./core/index.js";
import { truncateMessage, utf8Bytes, type InputLogEvent } from "./batching.js";
import type { OversizePolicy } from "./config.js";
import {
  logRecordToJson,
  type ExportOptions,
  type ExportResult,
  type LogExporter,
  type LogRecord,
} from "./logging/index.js";

/** Options of {@link CloudWatchLogsExporter}. */
export interface CloudWatchLogsExporterOptions {
  /** Log group. */
  group: string;
  /** Log stream (created on first use). */
  stream: string;
  /** Create a missing log group on the first export (default false). */
  createGroup?: boolean;
}

/**
 * Serialises a record to its normalized JSON message. When the message is larger than
 * `maxEventBytes`, `truncate` shortens the record `body` so the message stays valid JSON (falling
 * back to a plain cut only when the other fields alone are too large); `drop` and `error` return
 * `undefined`.
 */
export function recordToMessage(
  record: LogRecord,
  maxEventBytes: number,
  policy: OversizePolicy,
): string | undefined {
  const json = logRecordToJson(record);
  let message = JSON.stringify(json);
  let size = utf8Bytes(message);
  if (size <= maxEventBytes) return message;
  if (policy !== "truncate") return undefined;
  let body = json.body;
  while (size > maxEventBytes && body !== "") {
    const bodyBytes = utf8Bytes(body);
    const target = Math.max(0, bodyBytes - (size - maxEventBytes) - 64);
    const next = target < 48 ? "" : truncateMessage(body, target);
    body = next.length < body.length ? next : "";
    message = JSON.stringify({ ...json, body });
    size = utf8Bytes(message);
  }
  return size <= maxEventBytes ? message : truncateMessage(message, maxEventBytes);
}

/**
 * Exports log records with `PutLogEvents` to one group/stream. The message of each event is the
 * normalized LogRecord JSON (redaction already applied by the log client); the event timestamp is
 * the record timestamp in ms. The `gzip` option is ignored: PutLogEvents has no request
 * compression in the AWS API model.
 */
export class CloudWatchLogsExporter implements LogExporter {
  readonly #client: CloudWatchLogsClient;
  readonly #options: CloudWatchLogsExporterOptions;

  constructor(client: CloudWatchLogsClient, options: CloudWatchLogsExporterOptions) {
    this.#client = client;
    this.#options = options;
  }

  async export(records: LogRecord[], opts: ExportOptions): Promise<ExportResult> {
    const { maxEventBytes, oversizePolicy } = this.#client.config;
    const events: InputLogEvent[] = [];
    let oversized = 0;
    for (const record of records) {
      const message = recordToMessage(record, maxEventBytes, oversizePolicy);
      if (message === undefined) oversized++;
      else events.push({ timestamp: record.timestamp.getTime(), message });
    }
    let accepted = 0;
    let rejected = oversized;
    if (events.length > 0) {
      const result = await this.#client.putLogEvents(
        this.#options.group,
        this.#options.stream,
        events,
        {
          autoCreate: true,
          ...(this.#options.createGroup === true ? { createGroup: true } : {}),
          ...(opts.signal === undefined ? {} : { signal: opts.signal }),
        },
      );
      accepted = result.accepted;
      rejected += result.rejected;
    }
    if (rejected === 0) return { accepted, rejected };
    return {
      accepted,
      rejected,
      error: new ValidationError(
        `logging.export: ${rejected} records rejected (oversized, too old, too new or expired)`,
      ),
    };
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}
