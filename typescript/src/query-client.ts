// Service-owned: the logging-category query client (SPEC §4 `LogQueryClient`, DECISIONS 2).

import type { CloudWatchLogsClient } from "./client.js";
import { ValidationError } from "./core/index.js";
import {
  BaseLogQueryClient,
  Severity,
  parseLogRecord,
  type LogRecord,
  type LogSearchOptions,
  type LogSearchResult,
  type TailHandle,
  type TailOptions,
} from "./logging/index.js";
import type { FilteredLogEvent, LiveTailEvent, LiveTailHandle } from "./types.js";

/**
 * Look-back of a descending search without `start` when the group's creation time is unknown
 * (30 days). With a known creation time the window starts one hour before it.
 */
export const DEFAULT_SEARCH_LOOKBACK_MS = 30 * 24 * 60 * 60 * 1000;
/** First time slice of a descending search (5 minutes); each further slice is 4 times wider. */
export const SEARCH_FIRST_SLICE_MS = 5 * 60_000;
/** Largest time slice of a descending search (30 days). */
export const SEARCH_MAX_SLICE_MS = 30 * 24 * 60 * 60 * 1000;
/** Events read per slice before the slice is narrowed (newest half kept). */
export const SEARCH_SCAN_CAP = 10_000;
/** Largest page size of `search`. */
export const MAX_SEARCH_LIMIT = 10_000;

const NATIVE_PATTERN_START = new Set(["{", "?", "[", "-", '"']);

/**
 * Converts a search query to a CloudWatch filter pattern: empty → match everything; a query that
 * starts with `{`, `?`, `[`, `-` or `"` is passed through as a native pattern; anything else becomes
 * an exact phrase (`"…"`, inner quotes escaped).
 */
export function toFilterPattern(query: string): string {
  const q = query.trim();
  if (q === "") return "";
  if (NATIVE_PATTERN_START.has(q[0] as string)) return q;
  return `"${q.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

/**
 * Normalizes a CloudWatch event: a message holding a normalized LogRecord JSON is decoded; any
 * other message becomes `body` with severity `INFO` and the attributes `aws.log.stream` and
 * `aws.event.id`.
 */
export function eventToLogRecord(event: FilteredLogEvent | LiveTailEvent): LogRecord {
  const message = event.message;
  if (message.trimStart().startsWith("{")) {
    try {
      const record = parseLogRecord(message);
      if (event.logStreamName !== undefined) {
        record.attributes["aws.log.stream"] = event.logStreamName;
      }
      if ("eventId" in event && event.eventId !== undefined) {
        record.attributes["aws.event.id"] = event.eventId;
      }
      return record;
    } catch {
      // not a normalized record: fall through
    }
  }
  const attributes: Record<string, string> = {};
  if (event.logStreamName !== undefined) attributes["aws.log.stream"] = event.logStreamName;
  if ("eventId" in event && event.eventId !== undefined) attributes["aws.event.id"] = event.eventId;
  if ("logGroupIdentifier" in event && event.logGroupIdentifier !== undefined) {
    attributes["aws.log.group"] = event.logGroupIdentifier;
  }
  const record: LogRecord = {
    timestamp: event.timestamp,
    severity: Severity.Info,
    severityText: "INFO",
    body: message,
    attributes,
    resource: {},
  };
  if (event.ingestionTime !== undefined) record.observedTimestamp = event.ingestionTime;
  return record;
}

interface DescCursor {
  o: "d";
  /** Exclusive end of the current slice (ms). */
  e: number;
  /** Width of the current slice (ms). */
  w: number;
  /** Records of the current slice already returned. */
  k: number;
}

interface AscCursor {
  o: "a";
  /** FilterLogEvents nextToken. */
  t: string;
}

function encodeCursor(cursor: DescCursor | AscCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

function decodeCursor(text: string): DescCursor | AscCursor {
  try {
    const parsed = JSON.parse(Buffer.from(text, "base64url").toString("utf8")) as Record<
      string,
      unknown
    >;
    if (parsed.o === "a" && typeof parsed.t === "string") return { o: "a", t: parsed.t };
    if (
      parsed.o === "d" &&
      typeof parsed.e === "number" &&
      typeof parsed.w === "number" &&
      typeof parsed.k === "number" &&
      parsed.w > 0 &&
      parsed.k >= 0
    ) {
      return { o: "d", e: parsed.e, w: parsed.w, k: parsed.k };
    }
  } catch {
    // fall through
  }
  throw new ValidationError("logging.search: invalid cursor");
}

function newestFirst(a: FilteredLogEvent, b: FilteredLogEvent): number {
  return (
    b.timestamp.getTime() - a.timestamp.getTime() ||
    (b.eventId ?? "").localeCompare(a.eventId ?? "")
  );
}

/**
 * {@link LogQueryClient} over one log group. `search` uses `FilterLogEvents`: `order: "asc"` reads
 * natively (oldest first, cursor = the service token); `order: "desc"` (default) walks the time
 * window backwards in slices (5 min, each 4 times wider), from one hour before the group's creation time (else 30 days), newest slice first, and sorts each
 * slice newest first. `tail` uses Live Tail; `listStreams` uses `DescribeLogStreams`;
 * `listLabels` is unsupported (CloudWatch Logs has no label inventory).
 */
export class CloudWatchLogsQueryClient extends BaseLogQueryClient {
  readonly #client: CloudWatchLogsClient;
  /** The log group searched. */
  readonly group: string;
  #createdMs: number | undefined;

  constructor(client: CloudWatchLogsClient, group: string) {
    super();
    this.#client = client;
    this.group = group;
  }

  async search(query: string, opts: LogSearchOptions = {}): Promise<LogSearchResult> {
    const limit = opts.limit ?? 100;
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_SEARCH_LIMIT) {
      throw new ValidationError(
        `logging.search: limit must be an integer from 1 to ${MAX_SEARCH_LIMIT}`,
      );
    }
    const pattern = toFilterPattern(query);
    const cursor = opts.cursor === undefined ? undefined : decodeCursor(opts.cursor);
    const order = opts.order ?? "desc";
    if (cursor !== undefined && cursor.o !== order[0]) {
      throw new ValidationError("logging.search: the cursor belongs to the other order");
    }
    return order === "asc"
      ? this.#searchAsc(pattern, limit, opts, cursor as AscCursor | undefined)
      : this.#searchDesc(pattern, limit, opts, cursor as DescCursor | undefined);
  }

  async #searchAsc(
    pattern: string,
    limit: number,
    opts: LogSearchOptions,
    cursor: AscCursor | undefined,
  ): Promise<LogSearchResult> {
    const records: LogRecord[] = [];
    let token = cursor?.t;
    // FilterLogEvents can return empty pages with a token: keep reading until `limit` or the end.
    do {
      const page = await this.#client.filterLogEventsPage({
        group: this.group,
        pattern,
        pageSize: limit - records.length,
        ...(opts.start === undefined ? {} : { start: opts.start }),
        ...(opts.end === undefined ? {} : { end: opts.end }),
        ...(token === undefined ? {} : { nextToken: token }),
        ...(opts.signal === undefined ? {} : { signal: opts.signal }),
      });
      records.push(...page.events.slice(0, limit - records.length).map(eventToLogRecord));
      token = page.nextToken;
    } while (token !== undefined && records.length < limit);
    return token === undefined
      ? { records }
      : { records, nextCursor: encodeCursor({ o: "a", t: token }) };
  }

  async #searchDesc(
    pattern: string,
    limit: number,
    opts: LogSearchOptions,
    cursor: DescCursor | undefined,
  ): Promise<LogSearchResult> {
    const endMs = (opts.end ?? new Date()).getTime();
    const startMs = opts.start?.getTime() ?? (await this.#defaultStart(endMs, opts.signal));
    let sliceEnd = cursor?.e ?? endMs + 1;
    let width = cursor?.w ?? SEARCH_FIRST_SLICE_MS;
    let skip = cursor?.k ?? 0;
    const records: LogRecord[] = [];
    while (records.length < limit && sliceEnd > startMs) {
      let sliceStart = Math.max(startMs, sliceEnd - width);
      let events = await this.#scan(pattern, sliceStart, sliceEnd - 1, opts.signal);
      while (events.length > SEARCH_SCAN_CAP && sliceEnd - sliceStart > 1000) {
        width = Math.max(1000, Math.floor((sliceEnd - sliceStart) / 2));
        sliceStart = Math.max(startMs, sliceEnd - width);
        events = await this.#scan(pattern, sliceStart, sliceEnd - 1, opts.signal);
      }
      events.sort(newestFirst);
      const available = events.slice(skip);
      const take = available.slice(0, limit - records.length);
      records.push(...take.map(eventToLogRecord));
      if (take.length < available.length) {
        return {
          records,
          nextCursor: encodeCursor({ o: "d", e: sliceEnd, w: width, k: skip + take.length }),
        };
      }
      sliceEnd = sliceStart;
      skip = 0;
      width = Math.min(width * 4, SEARCH_MAX_SLICE_MS);
    }
    const result: LogSearchResult = { records };
    if (sliceEnd > startMs)
      result.nextCursor = encodeCursor({ o: "d", e: sliceEnd, w: width, k: 0 });
    return result;
  }

  /** One hour before the group's creation time, else `end` − 30 days. */
  async #defaultStart(endMs: number, signal: AbortSignal | undefined): Promise<number> {
    if (this.#createdMs === undefined) {
      const group = await this.#client.getLogGroup(
        this.group,
        signal === undefined ? {} : { signal },
      );
      if (group?.creationTime !== undefined) this.#createdMs = group.creationTime.getTime();
    }
    return this.#createdMs === undefined
      ? endMs - DEFAULT_SEARCH_LOOKBACK_MS
      : Math.min(endMs, this.#createdMs - 3_600_000);
  }

  /** Reads up to SEARCH_SCAN_CAP + 1 events of `[start, end]` (inclusive). */
  async #scan(
    pattern: string,
    start: number,
    end: number,
    signal: AbortSignal | undefined,
  ): Promise<FilteredLogEvent[]> {
    const out: FilteredLogEvent[] = [];
    for await (const ev of this.#client.filterLogEvents({
      group: this.group,
      pattern,
      start,
      end,
      limit: SEARCH_SCAN_CAP + 1,
      ...(signal === undefined ? {} : { signal }),
    })) {
      out.push(ev);
    }
    return out;
  }

  /**
   * Streams new records with Live Tail (filter pattern built like {@link search}). Errors and the
   * end of the session are reported to `opts.onError`; `opts.start`/`intervalMs` do not apply.
   */
  override tail(
    query: string,
    onRecord: (record: LogRecord) => void,
    opts: TailOptions = {},
  ): TailHandle {
    let stopped = false;
    let session: LiveTailHandle | undefined;
    const report = (error: unknown): void => {
      if (!stopped) opts.onError?.(error);
    };
    this.#client
      .startLiveTail(
        { groups: [this.group], pattern: toFilterPattern(query) },
        (event) => {
          if (!stopped) onRecord(eventToLogRecord(event));
        },
        { onError: report },
      )
      .then((handle) => {
        session = handle;
        if (stopped) void handle.close();
      }, report);
    return {
      stop(): void {
        stopped = true;
        void session?.close();
      },
    };
  }

  /** Names of the streams of the group (`DescribeLogStreams`, all pages). */
  override async listStreams(): Promise<string[]> {
    const names: string[] = [];
    for await (const s of this.#client.describeLogStreams(this.group)) names.push(s.name);
    return names;
  }
}
