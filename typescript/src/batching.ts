// Service-owned: client-side PutLogEvents batching (DECISIONS 5, API.md §2.2).
//
// CloudWatch Logs rejects a PutLogEvents call with more than 10,000 events, more than 1,048,576
// bytes (sum of UTF-8 message bytes + 26 per event), events spanning more than 24 hours or events
// out of chronological order. These limits are enforced here, before anything is sent (the Moto
// emulator enforces none of them).

import { ValidationError } from "./core/index.js";
import {
  DEFAULT_MAX_EVENT_BYTES,
  MAX_EVENT_BYTES_HARD_CAP,
  type OversizePolicy,
} from "./config.js";

/** Most events in one PutLogEvents call. */
export const MAX_EVENTS_PER_BATCH = 10_000;
/** Most bytes in one PutLogEvents call (Σ UTF-8 message bytes + 26 per event). */
export const MAX_BATCH_BYTES = 1_048_576;
/** Bytes AWS adds to every event when it checks the batch size. */
export const EVENT_OVERHEAD_BYTES = 26;
/** Longest time span (last − first timestamp) of one batch: 24 hours. */
export const MAX_BATCH_SPAN_MS = 24 * 60 * 60 * 1000;

/** One log event to send. */
export interface InputLogEvent {
  /** Event time (`Date` or epoch milliseconds). */
  timestamp: Date | number;
  /** Message text. */
  message: string;
}

/** An event as sent to AWS (epoch milliseconds). */
export interface WireLogEvent {
  timestamp: number;
  message: string;
}

/** Options of {@link planLogEventBatches}. */
export interface BatchPlanOptions {
  /** Per-event limit in UTF-8 bytes (default 262 144, max 1 048 550). */
  maxEventBytes?: number;
  /** What to do with larger events (default `truncate`). */
  oversizePolicy?: OversizePolicy;
}

/** Result of {@link planLogEventBatches}. */
export interface BatchPlan {
  /** Batches in send order; each is sorted by timestamp and within every service limit. */
  batches: WireLogEvent[][];
  /** Events that were truncated. */
  truncated: number;
  /** Events that were dropped (`oversizePolicy: "drop"`). */
  dropped: number;
}

/** UTF-8 size of a string in bytes. */
export function utf8Bytes(text: string): number {
  return Buffer.byteLength(text, "utf8");
}

/** Cuts a UTF-8 buffer to at most `maxBytes` without splitting a character. */
function cutUtf8(buf: Buffer, maxBytes: number): string {
  let end = Math.min(buf.length, Math.max(0, maxBytes));
  while (end > 0 && end < buf.length && ((buf[end] as number) & 0xc0) === 0x80) end--;
  return buf.subarray(0, end).toString("utf8");
}

function marker(removed: number): string {
  return ` …[truncated ${removed} bytes]`;
}

/**
 * Truncates `message` on a UTF-8 character boundary so the result, including the appended
 * ` …[truncated N bytes]` marker, is at most `maxBytes` UTF-8 bytes. `N` is the number of bytes
 * removed from the original. Returns `message` unchanged when it already fits.
 */
export function truncateMessage(message: string, maxBytes: number): string {
  const buf = Buffer.from(message, "utf8");
  if (buf.length <= maxBytes) return message;
  // `removed` only grows (a longer marker leaves less room) and is bounded, so this converges.
  let removed = buf.length - maxBytes;
  for (;;) {
    const kept = cutUtf8(buf, maxBytes - utf8Bytes(marker(removed)));
    const actual = buf.length - utf8Bytes(kept);
    if (actual === removed) {
      const out = kept + marker(actual);
      return utf8Bytes(out) <= maxBytes ? out : cutUtf8(Buffer.from(out, "utf8"), maxBytes);
    }
    removed = actual;
  }
}

function toMillis(value: Date | number, index: number): number {
  const ms = value instanceof Date ? value.getTime() : value;
  if (typeof ms !== "number" || !Number.isFinite(ms)) {
    throw new ValidationError(`putLogEvents: event ${index} has an invalid timestamp`);
  }
  return Math.trunc(ms);
}

/**
 * Applies the per-event size policy, sorts events by timestamp (stable) and splits them into
 * batches that respect every PutLogEvents limit. Throws `ValidationError` for invalid timestamps,
 * non-string messages, or an oversized event with `oversizePolicy: "error"`.
 */
export function planLogEventBatches(
  events: readonly InputLogEvent[],
  options: BatchPlanOptions = {},
): BatchPlan {
  const maxEventBytes = options.maxEventBytes ?? DEFAULT_MAX_EVENT_BYTES;
  const policy = options.oversizePolicy ?? "truncate";
  if (
    !Number.isInteger(maxEventBytes) ||
    maxEventBytes < 1 ||
    maxEventBytes > MAX_EVENT_BYTES_HARD_CAP
  ) {
    throw new ValidationError(
      `putLogEvents: maxEventBytes must be an integer from 1 to ${MAX_EVENT_BYTES_HARD_CAP}`,
    );
  }
  let truncated = 0;
  let dropped = 0;
  const prepared: (WireLogEvent & { bytes: number })[] = [];
  events.forEach((event, index) => {
    if (typeof event.message !== "string") {
      throw new ValidationError(`putLogEvents: event ${index} has no string message`);
    }
    const timestamp = toMillis(event.timestamp, index);
    let message = event.message;
    let bytes = utf8Bytes(message);
    if (bytes > maxEventBytes) {
      if (policy === "error") {
        throw new ValidationError(
          `putLogEvents: event ${index} is ${bytes} bytes, more than maxEventBytes ${maxEventBytes}`,
        );
      }
      if (policy === "drop") {
        dropped++;
        return;
      }
      message = truncateMessage(message, maxEventBytes);
      bytes = utf8Bytes(message);
      truncated++;
    }
    prepared.push({ timestamp, message, bytes });
  });
  prepared.sort((a, b) => a.timestamp - b.timestamp);
  const batches: WireLogEvent[][] = [];
  let current: WireLogEvent[] = [];
  let currentBytes = 0;
  let first = 0;
  for (const event of prepared) {
    const size = event.bytes + EVENT_OVERHEAD_BYTES;
    if (
      current.length > 0 &&
      (current.length >= MAX_EVENTS_PER_BATCH ||
        currentBytes + size > MAX_BATCH_BYTES ||
        event.timestamp - first > MAX_BATCH_SPAN_MS)
    ) {
      batches.push(current);
      current = [];
      currentBytes = 0;
    }
    if (current.length === 0) first = event.timestamp;
    current.push({ timestamp: event.timestamp, message: event.message });
    currentBytes += size;
  }
  if (current.length > 0) batches.push(current);
  return { batches, truncated, dropped };
}
