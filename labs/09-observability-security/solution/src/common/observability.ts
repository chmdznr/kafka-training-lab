import { randomUUID } from 'node:crypto';

// W3C Trace Context, version 00: 00-<32 hex trace-id>-<16 hex span-id>-<2 hex flags>
const TRACEPARENT_PATTERN =
  /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/;

/** A new traceparent. The trace id is 16 random bytes, the span id 8. */
export function newTraceparent(): string {
  const traceId = randomUUID().replaceAll('-', '');
  const spanId = randomUUID().replaceAll('-', '').slice(0, 16);
  return `00-${traceId}-${spanId}-01`;
}

/**
 * Keep a valid incoming traceparent and generate one when the caller did not
 * send it. The generated value travels with the event in the Kafka header, so
 * every hop logs the same trace id.
 */
export function traceparentFrom(incoming: string | undefined): string {
  const value = incoming?.trim();
  return value && TRACEPARENT_PATTERN.test(value) ? value : newTraceparent();
}

/** One JSON line per event, with the fields the lab asks for. */
export function logLine(
  service: string,
  event: string,
  fields: Record<string, unknown> = {},
): void {
  console.log(JSON.stringify({ level: 'info', service, event, ...fields }));
}
