'use strict';

// Contract helper shared by every lab (see kerangka-materi section 2).
//
//   envelope = {
//     eventId, eventType, version, occurredAt,
//     correlationId, causationId?, data
//   }
//
// eventType is the Kafka event type and matches the topic name, including the
// version suffix: "order.created.v1". The numeric version field must agree
// with that suffix. topicFor() also accepts the short form "order.created" and
// appends the suffix, so both spellings are safe.

const { randomUUID } = require('node:crypto');

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const EVENT_TYPE_PATTERN = /^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+$/;
const ISO_8601_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;

class EnvelopeError extends Error {
  constructor(errors) {
    super(`Envelope tidak valid: ${errors.join('; ')}`);
    this.name = 'EnvelopeError';
    this.errors = errors;
  }
}

/**
 * Build a valid envelope. eventType is normally the full topic name
 * ("order.created.v1"); a short name ("order.created") is also accepted.
 */
function createEnvelope({
  eventType,
  version = 1,
  data,
  correlationId,
  causationId,
  occurredAt,
  eventId,
} = {}) {
  const envelope = {
    eventId: eventId || randomUUID(),
    eventType,
    version,
    occurredAt: occurredAt || new Date().toISOString(),
    correlationId: correlationId || randomUUID(),
    ...(causationId ? { causationId } : {}),
    data,
  };
  validateEnvelope(envelope);
  return envelope;
}

/**
 * Validate an envelope and return it unchanged. Throws EnvelopeError listing
 * every problem, so a consumer cannot silently accept a broken event.
 */
function validateEnvelope(envelope) {
  if (envelope === null || typeof envelope !== 'object' || Array.isArray(envelope)) {
    throw new EnvelopeError(['envelope harus berupa object']);
  }

  const errors = [];
  const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

  if (typeof envelope.eventId !== 'string' || !UUID_PATTERN.test(envelope.eventId)) {
    errors.push('eventId wajib UUID');
  }
  if (typeof envelope.eventType !== 'string' || !EVENT_TYPE_PATTERN.test(envelope.eventType)) {
    errors.push('eventType wajib berformat <domain>.<event>[.v<n>]');
  }
  if (!Number.isInteger(envelope.version) || envelope.version < 1) {
    errors.push('version wajib integer >= 1');
  }
  if (
    typeof envelope.occurredAt !== 'string' ||
    !ISO_8601_PATTERN.test(envelope.occurredAt) ||
    Number.isNaN(Date.parse(envelope.occurredAt))
  ) {
    errors.push('occurredAt wajib ISO-8601 dengan zona waktu');
  }
  if (typeof envelope.correlationId !== 'string' || envelope.correlationId.trim() === '') {
    errors.push('correlationId wajib string tidak kosong');
  }
  if (
    envelope.causationId !== undefined &&
    (typeof envelope.causationId !== 'string' || envelope.causationId.trim() === '')
  ) {
    errors.push('causationId harus string tidak kosong bila diisi');
  }
  if (!isObject(envelope.data)) {
    errors.push('data wajib object');
  }
  if (
    typeof envelope.eventType === 'string' &&
    /\.v\d+$/.test(envelope.eventType) &&
    Number.isInteger(envelope.version) &&
    !envelope.eventType.endsWith(`.v${envelope.version}`)
  ) {
    errors.push(`eventType ${envelope.eventType} tidak cocok dengan version ${envelope.version}`);
  }

  if (errors.length > 0) {
    throw new EnvelopeError(errors);
  }
  return envelope;
}

/** Topic name for an event: order.created.v1 stays as-is, order.created gets .v1 */
function topicFor(envelope) {
  validateEnvelope(envelope);
  const suffix = `.v${envelope.version}`;
  return envelope.eventType.endsWith(suffix) ? envelope.eventType : `${envelope.eventType}${suffix}`;
}

/**
 * Kafka headers required by the contract. traceparent is added only when the
 * caller has one (sesi 09). Header values must be strings or Buffers.
 */
function headersFor(envelope, { traceparent } = {}) {
  const headers = {
    'correlation-id': validateEnvelope(envelope).correlationId,
    'event-type': topicFor(envelope),
  };
  if (traceparent) {
    headers.traceparent = traceparent;
  }
  return headers;
}

module.exports = {
  createEnvelope,
  validateEnvelope,
  topicFor,
  headersFor,
  EnvelopeError,
};
