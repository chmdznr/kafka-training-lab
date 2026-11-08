import { createRequire } from 'node:module';

// lab/shared/envelope.js is the contract helper created in Lab 03.
const require = createRequire(import.meta.url);
const shared = require('../../../../shared/envelope.js') as {
  createEnvelope: CreateEnvelope;
  validateEnvelope: (event: unknown) => Envelope;
  topicFor: (event: Envelope) => string;
  headersFor: (
    event: Envelope,
    options?: { traceparent?: string },
  ) => Record<string, string>;
};

export interface Envelope<T = unknown> {
  eventId: string;
  eventType: string;
  version: number;
  occurredAt: string;
  correlationId: string;
  causationId?: string;
  data: T;
}

export interface CreateEnvelopeInput<T = unknown> {
  eventType: string;
  data: T;
  version?: number;
  correlationId?: string;
  causationId?: string;
}

type CreateEnvelope = <T>(input: CreateEnvelopeInput<T>) => Envelope<T>;

export const createEnvelope = shared.createEnvelope;
export const validateEnvelope = shared.validateEnvelope;
export const topicFor = shared.topicFor;
export const headersFor = shared.headersFor;
