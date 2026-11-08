import type { KafkaContext } from '@nestjs/microservices';
import { validateEnvelope, type Envelope } from './contract.js';
import type { KafkaHeaders } from './kafka.js';

export interface SourceMessage {
  topic: string;
  partition: number;
  offset: string;
  key: Buffer | null;
  headers: KafkaHeaders;
}

export function sourceOf(context: KafkaContext): SourceMessage {
  const message = context.getMessage();
  return {
    topic: context.getTopic(),
    partition: context.getPartition(),
    offset: message.offset,
    key: message.key ?? null,
    headers: message.headers ?? {},
  };
}

/** Returns the contract problem, or null when the envelope is valid. */
export function envelopeProblem(envelope: unknown): string | null {
  try {
    validateEnvelope(envelope);
    return null;
  } catch (error) {
    return (error as Error).message;
  }
}

export function metaOf<T>(
  envelope: Partial<Envelope<T>> | null | undefined,
  source: SourceMessage,
): Record<string, unknown> {
  const payload = envelope !== null && typeof envelope === 'object' ? envelope : {};
  const data = (
    typeof payload.data === 'object' && payload.data !== null ? payload.data : {}
  ) as Record<string, unknown>;
  return {
    topic: source.topic,
    partition: source.partition,
    offset: source.offset,
    eventId: typeof payload.eventId === 'string' ? payload.eventId : undefined,
    eventType: typeof payload.eventType === 'string' ? payload.eventType : undefined,
    correlationId:
      typeof payload.correlationId === 'string' ? payload.correlationId : undefined,
    orderId: typeof data.orderId === 'string' ? data.orderId : undefined,
  };
}

export async function commitMessage(context: KafkaContext): Promise<void> {
  const message = context.getMessage();
  // Kafka's committed offset is the next offset to consume.
  await context.getConsumer().commitOffsets([
    {
      topic: context.getTopic(),
      partition: context.getPartition(),
      offset: (BigInt(message.offset) + 1n).toString(),
    },
  ]);
}
