import type { KafkaContext } from '@nestjs/microservices';
import type { Envelope } from '../common/contract.js';
import { validateEnvelope } from '../common/contract.js';
import type { OrderCreatedData } from '../order-service/orders.service.js';
import type { SourceMessage } from './retry-policy.js';

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

export function metaOf(
  envelope: Partial<Envelope<OrderCreatedData>> | null | undefined,
  source: SourceMessage,
): Record<string, unknown> {
  const payload =
    envelope !== null && typeof envelope === 'object' ? envelope : {};
  return {
    topic: source.topic,
    partition: source.partition,
    offset: source.offset,
    eventId: typeof payload.eventId === 'string' ? payload.eventId : undefined,
    correlationId:
      typeof payload.correlationId === 'string' ? payload.correlationId : undefined,
    orderId:
      typeof payload.data === 'object' && payload.data !== null
        ? payload.data.orderId
        : undefined,
    // LATIHAN: ambil header `traceparent` dari source.headers dan sertakan
    // di setiap baris log payment-service.
  };
}

/** Contract violations are skipped: retrying cannot fix a broken payload. */
export function isContractEvent(
  envelope: Partial<Envelope<OrderCreatedData>> | null | undefined,
  meta: Record<string, unknown>,
): boolean {
  try {
    validateEnvelope(envelope);
    if (typeof envelope?.data?.orderId !== 'string') {
      throw new Error('data.orderId wajib diisi');
    }
    return true;
  } catch (error) {
    console.warn(
      JSON.stringify({
        level: 'warn',
        service: 'payment-service',
        event: 'invalid-event',
        ...meta,
        reason: (error as Error).message,
      }),
    );
    return false;
  }
}

export async function commitMessage(context: KafkaContext): Promise<void> {
  const message = context.getMessage();
  // autoCommit is off, so the offset advances only here. Kafka's committed
  // offset is the next offset to consume.
  await context.getConsumer().commitOffsets([
    {
      topic: context.getTopic(),
      partition: context.getPartition(),
      offset: (BigInt(message.offset) + 1n).toString(),
    },
  ]);
}

export function logEvent(
  event: string,
  meta: Record<string, unknown>,
  extra: Record<string, unknown> = {},
): void {
  console.log(
    JSON.stringify({
      level: 'info',
      service: 'payment-service',
      event,
      ...meta,
      ...extra,
    }),
  );
}
