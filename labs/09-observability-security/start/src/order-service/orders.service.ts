import { randomUUID } from 'node:crypto';
import {
  BadRequestException,
  Inject,
  Injectable,
  type OnModuleInit,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import type { RecordMetadata } from 'kafkajs';
import { lastValueFrom } from 'rxjs';
import {
  createEnvelope,
  headersFor,
  validateEnvelope,
} from '../common/contract.js';
import { KAFKA_CLIENT, ORDER_CREATED_TOPIC } from '../common/kafka.js';
import { logLine } from '../common/observability.js';

export interface OrderInput {
  orderId?: string;
  customerId: string;
  amount: number;
  currency?: string;
}

export interface OrderCreatedData {
  orderId: string;
  customerId: string;
  amount: number;
  currency: string;
}

@Injectable()
export class OrdersService implements OnModuleInit {
  constructor(@Inject(KAFKA_CLIENT) private readonly kafka: ClientKafka) {}

  async onModuleInit(): Promise<void> {
    // Fail fast instead of losing the first order to a lazy connection.
    await this.kafka.connect();
  }

  async accept(
    input: Partial<OrderInput>,
    correlationId?: string,
  ) {
    if (!input || typeof input.customerId !== 'string' || input.customerId === '') {
      throw new BadRequestException('customerId wajib diisi');
    }
    if (
      typeof input.amount !== 'number' ||
      !Number.isFinite(input.amount) ||
      input.amount <= 0
    ) {
      throw new BadRequestException('amount harus berupa angka lebih besar dari 0');
    }

    const orderId = input.orderId ?? randomUUID();
    const correlation = correlationId?.trim() || randomUUID();
    // LATIHAN: terima `traceparent` dari controller, pakai nilainya bila
    // valid, dan buat yang baru bila header tidak ada. Helper tersedia di
    // ../common/observability.js:
    //   const traceparent = traceparentFrom(incomingTraceparent);
    // Lalu kirim sebagai header Kafka dan sertakan di logLine:
    //   headers: headersFor(envelope, { traceparent })
    //   logLine(..., { ..., traceparent })
    const envelope = createEnvelope<OrderCreatedData>({
      eventType: ORDER_CREATED_TOPIC,
      version: 1,
      correlationId: correlation,
      data: {
        orderId,
        customerId: input.customerId,
        amount: input.amount,
        currency: (input.currency ?? 'IDR').toUpperCase(),
      },
    });
    validateEnvelope(envelope);

    // key = orderId: every event for one order lands on the same partition,
    // so their order is preserved.
    const [metadata] = (await lastValueFrom(
      this.kafka.emit(ORDER_CREATED_TOPIC, {
        key: orderId,
        value: envelope,
        headers: headersFor(envelope),
      }),
    )) as RecordMetadata[];

    logLine('order-service', 'order.created', {
      orderId,
      eventId: envelope.eventId,
      correlationId: envelope.correlationId,
      topic: metadata?.topicName ?? ORDER_CREATED_TOPIC,
      partition: metadata?.partition,
      offset: metadata?.baseOffset,
    });

    return {
      orderId,
      eventId: envelope.eventId,
      correlationId: envelope.correlationId,
    };
  }
}
