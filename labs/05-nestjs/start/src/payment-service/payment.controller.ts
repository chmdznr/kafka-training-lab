import { Controller, Get } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import type { Envelope } from '../common/contract.js';
import { validateEnvelope } from '../common/contract.js';
import { ORDER_CREATED_TOPIC } from '../common/kafka.js';
import type { OrderCreatedData } from '../order-service/orders.service.js';
import { ConsumerDrain } from '../common/consumer-drain.js';
import { PaymentService } from './payment.service.js';

export interface MessageMeta {
  topic: string;
  partition: number;
  offset: string;
  eventId?: string;
  correlationId?: string;
  orderId?: string;
}

@Controller()
export class PaymentController {
  constructor(
    private readonly payments: PaymentService,
    private readonly drain: ConsumerDrain,
  ) {}

  @Get('health')
  health() {
    return { status: 'ok' };
  }

  // Nest 12 types event handlers as (data, ...args: unknown[]), which rejects a
  // typed KafkaContext parameter; the cast keeps the handler signature checked
  // against the actual KafkaContext type.
  @(EventPattern(ORDER_CREATED_TOPIC) as MethodDecorator)
  async handleOrderCreated(
    @Payload() envelope: Envelope<OrderCreatedData>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    const meta = this.messageMeta(context, envelope);

    this.drain.attach(context.getConsumer(), context.getTopic());
    this.drain.enter();
    try {
      if (!this.isValid(envelope, meta)) {
        // A message that does not follow the contract cannot block the partition.
        // Lab 06 replaces this skip with a dead letter topic.
        await this.commit(context);
        return;
      }

      console.log(
        JSON.stringify({
          level: 'info',
          service: 'payment-service',
          event: 'received',
          ...meta,
        }),
      );

      await this.payments.process(envelope);

      // LATIHAN: commit offset setelah proses berhasil. Gunakan
      // context.getConsumer().commitOffsets([...]) dengan offset pesan + 1.
      // Panggil helper this.commit(context) yang sudah tersedia.

      console.log(
        JSON.stringify({
          level: 'info',
          service: 'payment-service',
          event: 'committed',
          ...meta,
        }),
      );
    } finally {
      this.drain.leave();
    }
  }

  private messageMeta(
    context: KafkaContext,
    envelope: Partial<Envelope<OrderCreatedData>>,
  ): MessageMeta {
    const payload = typeof envelope === 'object' && envelope !== null ? envelope : {};
    return {
      topic: context.getTopic(),
      partition: context.getPartition(),
      offset: context.getMessage().offset,
      eventId: typeof payload.eventId === 'string' ? payload.eventId : undefined,
      correlationId:
        typeof payload.correlationId === 'string' ? payload.correlationId : undefined,
      orderId:
        typeof payload.data === 'object' && payload.data !== null
          ? payload.data.orderId
          : undefined,
    };
  }

  private isValid(
    envelope: Envelope<OrderCreatedData>,
    meta: MessageMeta,
  ): boolean {
    try {
      validateEnvelope(envelope);
      if (typeof envelope.data?.orderId !== 'string') {
        throw new Error('envelope: data.orderId wajib diisi');
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

  private async commit(context: KafkaContext): Promise<void> {
    const message = context.getMessage();
    // autoCommit is off, so this is the only place the offset moves forward.
    await context.getConsumer().commitOffsets([
      {
        topic: context.getTopic(),
        partition: context.getPartition(),
        offset: (BigInt(message.offset) + 1n).toString(),
      },
    ]);
  }
}
