import { Controller, Get } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ConsumerDrain } from '../common/consumer-drain.js';
import type { Envelope } from '../common/contract.js';
import { ORDER_CREATED_TOPIC } from '../common/kafka.js';
import type { OrderCreatedData } from '../order-service/orders.service.js';
import {
  commitMessage,
  isContractEvent,
  logEvent,
  metaOf,
  sourceOf,
} from './kafka-helpers.js';
import { PaymentService } from './payment.service.js';
import { RetryPolicy } from './retry-policy.js';

@Controller()
export class PaymentController {
  constructor(
    private readonly payments: PaymentService,
    private readonly retryPolicy: RetryPolicy,
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
    const source = sourceOf(context);
    const meta = metaOf(envelope, source);

    // One line before processing, so the correlation id and trace id are
    // visible even when the handler later fails.
    logEvent('received', meta);

    this.drain.attach(context.getConsumer(), context.getTopic());
    this.drain.enter();
    try {
      if (!isContractEvent(envelope, meta)) {
        await commitMessage(context);
        return;
      }

      const result = await this.payments.process(envelope, source.topic);
      await commitMessage(context);
      logEvent(result.duplicate ? 'duplicate-skipped' : 'processed', meta);
    } catch (error) {
      // The message is handed to the retry topic and the main offset advances:
      // the retry worker owns it from here. A crash between publish and commit
      // produces a duplicate retry, and the event id makes that harmless.
      const nextRetryAt = await this.retryPolicy.scheduleRetry(
        context.getProducer(),
        envelope,
        source,
        1,
        error as Error,
      );
      await commitMessage(context);
      logEvent('retry-scheduled', meta, {
        retryCount: 1,
        nextRetryAt,
        reason: (error as Error).message,
      });
    } finally {
      this.drain.leave();
    }
  }
}
