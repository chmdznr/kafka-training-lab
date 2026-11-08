import { Controller, Get } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ConsumerDrain } from '../common/consumer-drain.js';
import type { Envelope } from '../common/contract.js';
import type { OrderCreatedData } from '../common/events.js';
import {
  commitMessage,
  envelopeProblem,
  metaOf,
  sourceOf,
} from '../common/kafka-helpers.js';
import { ORDER_CREATED } from '../common/kafka.js';
import { JsonLogger } from '../common/logger.js';
import { PaymentService } from './payment.service.js';
import { RetryPolicy } from './retry-policy.js';

const logger = new JsonLogger('payment-service');

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

  // Nest 12 types event handlers as (data, ...args: unknown[]); the cast keeps
  // the KafkaContext parameter checked.
  @(EventPattern(ORDER_CREATED) as MethodDecorator)
  async handleOrderCreated(
    @Payload() envelope: Envelope<OrderCreatedData>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    const source = sourceOf(context);
    const meta = metaOf(envelope, source);

    this.drain.attach(context.getConsumer(), source.topic);
    this.drain.enter();
    try {
      const problem = envelopeProblem(envelope);
      if (problem || typeof envelope.data?.orderId !== 'string') {
        logger.warn({
          event: 'invalid-event',
          ...meta,
          reason: problem ?? 'data.orderId wajib diisi',
        });
        await commitMessage(context);
        return;
      }

      const result = await this.payments.process(envelope, source.topic);
      await commitMessage(context);
      logger.log({
        event: result.duplicate ? 'duplicate-skipped' : 'processed',
        outcome: result.outcome,
        ...meta,
      });
    } catch (error) {
      const nextRetryAt = await this.retryPolicy.scheduleRetry(
        context.getProducer(),
        envelope,
        source,
        1,
        error as Error,
      );
      await commitMessage(context);
      logger.warn({
        event: 'retry-scheduled',
        retryCount: 1,
        nextRetryAt,
        reason: (error as Error).message,
        ...meta,
      });
    } finally {
      this.drain.leave();
    }
  }
}
