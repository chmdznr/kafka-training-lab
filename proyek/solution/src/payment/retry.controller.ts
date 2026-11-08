import { setTimeout as sleep } from 'node:timers/promises';
import { Controller } from '@nestjs/common';
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
import {
  headerValue,
  MAX_RETRIES,
  ORDER_CREATED,
  PAYMENT_RETRY_TOPIC,
} from '../common/kafka.js';
import { JsonLogger } from '../common/logger.js';
import { PaymentService } from './payment.service.js';
import { RetryPolicy } from './retry-policy.js';

const logger = new JsonLogger('payment-service-retry');

@Controller()
export class RetryController {
  constructor(
    private readonly payments: PaymentService,
    private readonly retryPolicy: RetryPolicy,
    private readonly drain: ConsumerDrain,
  ) {}

  @(EventPattern(PAYMENT_RETRY_TOPIC) as MethodDecorator)
  async handleRetry(
    @Payload() envelope: Envelope<OrderCreatedData>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    const source = sourceOf(context);
    const retryCount = this.retryPolicy.retryCountOf(source.headers);

    // Header-based backoff. The retry worker has its own group, so this sleep
    // does not hold the main consumer.
    const waitMs = this.retryPolicy.nextRetryAt(source.headers) - Date.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }

    const meta = metaOf(envelope, source);
    this.drain.attach(context.getConsumer(), source.topic);
    this.drain.enter();
    try {
      const problem = envelopeProblem(envelope);
      if (problem) {
        await this.retryPolicy.toDeadLetter(
          context.getProducer(),
          envelope,
          source,
          retryCount,
          new Error(problem),
        );
        await commitMessage(context);
        logger.warn({ event: 'dead-lettered', retryCount, reason: problem, ...meta });
        return;
      }

      const originalTopic =
        headerValue(source.headers, 'original-topic') ?? ORDER_CREATED;
      const result = await this.payments.process(envelope, originalTopic);
      await commitMessage(context);
      logger.log({
        event: 'retry-succeeded',
        retryCount,
        duplicate: result.duplicate,
        outcome: result.outcome,
        ...meta,
      });
    } catch (error) {
      const reason = (error as Error).message;
      if (retryCount >= MAX_RETRIES) {
        await this.retryPolicy.toDeadLetter(
          context.getProducer(),
          envelope,
          source,
          retryCount,
          error as Error,
        );
        await commitMessage(context);
        logger.warn({ event: 'dead-lettered', retryCount, reason, ...meta });
      } else {
        const nextRetryCount = retryCount + 1;
        const nextRetryAt = await this.retryPolicy.scheduleRetry(
          context.getProducer(),
          envelope,
          source,
          nextRetryCount,
          error as Error,
        );
        await commitMessage(context);
        logger.warn({
          event: 'retry-scheduled',
          retryCount: nextRetryCount,
          nextRetryAt,
          reason,
          ...meta,
        });
      }
    } finally {
      this.drain.leave();
    }
  }
}
