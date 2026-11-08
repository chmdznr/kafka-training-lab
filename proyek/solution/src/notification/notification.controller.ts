import { Controller, Get } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ConsumerDrain } from '../common/consumer-drain.js';
import type { Envelope } from '../common/contract.js';
import type { PaymentEventData } from '../common/events.js';
import {
  commitMessage,
  envelopeProblem,
  metaOf,
  sourceOf,
} from '../common/kafka-helpers.js';
import {
  PAYMENT_COMPLETED,
  PAYMENT_FAILED,
} from '../common/kafka.js';
import { JsonLogger } from '../common/logger.js';
import { NotificationService } from './notification.service.js';

const logger = new JsonLogger('notification-service');

@Controller()
export class NotificationController {
  constructor(
    private readonly notifications: NotificationService,
    private readonly drain: ConsumerDrain,
  ) {}

  @Get('health')
  health() {
    return { status: 'ok' };
  }

  @(EventPattern(PAYMENT_COMPLETED) as MethodDecorator)
  handleCompleted(
    @Payload() envelope: Envelope<PaymentEventData>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.handle(envelope, PAYMENT_COMPLETED, context);
  }

  @(EventPattern(PAYMENT_FAILED) as MethodDecorator)
  handleFailed(
    @Payload() envelope: Envelope<PaymentEventData>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.handle(envelope, PAYMENT_FAILED, context);
  }

  private async handle(
    envelope: Envelope<PaymentEventData>,
    eventType: string,
    context: KafkaContext,
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

      const result = await this.notifications.record(envelope, eventType);
      await commitMessage(context);
      logger.log({
        event: result.duplicate ? 'notification-duplicate-skipped' : 'notification-stored',
        message: result.message,
        ...meta,
      });
    } finally {
      this.drain.leave();
    }
  }
}
