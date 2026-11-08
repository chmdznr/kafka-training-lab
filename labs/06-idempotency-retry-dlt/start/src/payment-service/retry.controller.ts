import { setTimeout as sleep } from 'node:timers/promises';
import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ConsumerDrain } from '../common/consumer-drain.js';
import type { Envelope } from '../common/contract.js';
import {
  headerValue,
  MAX_RETRIES,
  ORDER_CREATED_TOPIC,
  RETRY_TOPIC,
} from '../common/kafka.js';
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
export class RetryController {
  constructor(
    private readonly payments: PaymentService,
    private readonly retryPolicy: RetryPolicy,
    private readonly drain: ConsumerDrain,
  ) {}

  // Nest 12 types event handlers as (data, ...args: unknown[]); see the main
  // consumer for why the decorator is cast.
  @(EventPattern(RETRY_TOPIC) as MethodDecorator)
  async handleRetry(
    @Payload() envelope: Envelope<OrderCreatedData>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    const source = sourceOf(context);
    const retryCount = this.retryPolicy.retryCountOf(source.headers);

    // Header-based backoff. Sleeping blocks this retry partition only; the main
    // consumer keeps working because it is a different process and group.
    const waitMs = this.retryPolicy.nextRetryAt(source.headers) - Date.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }

    const meta = metaOf(envelope, source);

    this.drain.attach(context.getConsumer(), context.getTopic());
    this.drain.enter();
    try {
      if (!isContractEvent(envelope, meta)) {
        await this.retryPolicy.toDeadLetter(
          context.getProducer(),
          envelope,
          source,
          retryCount,
          new Error('envelope tidak sesuai kontrak'),
        );
        await commitMessage(context);
        return;
      }

      const originalTopic =
        headerValue(source.headers, 'original-topic') ?? ORDER_CREATED_TOPIC;
      const result = await this.payments.process(envelope, originalTopic);
      await commitMessage(context);
      logEvent('retry-succeeded', meta, {
        retryCount,
        duplicate: result.duplicate,
      });
    } catch (error) {
      // LATIHAN 2: kalau retryCount sudah mencapai MAX_RETRIES, kirim pesan ke
      // DLT dengan retryPolicy.toDeadLetter(...). Kalau belum, jadwalkan
      // percobaan berikutnya dengan retryPolicy.scheduleRetry(...). Kedua
      // jalur harus meng-commit offset setelah publish.
      //
      // if (retryCount >= MAX_RETRIES) { ... } else { ... }
      throw error;
    } finally {
      this.drain.leave();
    }
  }
}
