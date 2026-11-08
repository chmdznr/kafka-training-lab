import { Injectable } from '@nestjs/common';
import type { Producer } from 'kafkajs';
import type { Envelope } from '../common/contract.js';
import {
  DLT_TOPIC,
  headerValue,
  type KafkaHeaders,
  retryDelayMs,
  RETRY_TOPIC,
} from '../common/kafka.js';

// The retry worker consumes the retry topic, so source.topic is not the topic
// the event originally came from. Preserve the original topic and position
// across retries and into the DLT.
function originalPositionOf(source: SourceMessage): {
  topic: string;
  partition: string;
  offset: string;
} {
  return {
    topic: headerValue(source.headers, 'original-topic') ?? source.topic,
    partition:
      headerValue(source.headers, 'original-partition') ?? String(source.partition),
    offset: headerValue(source.headers, 'original-offset') ?? source.offset,
  };
}

export interface SourceMessage {
  topic: string;
  partition: number;
  offset: string;
  key: Buffer | null;
  headers: KafkaHeaders;
}

@Injectable()
export class RetryPolicy {
  /** Publish the event to the retry topic with the delay it must wait for. */
  async scheduleRetry(
    producer: Producer,
    envelope: Envelope,
    source: SourceMessage,
    retryCount: number,
    error: Error,
  ): Promise<number> {
    const nextRetryAt = Date.now() + retryDelayMs(retryCount);
    const original = originalPositionOf(source);
    await producer.send({
      topic: RETRY_TOPIC,
      messages: [
        {
          key: source.key,
          value: JSON.stringify(envelope),
          headers: {
            'event-type': envelope.eventType,
            'correlation-id': envelope.correlationId,
            'original-topic': original.topic,
            'original-partition': original.partition,
            'original-offset': original.offset,
            'retry-count': String(retryCount),
            'next-retry-at': String(nextRetryAt),
            'last-error': error.message,
          },
        },
      ],
    });
    return nextRetryAt;
  }

  async toDeadLetter(
    producer: Producer,
    envelope: Envelope,
    source: SourceMessage,
    retryCount: number,
    error: Error,
  ): Promise<void> {
    const original = originalPositionOf(source);
    await producer.send({
      topic: DLT_TOPIC,
      messages: [
        {
          key: source.key,
          value: JSON.stringify(envelope),
          headers: {
            'event-type': envelope.eventType,
            'correlation-id': envelope.correlationId,
            'original-topic': original.topic,
            'original-partition': original.partition,
            'original-offset': original.offset,
            'retry-count': String(retryCount),
            'error-message': error.message,
            'failed-at': new Date().toISOString(),
          },
        },
      ],
    });
  }

  retryCountOf(headers: KafkaHeaders): number {
    const value = Number(headerValue(headers, 'retry-count'));
    return Number.isInteger(value) && value > 0 ? value : 1;
  }

  nextRetryAt(headers: KafkaHeaders): number {
    const value = Number(headerValue(headers, 'next-retry-at'));
    return Number.isFinite(value) ? value : 0;
  }
}
