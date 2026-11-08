import { setTimeout as sleep } from 'node:timers/promises';
import { Injectable, type OnModuleInit } from '@nestjs/common';
import type { Envelope } from '../common/contract.js';
import { createEnvelope, headersFor } from '../common/contract.js';
import { DbService } from '../common/db.js';
import type { OrderCreatedData, PaymentEventData } from '../common/events.js';
import {
  PAYMENT_COMPLETED,
  PAYMENT_FAILED,
} from '../common/kafka.js';

export const PAYMENT_LIMIT = 1_000_000;

export const PAYMENT_SCHEMA = `
  CREATE TABLE IF NOT EXISTS proyek_processed_events (
    event_id uuid PRIMARY KEY,
    topic text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS proyek_payments (
    id bigserial PRIMARY KEY,
    event_id uuid NOT NULL,
    order_id text NOT NULL,
    amount numeric NOT NULL,
    currency text NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS proyek_payment_outbox (
    id bigserial PRIMARY KEY,
    topic text NOT NULL,
    key text NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
  );
`;

export interface ProcessResult {
  duplicate: boolean;
  outcome?: 'paid' | 'failed';
}

@Injectable()
export class PaymentService implements OnModuleInit {
  constructor(private readonly db: DbService) {}

  async onModuleInit(): Promise<void> {
    await this.db.query(PAYMENT_SCHEMA);
  }

  async process(
    envelope: Envelope<OrderCreatedData>,
    topic: string,
  ): Promise<ProcessResult> {
    const delay = Number(process.env.PROCESSING_DELAY_MS ?? 0);
    if (delay > 0) {
      await sleep(delay);
    }

    this.assertValid(envelope);

    const { orderId, amount, currency } = envelope.data;
    const failed = amount > PAYMENT_LIMIT;

    return this.db.withTransaction(async (client) => {
      // Idempotent consumer: claim the event id and write every effect in one
      // transaction. A redelivered event stops here.
      const claimed = await client.query(
        `INSERT INTO proyek_processed_events (event_id, topic)
         VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING
         RETURNING event_id`,
        [envelope.eventId, topic],
      );
      if (claimed.rowCount === 0) {
        return { duplicate: true };
      }

      await client.query(
        `INSERT INTO proyek_payments (event_id, order_id, amount, currency, status)
         VALUES ($1, $2, $3, $4, $5)`,
        [envelope.eventId, orderId, amount, currency, failed ? 'failed' : 'paid'],
      );

      // Outbox row and payment commit together; the poller publishes it.
      const eventType = failed ? PAYMENT_FAILED : PAYMENT_COMPLETED;
      const paymentEvent = createEnvelope<PaymentEventData>({
        eventType,
        version: 1,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        data: {
          orderId,
          amount,
          currency,
          ...(failed ? { reason: 'LIMIT_EXCEEDED' } : {}),
        },
      });
      await client.query(
        `INSERT INTO proyek_payment_outbox (topic, key, payload, headers)
         VALUES ($1, $2, $3::jsonb, $4::jsonb)`,
        [
          eventType,
          orderId,
          JSON.stringify(paymentEvent),
          JSON.stringify(headersFor(paymentEvent)),
        ],
      );

      return { duplicate: false, outcome: failed ? 'failed' : 'paid' };
    });
  }

  private assertValid(envelope: Envelope<OrderCreatedData>): void {
    const data = envelope.data;
    if (typeof data?.orderId !== 'string' || data.orderId === '') {
      throw new Error('data.orderId wajib diisi');
    }
    const amount = Number(data.amount);
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new Error('data.amount harus berupa angka lebih besar dari 0');
    }
    if (typeof data.currency !== 'string' || data.currency === '') {
      throw new Error('data.currency wajib diisi');
    }
  }
}
