import { setTimeout as sleep } from 'node:timers/promises';
import { Injectable, type OnModuleInit } from '@nestjs/common';
import type { Envelope } from '../common/contract.js';
import { DbService } from '../common/db.js';
import type { OrderCreatedData } from '../order-service/orders.service.js';

export const SUPPORTED_CURRENCIES: Record<string, true> = {
  IDR: true,
  USD: true,
  EUR: true,
};

export const PAYMENT_SCHEMA = `
  CREATE TABLE IF NOT EXISTS processed_events (
    event_id uuid PRIMARY KEY,
    topic text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS payments (
    id bigserial PRIMARY KEY,
    event_id uuid NOT NULL,
    order_id text NOT NULL,
    amount numeric NOT NULL,
    currency text NOT NULL,
    status text NOT NULL DEFAULT 'paid',
    created_at timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS payment_limits (
    id int PRIMARY KEY,
    max_amount numeric NOT NULL
  );

  INSERT INTO payment_limits (id, max_amount) VALUES (1, 1000000)
  ON CONFLICT (id) DO NOTHING;
`;

export interface ProcessResult {
  duplicate: boolean;
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
    // A slow handler makes retry and crash tests easy to observe.
    const delay = Number(process.env.PROCESSING_DELAY_MS ?? 0);
    if (delay > 0) {
      await sleep(delay);
    }

    this.assertValid(envelope);

    const { orderId, amount } = envelope.data;
    const currency = envelope.data.currency;

    return this.db.withTransaction(async (client) => {
      // Idempotent consumer: the event id and the payment effect commit in the
      // same database transaction. A redelivered event finds the row and stops.
      const claimed = await client.query(
        `INSERT INTO processed_events (event_id, topic)
         VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING
         RETURNING event_id`,
        [envelope.eventId, topic],
      );
      if (claimed.rowCount === 0) {
        return { duplicate: true };
      }

      const { rows } = await client.query<{ max_amount: string }>(
        'SELECT max_amount FROM payment_limits WHERE id = 1',
      );
      const maxAmount = Number(rows[0]?.max_amount);
      if (amount > maxAmount) {
        throw new Error(
          `amount ${amount} melebihi batas ${maxAmount}; perbaiki batas lalu replay`,
        );
      }

      await client.query(
        `INSERT INTO payments (event_id, order_id, amount, currency)
         VALUES ($1, $2, $3, $4)`,
        [envelope.eventId, orderId, amount, currency],
      );
      return { duplicate: false };
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
    if (
      typeof data.currency !== 'string' ||
      SUPPORTED_CURRENCIES[data.currency] !== true
    ) {
      throw new Error(`currency tidak didukung: ${data.currency}`);
    }
  }
}
