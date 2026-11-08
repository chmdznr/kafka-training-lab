import { setTimeout as sleep } from 'node:timers/promises';
import { Injectable, type OnModuleInit } from '@nestjs/common';
import type { Envelope } from '../common/contract.js';
import { DbService } from '../common/db.js';
import type { OrderCreatedData } from '../order-service/orders.service.js';

export const PAYMENT_SCHEMA = `
  CREATE TABLE IF NOT EXISTS payment_events (
    id bigserial PRIMARY KEY,
    event_id uuid NOT NULL,
    order_id text NOT NULL,
    amount numeric NOT NULL,
    currency text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
  )
`;

@Injectable()
export class PaymentService implements OnModuleInit {
  constructor(private readonly db: DbService) {}

  async onModuleInit(): Promise<void> {
    await this.db.query(PAYMENT_SCHEMA);
  }

  async process(envelope: Envelope<OrderCreatedData>): Promise<void> {
    // A slow handler makes the commit window visible when the process is killed.
    const delay = Number(process.env.PROCESSING_DELAY_MS ?? 0);
    if (delay > 0) {
      await sleep(delay);
    }

    const { orderId, amount, currency } = envelope.data;
    await this.db.query(
      `INSERT INTO payment_events (event_id, order_id, amount, currency)
       VALUES ($1, $2, $3, $4)`,
      [envelope.eventId, orderId, amount, currency],
    );
  }
}
