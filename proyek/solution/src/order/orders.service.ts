import { randomUUID } from 'node:crypto';
import {
  BadRequestException,
  Injectable,
  NotFoundException,
  type OnModuleInit,
} from '@nestjs/common';
import { createEnvelope, headersFor } from '../common/contract.js';
import { DbService } from '../common/db.js';
import type { OrderCreatedData } from '../common/events.js';
import { ORDER_CREATED } from '../common/kafka.js';
import { JsonLogger } from '../common/logger.js';

export const ORDER_SCHEMA = `
  CREATE TABLE IF NOT EXISTS proyek_orders (
    id uuid PRIMARY KEY,
    customer_id text NOT NULL,
    amount numeric NOT NULL,
    currency text NOT NULL,
    status text NOT NULL DEFAULT 'PENDING',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS proyek_order_outbox (
    id bigserial PRIMARY KEY,
    topic text NOT NULL,
    key text NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
  );
`;

export interface OrderInput {
  orderId?: string;
  customerId: string;
  amount: number;
  currency?: string;
}

export interface OrderRow {
  id: string;
  customer_id: string;
  amount: string;
  currency: string;
  status: string;
  created_at: string;
  updated_at: string;
}

const logger = new JsonLogger('order-service');

@Injectable()
export class OrdersService implements OnModuleInit {
  constructor(private readonly db: DbService) {}

  async onModuleInit(): Promise<void> {
    await this.db.query(ORDER_SCHEMA);
  }

  async place(input: Partial<OrderInput>, correlationId?: string) {
    if (!input || typeof input.customerId !== 'string' || input.customerId === '') {
      throw new BadRequestException('customerId wajib diisi');
    }
    if (
      typeof input.amount !== 'number' ||
      !Number.isFinite(input.amount) ||
      input.amount <= 0
    ) {
      throw new BadRequestException('amount harus berupa angka lebih besar dari 0');
    }

    const orderId = input.orderId ?? randomUUID();
    const currency = (input.currency ?? 'IDR').toUpperCase();
    const envelope = createEnvelope<OrderCreatedData>({
      eventType: ORDER_CREATED,
      version: 1,
      correlationId: correlationId ?? randomUUID(),
      data: {
        orderId,
        customerId: input.customerId,
        amount: input.amount,
        currency,
      },
    });

    // One transaction: order + event. There is no window where one exists
    // without the other, even when Kafka is down.
    await this.db.withTransaction(async (client) => {
      await client.query(
        `INSERT INTO proyek_orders (id, customer_id, amount, currency)
         VALUES ($1, $2, $3, $4)`,
        [orderId, input.customerId, input.amount, currency],
      );
      await client.query(
        `INSERT INTO proyek_order_outbox (topic, key, payload, headers)
         VALUES ($1, $2, $3::jsonb, $4::jsonb)`,
        [ORDER_CREATED, orderId, JSON.stringify(envelope), JSON.stringify(headersFor(envelope))],
      );
    });

    logger.log({
      event: 'order.accepted',
      orderId,
      eventId: envelope.eventId,
      correlationId: envelope.correlationId,
    });

    return {
      orderId,
      eventId: envelope.eventId,
      correlationId: envelope.correlationId,
      status: 'PENDING',
    };
  }

  async get(orderId: string): Promise<OrderRow> {
    const { rows } = await this.db.query<OrderRow>(
      `SELECT id, customer_id, amount, currency, status, created_at, updated_at
         FROM proyek_orders
        WHERE id::text = $1`,
      [orderId],
    );
    if (!rows[0]) {
      throw new NotFoundException(`order ${orderId} tidak ditemukan`);
    }
    return rows[0];
  }

  async markStatus(orderId: string, status: 'PAID' | 'FAILED'): Promise<boolean> {
    const result = await this.db.query(
      `UPDATE proyek_orders
          SET status = $2, updated_at = now()
        WHERE id::text = $1 AND status = 'PENDING'`,
      [orderId, status],
    );
    return (result.rowCount ?? 0) > 0;
  }
}
