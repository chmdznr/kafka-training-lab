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

  -- LATIHAN 1: tambahkan tabel penanda event yang sudah diproses.
  -- CREATE TABLE IF NOT EXISTS processed_events (
  --   event_id uuid PRIMARY KEY,
  --   topic text NOT NULL,
  --   processed_at timestamptz NOT NULL DEFAULT now()
  -- );
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

    // LATIHAN 1: klaim eventId lebih dulu di dalam transaksi yang sama dengan
    // efek pembayaran, lalu hanya tulis payments kalau klaim berhasil.
    //
    // return this.db.withTransaction(async (client) => {
    //   const claimed = await client.query(
    //     `INSERT INTO processed_events (event_id, topic)
    //      VALUES ($1, $2)
    //      ON CONFLICT (event_id) DO NOTHING
    //      RETURNING event_id`,
    //     [envelope.eventId, topic],
    //   );
    //   if (claimed.rowCount === 0) {
    //     return { duplicate: true };
    //   }
    //   ... insert payments dan cek payment_limits ...
    //   return { duplicate: false };
    // });
    await this.db.query(
      `INSERT INTO payments (event_id, order_id, amount, currency)
       VALUES ($1, $2, $3, $4)`,
      [envelope.eventId, orderId, amount, currency],
    );
    return { duplicate: false };
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
