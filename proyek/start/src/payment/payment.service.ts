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
  -- Dipakai bersama payment-service dan notification-service, jadi kunci klaim
  -- adalah (consumer_group, event_id): satu group tidak menghalangi group lain
  -- memproses event yang sama.
  CREATE TABLE IF NOT EXISTS proyek_processed_events (
    consumer_group text NOT NULL,
    event_id uuid NOT NULL,
    topic text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
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

    // LATIHAN 3: klaim (consumer_group, eventId) di proyek_processed_events dan
    // tulis semua efek (proyek_payments + proyek_payment_outbox) dalam satu
    // transaksi Postgres. consumer_group diisi PAYMENT_GROUP dari
    // ../common/kafka.js; retry worker memanggil method ini juga dan memakai
    // nilai yang sama. Pakai ON CONFLICT (consumer_group, event_id) DO NOTHING.
    // Kalau klaim tidak mendapat baris, kembalikan { duplicate: true } tanpa
    // menulis apa pun. Baris outbox berisi payment.completed.v1 atau
    // payment.failed.v1; poller yang mengirimnya ke Kafka.
    await this.db.query(
      `INSERT INTO proyek_payments (event_id, order_id, amount, currency, status)
       VALUES ($1, $2, $3, $4, $5)`,
      [envelope.eventId, orderId, amount, currency, failed ? 'failed' : 'paid'],
    );

    return { duplicate: false, outcome: failed ? 'failed' : 'paid' };
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
