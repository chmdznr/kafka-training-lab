import { Injectable, type OnModuleInit } from '@nestjs/common';
import type { Envelope } from '../common/contract.js';
import { DbService } from '../common/db.js';
import type { PaymentEventData } from '../common/events.js';
import { PAYMENT_COMPLETED } from '../common/kafka.js';

export const NOTIFICATION_SCHEMA = `
  CREATE TABLE IF NOT EXISTS proyek_processed_events (
    event_id uuid PRIMARY KEY,
    topic text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE IF NOT EXISTS proyek_notifications (
    id bigserial PRIMARY KEY,
    event_id uuid NOT NULL,
    order_id text NOT NULL,
    event_type text NOT NULL,
    message text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
  );
`;

export interface NotificationResult {
  duplicate: boolean;
  message?: string;
}

@Injectable()
export class NotificationService implements OnModuleInit {
  constructor(private readonly db: DbService) {}

  async onModuleInit(): Promise<void> {
    await this.db.query(NOTIFICATION_SCHEMA);
  }

  async record(
    envelope: Envelope<PaymentEventData>,
    eventType: string,
  ): Promise<NotificationResult> {
    const { orderId } = envelope.data;
    const message =
      eventType === PAYMENT_COMPLETED
        ? `Pembayaran order ${orderId} berhasil`
        : `Pembayaran order ${orderId} gagal`;

    return this.db.withTransaction(async (client) => {
      const claimed = await client.query(
        `INSERT INTO proyek_processed_events (event_id, topic)
         VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING
         RETURNING event_id`,
        [envelope.eventId, eventType],
      );
      if (claimed.rowCount === 0) {
        return { duplicate: true };
      }

      await client.query(
        `INSERT INTO proyek_notifications (event_id, order_id, event_type, message)
         VALUES ($1, $2, $3, $4)`,
        [envelope.eventId, orderId, eventType, message],
      );
      return { duplicate: false, message };
    });
  }
}
