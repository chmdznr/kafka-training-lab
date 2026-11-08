import { setTimeout as sleep } from 'node:timers/promises';
import {
  Inject,
  Injectable,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';
import { DbService } from './db.js';
import { KAFKA_CLIENT, OUTBOX_TABLE } from './kafka.js';
import { JsonLogger } from './logger.js';

interface OutboxRow {
  id: string;
  topic: string;
  key: string;
  payload: unknown;
  headers: Record<string, string>;
}

/**
 * Publishes rows from one service's outbox table.
 *
 * The outbox row and the business change commit in the same database
 * transaction, so there is no dual write. If Kafka is down the poller keeps
 * retrying; the order is already durable.
 *
 * FOR UPDATE SKIP LOCKED lets more than one poller run without taking the same
 * row. `table` is a module constant, never request input.
 */
@Injectable()
export class OutboxPoller implements OnApplicationBootstrap, OnApplicationShutdown {
  #running = false;
  #loop: Promise<void> | undefined;
  #logger = new JsonLogger('outbox');

  constructor(
    private readonly db: DbService,
    @Inject(KAFKA_CLIENT) private readonly kafka: ClientKafka,
    @Inject(OUTBOX_TABLE) private readonly table: string,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.kafka.connect();
    this.#running = true;
    this.#loop = this.poll();
    this.#logger.log({ event: 'outbox-poller-started', table: this.table });
  }

  async onApplicationShutdown(): Promise<void> {
    this.#running = false;
    await this.#loop;
  }

  private async poll(): Promise<void> {
    while (this.#running) {
      try {
        const published = await this.publishBatch();
        if (published === 0) {
          await sleep(200);
        }
      } catch (error) {
        this.#logger.warn({
          event: 'outbox-publish-failed',
          table: this.table,
          reason: (error as Error).message,
        });
        await sleep(1000);
      }
    }
  }

  private async publishBatch(): Promise<number> {
    return this.db.withTransaction(async (client) => {
      const { rows } = await client.query<OutboxRow>(
        `SELECT id, topic, key, payload, headers
           FROM ${this.table}
          WHERE published_at IS NULL
          ORDER BY id
          LIMIT 10
          FOR UPDATE SKIP LOCKED`,
      );

      for (const row of rows) {
        await lastValueFrom(
          this.kafka.emit(row.topic, {
            key: row.key,
            value: row.payload,
            headers: row.headers,
          }),
        );
        await client.query(
          `UPDATE ${this.table} SET published_at = now() WHERE id = $1`,
          [row.id],
        );
      }
      return rows.length;
    });
  }
}
