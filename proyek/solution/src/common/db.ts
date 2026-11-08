import pg from 'pg';
import type { QueryResult, QueryResultRow, PoolClient } from 'pg';
import { Injectable, type OnApplicationShutdown } from '@nestjs/common';

const { Pool } = pg;

@Injectable()
export class DbService implements OnApplicationShutdown {
  readonly pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ?? 'postgres://app:app@localhost:5432/shop',
    max: 5,
  });

  query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<T>> {
    return this.pool.query<T>(text, values);
  }

  async withTransaction<T>(
    work: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // Runs after Nest has closed the transports (onModuleDestroy runs before
  // that), so an in-flight handler can still use the pool while it finishes.
  async onApplicationShutdown(): Promise<void> {
    await this.pool.end();
  }
}
